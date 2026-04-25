"""pizza deep-research — Gemini 提案 + Co-occurrence Gate + Claude LLM critic + 国税庁 検証。

**ハルシネ防止の本質** (初心回帰):
  LLM は必ずハルシネする。だから LLM 単独出力を信用する layer は一切作らない。
  独立な 4 Gate をすべて通過しないと DB update しない。

**4 段 Gate**:
  Gate 1: **Gemini 候補生成** (Google Search grounding、evidence URL 必須)
  Gate 2: **Co-occurrence Gate** (literal match ではない)
           evidence URL HTML 内で operator_name と store identifier
           (store_name OR address_key OR phone) が 500 chars 以内に
           **同時に**存在するか。片方だけなら reject。
  Gate 3: **Claude LLM critic (reasoning)**
           co-occurrence snippet を Claude に渡し
           「この文脈で店舗 X が operator Y によって運営されている と
           読み取れるか」 を yes/no + reasoning で判定。
           単純 string match ではなく、semantic な関係性判定。
  Gate 4: **国税庁 CSV verify** (法人実在 + corp_number 確定)

**失敗時の振る舞い**: どれか 1 Gate でも fail → そのまま operator 不明のまま
  (楽観的な update は絶対にしない)。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DeepResearchProposal:
    """1 店舗に対する 4 段 Gate 検証の結果。"""

    place_id: str
    store_name: str
    address: str
    phone: str
    brand: str
    # Gate 1: Gemini output
    gemini_operator: str = ""
    gemini_corp_guess: str = ""
    gemini_evidence_urls: list[str] = field(default_factory=list)
    gemini_confidence: float = 0.0
    gemini_error: str = ""
    # Gate 2: Co-occurrence
    cooccurrence_found: bool = False
    cooccurrence_url: str = ""
    cooccurrence_snippet: str = ""
    cooccurrence_store_key: str = ""  # どの store identifier が一致したか
    # Gate 3: Claude LLM critic (reasoning)
    claude_llm_verdict: str = ""      # yes | no | ambiguous
    claude_llm_reason: str = ""
    # Gate 4: 国税庁 final validation
    houjin_verified: bool = False
    final_operator: str = ""
    final_corp: str = ""
    # Accept or reject
    accepted: bool = False
    reject_reason: str = ""


@dataclass
class DeepResearchStats:
    target_stores: int = 0
    gemini_called: int = 0
    gemini_returned: int = 0
    claude_verified: int = 0
    houjin_verified: int = 0
    accepted: int = 0
    rejected: list[str] = field(default_factory=list)


# ─── Gemini deep research (web grounding 付き) ────────────────


_GEMINI_SYSTEM = """あなたは日本のフランチャイズ事業会社特定の専門家。
与えられた店舗情報から、その店舗の **運営会社 (FC 加盟企業、法人名)** を
web 検索で調べて JSON で返す。

ハルシネ禁止ルール:
  - 必ず Google 検索結果を根拠にする
  - 「推測」「一般論」は絶対に書かない、事実のみ
  - 運営会社が特定できなければ operator_name="" で返す
  - 必ず evidence_urls に 1 つ以上の参考 URL を含める
  - 本部 (例: 「株式会社モスフードサービス」) は operator ではない → 除外する

JSON schema (必須):
  {
    "operator_name": "株式会社〇〇",         // 加盟店法人、空でも可
    "corporate_number_guess": "1234567890123", // 13 桁、分からなければ空
    "evidence_urls": ["https://...", "..."], // 必須、最低 1 URL
    "confidence": 0.0-1.0,
    "reasoning": "50 字以内"
  }
"""


async def _call_gemini_research(
    store_name: str, address: str, phone: str, brand: str,
) -> dict[str, Any]:
    """Gemini 2.5 Pro (Google Search grounding) で運営会社調査。

    未対応 environment (API key 無し等) なら None-like dict を返す。
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"error": "gemini_api_key_missing"}
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        return {"error": "google_genai_not_installed"}

    model_id = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")

    user = (
        f"ブランド: {brand}\n"
        f"店舗名: {store_name}\n"
        f"住所: {address}\n"
        f"電話: {phone}\n\n"
        f"この店舗の運営会社 (FC 加盟企業) を web 検索で調べて JSON で返してください。"
    )

    try:
        client = genai.Client(api_key=api_key)
        # Grounding with Google Search を有効化
        config = types.GenerateContentConfig(
            system_instruction=_GEMINI_SYSTEM,
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0.0,
        )
        resp = await asyncio.to_thread(
            client.models.generate_content,
            model=model_id,
            contents=user,
            config=config,
        )
    except Exception as e:
        return {"error": f"gemini_call: {e}"}

    # 返却テキスト → JSON parse
    text = getattr(resp, "text", "") or ""
    data: dict[str, Any] = {}
    # JSON ブロックを探す
    import re

    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    # grounding chunks からも evidence URL を補完
    try:
        chunks = (
            resp.candidates[0].grounding_metadata.grounding_chunks
            if resp.candidates else []
        )
        extra_urls = [c.web.uri for c in chunks if c.web and c.web.uri]
        if extra_urls and "evidence_urls" not in data:
            data["evidence_urls"] = extra_urls[:5]
        elif extra_urls:
            data["evidence_urls"] = list({*data.get("evidence_urls", []), *extra_urls})[:5]
    except Exception:
        pass
    return data


# ─── Gate 2: Co-occurrence Gate (literal match ではない、関係性証明) ──


def _build_store_keys(store_name: str, address: str, phone: str) -> list[str]:
    """店舗識別のための検索 key 群を構築。

    address は「町名+番地」相当を key として使う (全住所 string では miss が多い)。
    phone は「-」除去形も含める。
    """
    import re as _re

    keys: list[str] = []
    if phone:
        keys.append(phone)
        keys.append(phone.replace("-", ""))
    if store_name:
        # 「モスバーガー」 brand 名は除いて店舗 suffix のみ key に
        store_core = _re.sub(r"^(モスバーガー|MOS BURGER)\s*", "", store_name)
        store_core = _re.sub(r"店$", "", store_core).strip()
        if len(store_core) >= 2:
            keys.append(store_core)  # 例: "梅ヶ丘駅前"
    if address:
        # 〒 除去 + 都道府県 skip して 「市+町名+番地」部分を抽出
        addr = _re.sub(r"^〒?\d{3}[-‐]?\d{4}\s*", "", address).strip()
        # 郡/市/区/町/村 から番地までの連続
        m = _re.search(
            r"[一-龥]{1,6}(?:市|区|町|村|郡)[一-龥ぁ-んァ-ヶ]{1,8}[0-9０-９]",
            addr,
        )
        if m:
            keys.append(m.group(0))  # 例: "世田谷区梅丘1"
        # 番地前の町名だけも key に (「梅丘」「大崎」等)
        m2 = _re.search(r"(?:市|区|町|村|郡)[^0-9０-９]{2,4}", addr)
        if m2:
            keys.append(m2.group(0)[1:])  # 区 を除いた部分
    # dedup + 短すぎる key を除外
    out: list[str] = []
    seen = set()
    for k in keys:
        k = k.strip()
        if not k or k in seen or len(k) < 2:
            continue
        seen.add(k)
        out.append(k)
    return out


async def _check_cooccurrence(
    operator_name: str,
    evidence_urls: list[str],
    store_keys: list[str],
    max_distance: int = 500,
) -> tuple[bool, str, str, str]:
    """Gate 2: operator_name と store_key が同 HTML 内 max_distance 以内に両方存在するか。

    Returns: (found, url, snippet, matched_store_key)
    """
    if not operator_name or not evidence_urls or not store_keys:
        return False, "", "", ""
    from pizza_delivery.scrapling_fetcher import ScraplingFetcher
    import re as _re

    sf = ScraplingFetcher()
    for url in evidence_urls[:4]:
        try:
            html = await asyncio.to_thread(sf.fetch_static, url)
            if not html or len(html) < 500:
                html = await asyncio.to_thread(sf.fetch_dynamic, url) or ""
        except Exception as e:
            logger.debug("cooccurrence fetch %s: %s", url, e)
            continue
        if not html or operator_name not in html:
            continue
        # HTML → text (タグ剥離で co-occurrence 距離を正確に)
        try:
            from bs4 import BeautifulSoup

            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
        except Exception:
            text = _re.sub(r"<[^>]+>", " ", html)
        op_idx = text.find(operator_name)
        if op_idx < 0:
            continue
        # 各 store_key について、operator_name 近傍 (±max_distance) に含まれるか
        window_start = max(0, op_idx - max_distance)
        window_end = op_idx + len(operator_name) + max_distance
        window = text[window_start:window_end]
        for key in store_keys:
            if key in window:
                # snippet 構築: operator_name と key の両方を含む最小範囲
                k_idx_in_window = window.find(key)
                start = max(0, min(op_idx - window_start, k_idx_in_window) - 80)
                end = max(op_idx - window_start + len(operator_name),
                          k_idx_in_window + len(key)) + 80
                snippet = window[start:end].strip()
                snippet = _re.sub(r"\s+", " ", snippet)
                return True, url, snippet[:400], key
    return False, "", "", ""


# ─── Gate 3: Claude LLM critic (reasoning) ─────────────────


_CLAUDE_CRITIC_SYSTEM = """あなたは FC 事業会社の特定検証の critic。
与えられた store 情報と candidate operator 名、および証拠 snippet を見て、
『この snippet は store がこの operator によって運営されていることの
証拠として妥当か』を判定する。

判定基準 (厳格):
  yes         : snippet が明示的に「operator が store を運営/加盟/所有」等を
                記載しており、両者の関係が明白
  no          : operator と store が同一 snippet にあるが、直接関係が不明
                (例: 「operator は別の業種」「求人募集のみで雇用主不明」「広告文」)
  ambiguous   : 関係がありそうだが確定証拠不足 (上記に迷うケース)

JSON 必須: {verdict: "yes"|"no"|"ambiguous", reasoning: "50字以内"}
"""


async def _claude_llm_critic(
    store_name: str, address: str, brand: str,
    operator_name: str, snippet: str,
) -> tuple[str, str]:
    """Claude Haiku に snippet の妥当性を semantic 判定させる。

    Returns: (verdict, reasoning)   verdict ∈ {"yes","no","ambiguous","error"}
    """
    if not operator_name or not snippet:
        return "no", "empty_input"
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "error", "no_anthropic_key"
    try:
        from pizza_delivery.providers.registry import get_provider

        provider = get_provider("anthropic")
        llm = provider.make_llm()
    except Exception as e:
        return "error", f"llm_init: {e}"

    user = (
        f"ブランド: {brand}\n"
        f"店舗名: {store_name}\n"
        f"店舗住所: {address}\n"
        f"候補 operator: {operator_name}\n\n"
        f"証拠 snippet (web から抽出):\n{snippet}\n\n"
        f"この snippet は候補 operator が当該店舗を運営している証拠として妥当か?"
    )

    try:
        from browser_use.llm.messages import SystemMessage, UserMessage
    except ImportError:
        return "error", "browser_use_messages_unavailable"

    messages = [
        SystemMessage(content=_CLAUDE_CRITIC_SYSTEM),
        UserMessage(content=user),
    ]

    try:
        resp = await llm.ainvoke(messages)
    except Exception as e:
        return "error", f"llm_invoke: {e}"

    raw = (
        getattr(resp, "completion", None)
        or getattr(resp, "content", None)
        or str(resp)
    )
    if hasattr(raw, "content"):
        raw = raw.content
    text = str(raw) if raw is not None else ""
    # JSON block 抽出
    import re as _re

    m = _re.search(r"\{[\s\S]*?\}", text)
    if not m:
        return "error", f"no_json: {text[:80]}"
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError:
        return "error", f"bad_json: {text[:80]}"
    verdict = str(data.get("verdict", "")).strip().lower()
    reason = str(data.get("reasoning", ""))[:80]
    if verdict not in ("yes", "no", "ambiguous"):
        verdict = "error"
    return verdict, reason


# ─── 3 段検証 orchestrator ─────────────────────────────


async def research_one_store(
    place_id: str, store_name: str, address: str, phone: str, brand: str,
) -> DeepResearchProposal:
    """1 店舗を 4 段 Gate (Gemini → Co-occurrence → Claude LLM → 国税庁) に通す。

    どれか 1 Gate でも fail → reject (accept=False)。
    """
    p = DeepResearchProposal(
        place_id=place_id, store_name=store_name,
        address=address, phone=phone, brand=brand,
    )

    # === Gate 1: Gemini 候補生成 ===
    data = await _call_gemini_research(store_name, address, phone, brand)
    if "error" in data:
        p.gemini_error = str(data["error"])
        p.reject_reason = f"gemini_{data['error']}"
        return p
    p.gemini_operator = str(data.get("operator_name") or "").strip()
    p.gemini_corp_guess = str(data.get("corporate_number_guess") or "").strip()
    p.gemini_evidence_urls = list(data.get("evidence_urls") or [])
    try:
        p.gemini_confidence = float(data.get("confidence") or 0)
    except Exception:
        p.gemini_confidence = 0.0
    if not p.gemini_operator:
        p.reject_reason = "gate1_no_operator"
        return p
    if not p.gemini_evidence_urls:
        p.reject_reason = "gate1_no_evidence_urls"
        return p

    # === Gate 2: Co-occurrence Gate (operator + store identifier 近接) ===
    store_keys = _build_store_keys(store_name, address, phone)
    if not store_keys:
        p.reject_reason = "gate2_no_store_keys"
        return p
    cooc_found, cooc_url, cooc_snippet, matched_key = await _check_cooccurrence(
        p.gemini_operator, p.gemini_evidence_urls, store_keys,
    )
    p.cooccurrence_found = cooc_found
    p.cooccurrence_url = cooc_url
    p.cooccurrence_snippet = cooc_snippet
    p.cooccurrence_store_key = matched_key
    if not cooc_found:
        p.reject_reason = "gate2_no_cooccurrence (operator only, no store context)"
        return p

    # === Gate 3: Claude LLM critic (semantic reasoning) ===
    verdict, reason = await _claude_llm_critic(
        store_name, address, brand, p.gemini_operator, cooc_snippet,
    )
    p.claude_llm_verdict = verdict
    p.claude_llm_reason = reason
    if verdict != "yes":
        p.reject_reason = f"gate3_claude_{verdict}: {reason[:40]}"
        return p

    # === Gate 4: 国税庁 CSV で法人存在検証 (exact match 優先) ===
    from pizza_delivery.houjin_csv import HoujinCSVIndex

    idx = HoujinCSVIndex()
    recs = idx.search_by_name(p.gemini_operator, limit=3, active_only=True)
    if not recs:
        recs = idx.search_by_name(p.gemini_operator, limit=3, active_only=False)
    best = None
    for r in recs:
        if r.name == p.gemini_operator:
            best = r
            break
    if best is None:
        p.reject_reason = "gate4_houjin_no_exact_match"
        return p
    p.houjin_verified = True
    p.final_operator = best.name
    p.final_corp = best.corporate_number
    p.accepted = True
    return p


async def deep_research_brand(
    db_path: str | Path,
    *,
    brand: str,
    max_stores: int = 20,
    dry_run: bool = False,
    concurrency: int = 2,
) -> tuple[DeepResearchStats, list[DeepResearchProposal]]:
    """brand の operator 不明店舗を deep-research で解析。

    コスト配慮: Gemini 2.5 Pro は 1 call $0.005-0.02。
    max_stores=20 で $0.1-0.4 程度。
    """
    stats = DeepResearchStats()
    proposals: list[DeepResearchProposal] = []

    # 対象 store
    conn = sqlite3.connect(db_path)
    try:
        q = """
        SELECT s.place_id, s.name, s.address,
               COALESCE(s.phone,'') AS phone
        FROM stores s
        WHERE s.brand = ?
        AND s.address != ''
        AND s.place_id NOT IN (
            SELECT os.place_id FROM operator_stores os
            WHERE os.operator_name != ''
              AND COALESCE(os.operator_type,'') NOT IN ('franchisor')
        )
        ORDER BY s.place_id
        """
        rows = conn.execute(q, (brand,)).fetchall()
        if max_stores > 0:
            rows = rows[:max_stores]
        stats.target_stores = len(rows)
    finally:
        conn.close()

    sem = asyncio.Semaphore(concurrency)

    async def _task(row) -> DeepResearchProposal:
        pid, name, addr, phone = row
        async with sem:
            return await research_one_store(pid, name, addr, phone, brand)

    proposals = await asyncio.gather(*(_task(r) for r in rows))

    for p in proposals:
        stats.gemini_called += 1
        if p.gemini_operator:
            stats.gemini_returned += 1
        if p.cooccurrence_found:
            stats.claude_verified += 1
        if p.houjin_verified:
            stats.houjin_verified += 1
        if p.accepted:
            stats.accepted += 1
        else:
            stats.rejected.append(f"{p.place_id}:{p.reject_reason}")

    # Apply to DB (4 Gate 全通過 accepted のみ)
    if not dry_run:
        conn = sqlite3.connect(db_path)
        try:
            for p in proposals:
                if not p.accepted:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO operator_stores "
                    "(operator_name, place_id, brand, operator_type, confidence, "
                    " discovered_via, corporate_number) "
                    "VALUES (?, ?, ?, 'franchisee', 0.85, "
                    "       'gemini_cooccur_claude_llm_houjin_4gate', ?)",
                    (p.final_operator, p.place_id, brand, p.final_corp),
                )
            conn.commit()
        finally:
            conn.close()

    return stats, proposals


def _main() -> None:
    import argparse
    import sys

    ap = argparse.ArgumentParser(
        description="operator 不明店舗を Gemini research + Claude critic + 国税庁 検証"
    )
    ap.add_argument("--brand", required=True)
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--max-stores", type=int, default=20,
                    help="Gemini 呼出上限 (cost 配慮、default 20)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--concurrency", type=int, default=2)
    ap.add_argument("--out", default="", help="proposal JSON 出力")
    args = ap.parse_args()

    if not Path(args.db).exists():
        print(f"❌ db not found: {args.db}", file=sys.stderr)
        sys.exit(2)

    stats, proposals = asyncio.run(deep_research_brand(
        args.db, brand=args.brand, max_stores=args.max_stores,
        dry_run=args.dry_run, concurrency=args.concurrency,
    ))
    print(f"✅ deep-research {'dry-run' if args.dry_run else 'apply'}  brand={args.brand}")
    print(f"   target_stores   = {stats.target_stores}")
    print(f"   gemini_returned = {stats.gemini_returned}")
    print(f"   claude_verified = {stats.claude_verified}")
    print(f"   houjin_verified = {stats.houjin_verified}")
    print(f"   ACCEPTED        = {stats.accepted}")
    print(f"   rejected        = {len(stats.rejected)}")

    if args.out:
        data = []
        for p in proposals:
            data.append({
                "place_id": p.place_id, "store_name": p.store_name,
                "address": p.address, "phone": p.phone,
                "gemini_operator": p.gemini_operator,
                "gemini_evidence_urls": p.gemini_evidence_urls,
                "cooccurrence_found": p.cooccurrence_found,
                "cooccurrence_url": p.cooccurrence_url,
                "cooccurrence_snippet": p.cooccurrence_snippet,
                "cooccurrence_store_key": p.cooccurrence_store_key,
                "claude_llm_verdict": p.claude_llm_verdict,
                "claude_llm_reason": p.claude_llm_reason,
                "houjin_verified": p.houjin_verified,
                "final_operator": p.final_operator, "final_corp": p.final_corp,
                "accepted": p.accepted, "reject_reason": p.reject_reason,
            })
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"📄 proposals: {args.out}")
    for r in stats.rejected[:5]:
        print(f"   ⚠  reject: {r}")


if __name__ == "__main__":
    _main()
