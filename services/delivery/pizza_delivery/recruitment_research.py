"""求人・採用ページ特化の operator 特定 pipeline。

検索自体は Gemini Google Search grounding を使うが、DB に採用する条件は
決定論的 gate に限定する:

  1. evidence URL の本文を取得できる
  2. 本文中で operator 名と店舗識別子が近接する
  3. operator 名の近傍に「会社名」「雇用主」「運営会社」等の雇用主ラベルがある
  4. 国税庁 CSV で法人名が normalized exact match する

求人サイトの検索 snippet や LLM の回答は ground truth として扱わない。
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


TARGET_BRANDS = [
    "カーブス",
    "モスバーガー",
    "業務スーパー",
    "Itto個別指導学院",
    "エニタイムフィットネス",
    "コメダ珈琲",
    "シャトレーゼ",
    "ハードオフ",
    "オフハウス",
    "Kids Duo",
    "アップガレージ",
    "カルビ丼とスン豆腐専門店韓丼",
    "Brand off",
    "TSUTAYA",
]


_STORE_NAME_BRANDS = [
    "カーブス",
    "モスバーガー",
    "MOS BURGER",
    "業務スーパー",
    "ITTO個別指導学院",
    "Itto個別指導学院",
    "エニタイムフィットネス",
    "Anytime Fitness",
    "コメダ珈琲店",
    "コメダ珈琲",
    "シャトレーゼ",
    "ハードオフ",
    "HARD OFF",
    "オフハウス",
    "OFF HOUSE",
    "Kids Duo",
    "キッズデュオ",
    "アップガレージ",
    "UP GARAGE",
    "カルビ丼とスン豆腐専門店韓丼",
    "韓丼",
    "Brand off",
    "BRAND OFF",
    "TSUTAYA",
]


_EMPLOYER_LABELS = [
    "会社名",
    "企業名",
    "法人名",
    "社名",
    "商号",
    "雇用主",
    "求人企業",
    "掲載企業",
    "募集企業",
    "採用企業",
    "応募先企業",
    "運営会社",
    "店舗運営",
    "事業者",
]


_JOB_PAGE_KEYWORDS = [
    "求人",
    "採用",
    "アルバイト",
    "パート",
    "正社員",
    "募集",
    "転職",
    "応募",
    "ハローワーク",
]


_JOB_SITE_HINTS = [
    "indeed.com",
    "townwork.net",
    "baitoru.com",
    "froma.com",
    "mynavi.jp",
    "job-medley.com",
    "kyujinbox.com",
    "engage",
    "airwork",
    "hellowork",
    "求人",
    "採用",
]


@dataclass
class RecruitmentCandidate:
    operator_name: str = ""
    evidence_urls: list[str] = field(default_factory=list)
    source_type: str = ""
    confidence: float = 0.0
    reasoning: str = ""


@dataclass
class RecruitmentEvidence:
    found: bool = False
    url: str = ""
    snippet: str = ""
    matched_store_key: str = ""
    matched_label: str = ""
    reject_reason: str = ""


@dataclass
class RecruitmentEvidenceAttempt:
    candidate_operator: str = ""
    candidate_confidence: float = 0.0
    source_type: str = ""
    url: str = ""
    fetched: bool = False
    found: bool = False
    reject_reason: str = ""
    matched_store_key: str = ""
    matched_label: str = ""
    snippet: str = ""


@dataclass
class RecruitmentProposal:
    place_id: str
    store_name: str
    address: str
    phone: str
    brand: str
    candidates: list[RecruitmentCandidate] = field(default_factory=list)
    evidence_attempts: list[RecruitmentEvidenceAttempt] = field(default_factory=list)
    evidence: RecruitmentEvidence = field(default_factory=RecruitmentEvidence)
    final_operator: str = ""
    final_corp: str = ""
    accepted: bool = False
    reject_reason: str = ""


@dataclass
class RecruitmentStats:
    target_stores: int = 0
    gemini_called: int = 0
    candidates_returned: int = 0
    evidence_verified: int = 0
    houjin_verified: int = 0
    accepted: int = 0
    rejected: list[str] = field(default_factory=list)


_GEMINI_SYSTEM = """あなたは日本のFC店舗の運営会社特定リサーチャー。
求人サイト・採用ページ・公式採用ページを優先して、与えられた店舗の
雇用主または運営会社である法人名を調査し、JSONだけで返す。

厳格ルール:
  - 求人/採用/アルバイト/転職ページなど、雇用主または会社名が明記されたURLを優先
  - ブランド本部名はoperatorではないので除外
  - 推測しない。明示的な候補がなければ candidates=[]
  - evidence_urls は実際に確認したページURLだけ
  - 検索snippetだけを根拠にしない

JSON schema:
{
  "candidates": [
    {
      "operator_name": "株式会社〇〇",
      "evidence_urls": ["https://..."],
      "source_type": "job_site|official_recruit|other",
      "confidence": 0.0,
      "reasoning": "50字以内"
    }
  ]
}
"""


def build_store_keys(
    store_name: str,
    address: str,
    phone: str,
    brand: str = "",
) -> list[str]:
    """求人ページ本文との照合用に店舗識別 key を作る。"""
    keys: list[str] = []
    if phone:
        keys.append(phone)
        keys.append(phone.replace("-", ""))
    name = (store_name or "").strip()
    if name:
        keys.append(name)
        core = name
        for b in sorted([brand, *_STORE_NAME_BRANDS], key=len, reverse=True):
            if b:
                core = re.sub(rf"^{re.escape(b)}\s*", "", core, flags=re.IGNORECASE)
        core = re.sub(r"(?:店|店舗)$", "", core).strip(" 　-ー")
        if len(core) >= 2:
            keys.append(core)
            keys.append(core + "店")
    if address:
        addr = re.sub(r"^〒?\d{3}[-‐]?\d{4}\s*", "", address).strip()
        m = re.search(
            r"[一-龥]{1,6}(?:市|区|町|村|郡)[一-龥ぁ-んァ-ヶ]{1,12}[0-9０-９]",
            addr,
        )
        if m:
            keys.append(m.group(0))
        m2 = re.search(r"(?:市|区|町|村|郡)([^0-9０-９]{2,8})", addr)
        if m2:
            keys.append(m2.group(1))

    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        k = re.sub(r"\s+", "", k.strip())
        if not k or len(k) < 2 or k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def parse_recruitment_candidates(data: dict[str, Any]) -> list[RecruitmentCandidate]:
    """Gemini JSON を型付き候補に変換する。旧 single-object 形式も許容する。"""
    raw_items = data.get("candidates")
    if raw_items is None and data.get("operator_name"):
        raw_items = [data]
    if not isinstance(raw_items, list):
        return []

    out: list[RecruitmentCandidate] = []
    seen: set[tuple[str, str]] = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("operator_name") or "").strip()
        urls_raw = item.get("evidence_urls") or []
        if isinstance(urls_raw, str):
            urls = [urls_raw]
        else:
            urls = [str(u).strip() for u in urls_raw if str(u).strip()]
        urls = _filter_evidence_urls(urls)
        if not name or not urls:
            continue
        key = (name, urls[0])
        if key in seen:
            continue
        seen.add(key)
        try:
            conf = float(item.get("confidence") or 0.0)
        except Exception:
            conf = 0.0
        out.append(RecruitmentCandidate(
            operator_name=name,
            evidence_urls=urls[:5],
            source_type=str(item.get("source_type") or ""),
            confidence=conf,
            reasoning=str(item.get("reasoning") or "")[:120],
        ))
    return out


def _filter_evidence_urls(urls: list[str]) -> list[str]:
    """検索結果ページなど、証拠本文として不適切な URL を除外する。"""
    out: list[str] = []
    seen: set[str] = set()
    for url in urls:
        u = (url or "").strip()
        if not u or u in seen:
            continue
        host = urlparse(u).netloc.lower()
        path = urlparse(u).path.lower()
        if "google." in host and path.startswith("/search"):
            continue
        if "bing.com" in host and path.startswith("/search"):
            continue
        if "search.yahoo." in host:
            continue
        seen.add(u)
        out.append(u)
    return out


def _json_from_text(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}


async def _call_gemini_recruitment_search(
    store_name: str,
    address: str,
    phone: str,
    brand: str,
) -> dict[str, Any]:
    """Gemini Google Search grounding で求人・採用ページ候補を探す。"""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"error": "gemini_api_key_missing"}
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        return {"error": "google_genai_not_installed"}

    query_bits = [
        f"ブランド: {brand}",
        f"店舗名: {store_name}",
        f"住所: {address}",
    ]
    if phone:
        query_bits.append(f"電話番号: {phone}")
    user = (
        "\n".join(query_bits)
        + "\n\nこの店舗の求人・採用ページに記載された会社名/雇用主/運営会社を調べてください。"
        + "\n優先検索語: 会社名 雇用主 運営会社 求人 採用 アルバイト パート"
    )

    try:
        client = genai.Client(api_key=api_key)
        config = types.GenerateContentConfig(
            system_instruction=_GEMINI_SYSTEM,
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0.0,
        )
        resp = await asyncio.to_thread(
            client.models.generate_content,
            model=os.getenv("GEMINI_MODEL", "gemini-2.5-pro"),
            contents=user,
            config=config,
        )
    except Exception as e:
        return {"error": f"gemini_call: {e}"}

    data = _json_from_text(getattr(resp, "text", "") or "")
    try:
        chunks = (
            resp.candidates[0].grounding_metadata.grounding_chunks
            if resp.candidates else []
        )
        extra_urls = [c.web.uri for c in chunks if c.web and c.web.uri]
    except Exception:
        extra_urls = []
    if extra_urls and not data.get("candidates") and data.get("operator_name"):
        data["evidence_urls"] = list({*data.get("evidence_urls", []), *extra_urls})[:5]
    return data


def _text_from_html(html: str) -> str:
    if not html:
        return ""
    try:
        from bs4 import BeautifulSoup

        text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _iter_positions(text: str, needle: str) -> Iterable[int]:
    if not text or not needle:
        return []
    positions: list[int] = []
    start = 0
    while True:
        idx = text.find(needle, start)
        if idx < 0:
            break
        positions.append(idx)
        start = idx + max(len(needle), 1)
    return positions


def _label_near_operator(text: str, operator_name: str, radius: int = 120) -> str:
    for op_idx in _iter_positions(text, operator_name):
        window = text[max(0, op_idx - radius):op_idx + len(operator_name) + radius]
        for label in _EMPLOYER_LABELS:
            if label in window:
                return label
    return ""


def check_recruitment_evidence(
    html: str,
    *,
    source_url: str,
    operator_name: str,
    store_keys: list[str],
    max_distance: int = 1500,
    require_employer_label: bool = True,
) -> RecruitmentEvidence:
    """求人ページ本文が operator-store 関係の証拠になるかを決定論で判定する。"""
    text = _text_from_html(html)
    if not text:
        return RecruitmentEvidence(url=source_url, reject_reason="empty_html")
    if operator_name not in text:
        return RecruitmentEvidence(url=source_url, reject_reason="operator_missing")
    if not any(k in text for k in _JOB_PAGE_KEYWORDS):
        return RecruitmentEvidence(url=source_url, reject_reason="not_job_or_recruit_page")

    op_positions = list(_iter_positions(text, operator_name))
    for key in store_keys:
        for key_idx in _iter_positions(text, key):
            nearest_op = min(op_positions, key=lambda p: abs(p - key_idx))
            distance = abs(nearest_op - key_idx)
            if distance > max_distance:
                continue
            label = _label_near_operator(text, operator_name)
            if require_employer_label and not label:
                return RecruitmentEvidence(
                    url=source_url,
                    matched_store_key=key,
                    reject_reason="operator_without_employer_label",
                )
            start = max(0, min(nearest_op, key_idx) - 120)
            end = min(len(text), max(nearest_op + len(operator_name), key_idx + len(key)) + 160)
            snippet = text[start:end]
            return RecruitmentEvidence(
                found=True,
                url=source_url,
                snippet=snippet[:500],
                matched_store_key=key,
                matched_label=label,
            )
    return RecruitmentEvidence(
        url=source_url,
        reject_reason="store_key_not_near_operator",
    )


_PAGE_CRITIC_SYSTEM = """あなたはFC店舗の求人ページ証拠を検証するcritic。
与えられたsnippetだけを根拠に、候補operatorが当該店舗の雇用主または運営会社
だと読めるかを判定する。外部知識は使わない。

yes条件:
  - snippet内で候補operatorと店舗名/勤務地/ブランド店舗が同じ求人文脈にある
  - 会社名・勤務地・募集店舗・求人企業などの関係が読み取れる

no条件:
  - 候補operator名と店舗名があるだけで関係が不明
  - ブランド本部・求人媒体・広告ページで雇用主が不明

JSONだけで返す:
{"verdict":"yes|no|ambiguous","reasoning":"50字以内"}
"""


async def _llm_page_critic(
    *,
    store_name: str,
    address: str,
    brand: str,
    operator_name: str,
    snippet: str,
) -> tuple[str, str]:
    if not snippet or not operator_name:
        return "no", "empty"
    try:
        from browser_use.llm.messages import SystemMessage, UserMessage
        from pizza_delivery.providers.registry import get_provider
    except Exception as e:
        return "error", f"llm_import:{e}"

    user = (
        f"ブランド: {brand}\n"
        f"店舗名: {store_name}\n"
        f"住所: {address}\n"
        f"候補operator: {operator_name}\n\n"
        f"取得済みHTML本文snippet:\n{snippet[:1800]}\n\n"
        "このsnippetだけから、候補operatorが当該店舗の雇用主または運営会社だと読めるか?"
    )

    provider_specs: list[tuple[str, str | None]] = []
    if os.getenv("ANTHROPIC_API_KEY"):
        provider_specs.append(("anthropic", None))
    if os.getenv("GEMINI_API_KEY"):
        provider_specs.append((
            "gemini",
            os.getenv("GEMINI_PAGE_CRITIC_MODEL", "gemini-2.0-flash"),
        ))
    if not provider_specs:
        return "error", "no_llm_key"

    last_error = ""
    for provider_name, model in provider_specs:
        try:
            provider = get_provider(provider_name)
            kwargs = {"model": model} if model else {}
            llm = provider.make_llm(**kwargs)
            resp = await llm.ainvoke([
                SystemMessage(content=_PAGE_CRITIC_SYSTEM),
                UserMessage(content=user),
            ])
        except Exception as e:
            last_error = f"{provider_name}:{e}"
            continue

        raw = (
            getattr(resp, "completion", None)
            or getattr(resp, "content", None)
            or str(resp)
        )
        if hasattr(raw, "content"):
            raw = raw.content
        data = _json_from_text(str(raw) if raw is not None else "")
        verdict = str(data.get("verdict") or "").strip().lower()
        reason = str(data.get("reasoning") or "")[:80]
        if verdict in {"yes", "no", "ambiguous"}:
            return verdict, reason
        last_error = f"{provider_name}:bad_json"
    return "error", last_error or "llm_failed"


async def _fetch_evidence_url(url: str) -> str:
    from pizza_delivery.scrapling_fetcher import ScraplingFetcher

    fetcher = ScraplingFetcher(timeout_static_sec=10.0, timeout_dynamic_sec=8.0)
    html = await asyncio.to_thread(fetcher.fetch_static, url)
    # 求人サイトは bot block/SPA が多く、全国実行で dynamic fetch が詰まりやすい。
    # 明示的に有効化された時だけ短 timeout の dynamic fallback を使う。
    if (not html or len(html) < 500) and os.getenv("ENABLE_RECRUITMENT_DYNAMIC_FETCH") == "1":
        html = await asyncio.to_thread(fetcher.fetch_dynamic, url) or html or ""
    return html or ""


async def _find_recruitment_evidence(
    candidate: RecruitmentCandidate,
    store_keys: list[str],
    *,
    store_name: str = "",
    address: str = "",
    brand: str = "",
    llm_page_critic: bool = False,
) -> tuple[RecruitmentEvidence, list[RecruitmentEvidenceAttempt]]:
    last = RecruitmentEvidence(reject_reason="no_urls")
    attempts: list[RecruitmentEvidenceAttempt] = []
    for url in candidate.evidence_urls[:5]:
        try:
            html = await _fetch_evidence_url(url)
        except Exception as e:
            last = RecruitmentEvidence(url=url, reject_reason=f"fetch_error:{e}")
            attempts.append(RecruitmentEvidenceAttempt(
                candidate_operator=candidate.operator_name,
                candidate_confidence=candidate.confidence,
                source_type=candidate.source_type,
                url=url,
                fetched=False,
                found=False,
                reject_reason=last.reject_reason,
            ))
            continue
        ev = check_recruitment_evidence(
            html,
            source_url=url,
            operator_name=candidate.operator_name,
            store_keys=store_keys,
        )
        attempts.append(RecruitmentEvidenceAttempt(
            candidate_operator=candidate.operator_name,
            candidate_confidence=candidate.confidence,
            source_type=candidate.source_type,
            url=url,
            fetched=bool(html),
            found=ev.found,
            reject_reason=ev.reject_reason,
            matched_store_key=ev.matched_store_key,
            matched_label=ev.matched_label,
            snippet=ev.snippet[:500],
        ))
        if ev.found:
            return ev, attempts
        if llm_page_critic and ev.reject_reason == "operator_without_employer_label":
            loose_ev = check_recruitment_evidence(
                html,
                source_url=url,
                operator_name=candidate.operator_name,
                store_keys=store_keys,
                require_employer_label=False,
            )
            if loose_ev.found:
                verdict, reason = await _llm_page_critic(
                    store_name=store_name,
                    address=address,
                    brand=brand,
                    operator_name=candidate.operator_name,
                    snippet=loose_ev.snippet,
                )
                if verdict == "yes":
                    loose_ev.matched_label = f"llm_page_critic:{reason}"
                    attempts.append(RecruitmentEvidenceAttempt(
                        candidate_operator=candidate.operator_name,
                        candidate_confidence=candidate.confidence,
                        source_type=candidate.source_type,
                        url=url,
                        fetched=bool(html),
                        found=True,
                        reject_reason="",
                        matched_store_key=loose_ev.matched_store_key,
                        matched_label=loose_ev.matched_label,
                        snippet=loose_ev.snippet[:500],
                    ))
                    return loose_ev, attempts
                loose_ev.found = False
                loose_ev.reject_reason = f"llm_page_critic_{verdict}:{reason}"
                ev = loose_ev
        last = ev
    return last, attempts


def _load_franchisor_blocklist() -> set[str]:
    from pizza_delivery.normalize import canonical_key
    from pizza_delivery.registry_expander import _load_known_franchisor_names

    return {canonical_key(n) for n in _load_known_franchisor_names()}


def _is_blocked_operator(name: str, blocklist: set[str]) -> bool:
    from pizza_delivery.normalize import canonical_key

    return canonical_key(name) in blocklist


def _verify_houjin_exact(name: str):
    from pizza_delivery.houjin_csv import HoujinCSVIndex
    from pizza_delivery.normalize import canonical_key

    idx = HoujinCSVIndex()
    recs = idx.search_by_name(name, limit=10, active_only=True)
    if not recs:
        recs = idx.search_by_name(name, limit=10, active_only=False)
    target = canonical_key(name)
    for r in recs:
        if r.name == name or canonical_key(r.name) == target:
            return r
    return None


async def research_one_store(
    place_id: str,
    store_name: str,
    address: str,
    phone: str,
    brand: str,
    *,
    franchisor_blocklist: set[str] | None = None,
    llm_page_critic: bool = False,
) -> RecruitmentProposal:
    p = RecruitmentProposal(
        place_id=place_id,
        store_name=store_name,
        address=address,
        phone=phone,
        brand=brand,
    )
    data = await _call_gemini_recruitment_search(store_name, address, phone, brand)
    if "error" in data:
        p.reject_reason = str(data["error"])
        return p
    p.candidates = parse_recruitment_candidates(data)
    if not p.candidates:
        p.reject_reason = "no_candidates"
        return p

    block = franchisor_blocklist if franchisor_blocklist is not None else _load_franchisor_blocklist()
    store_keys = build_store_keys(store_name, address, phone, brand)
    if not store_keys:
        p.reject_reason = "no_store_keys"
        return p

    for cand in sorted(p.candidates, key=lambda c: c.confidence, reverse=True):
        if _is_blocked_operator(cand.operator_name, block):
            p.reject_reason = f"blocked_franchisor:{cand.operator_name}"
            continue
        ev, attempts = await _find_recruitment_evidence(
            cand,
            store_keys,
            store_name=store_name,
            address=address,
            brand=brand,
            llm_page_critic=llm_page_critic,
        )
        p.evidence_attempts.extend(attempts)
        p.evidence = ev
        if not ev.found:
            p.reject_reason = ev.reject_reason or "evidence_rejected"
            continue
        rec = _verify_houjin_exact(cand.operator_name)
        if rec is None:
            p.reject_reason = "houjin_no_exact_match"
            continue
        p.evidence = ev
        p.final_operator = rec.name
        p.final_corp = rec.corporate_number
        p.accepted = True
        p.reject_reason = ""
        return p

    if not p.reject_reason:
        p.reject_reason = "all_candidates_rejected"
    return p


def _load_target_rows(
    db_path: str | Path,
    brand: str,
    max_stores: int,
    offset: int = 0,
) -> list[tuple]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT s.place_id, s.name, COALESCE(s.address,''), COALESCE(s.phone,'')
            FROM stores s
            WHERE s.brand = ?
              AND s.address != ''
              AND s.place_id NOT IN (
                SELECT os.place_id FROM operator_stores os
                WHERE os.operator_name != ''
                  AND COALESCE(os.operator_type,'') NOT IN ('franchisor')
              )
            ORDER BY
              CASE WHEN COALESCE(s.phone,'') != '' THEN 0 ELSE 1 END,
              s.place_id
            """,
            (brand,),
        ).fetchall()
    finally:
        conn.close()
    if offset > 0:
        rows = rows[offset:]
    if max_stores > 0:
        rows = rows[:max_stores]
    return rows


def _apply_accepted(db_path: str | Path, proposals: list[RecruitmentProposal]) -> int:
    conn = sqlite3.connect(db_path)
    applied = 0
    try:
        for p in proposals:
            if not p.accepted:
                continue
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO operator_stores
                  (operator_name, place_id, brand, operator_type, confidence,
                   discovered_via, verification_score, corporate_number,
                   verification_source)
                VALUES (?, ?, ?, 'franchisee', 0.82,
                        'recruitment_search_houjin_verified', 1.0, ?,
                        'houjin_csv')
                """,
                (p.final_operator, p.place_id, p.brand, p.final_corp),
            )
            applied += cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return applied


def apply_accepted_from_proposal_json(db_path: str | Path, json_path: str | Path) -> int:
    """既存 proposal JSON の accepted row だけを DB へ反映する。

    dry-run で evidence + houjin verify まで通った結果を、再検索せず採用する用途。
    JSON はこの pipeline が生成したものに限定する。
    """
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    proposals: list[RecruitmentProposal] = []
    for item in data.get("proposals") or []:
        if not item.get("accepted"):
            continue
        if not item.get("final_operator") or not item.get("final_corp"):
            continue
        ev_raw = item.get("evidence") or {}
        p = RecruitmentProposal(
            place_id=str(item.get("place_id") or ""),
            store_name=str(item.get("store_name") or ""),
            address=str(item.get("address") or ""),
            phone=str(item.get("phone") or ""),
            brand=str(item.get("brand") or ""),
            evidence=RecruitmentEvidence(
                found=bool(ev_raw.get("found")),
                url=str(ev_raw.get("url") or ""),
                snippet=str(ev_raw.get("snippet") or ""),
                matched_store_key=str(ev_raw.get("matched_store_key") or ""),
                matched_label=str(ev_raw.get("matched_label") or ""),
                reject_reason=str(ev_raw.get("reject_reason") or ""),
            ),
            final_operator=str(item.get("final_operator") or ""),
            final_corp=str(item.get("final_corp") or ""),
            accepted=True,
        )
        if p.place_id and p.brand:
            proposals.append(p)
    return _apply_accepted(db_path, proposals)


async def recruitment_research_brand(
    db_path: str | Path,
    *,
    brand: str,
    max_stores: int = 20,
    offset: int = 0,
    dry_run: bool = False,
    concurrency: int = 2,
    llm_page_critic: bool = False,
) -> tuple[RecruitmentStats, list[RecruitmentProposal]]:
    rows = _load_target_rows(db_path, brand, max_stores, offset=offset)
    stats = RecruitmentStats(target_stores=len(rows))
    block = _load_franchisor_blocklist()
    sem = asyncio.Semaphore(concurrency)

    async def _task(row: tuple) -> RecruitmentProposal:
        pid, name, addr, phone = row
        async with sem:
            return await research_one_store(
                pid,
                name,
                addr,
                phone,
                brand,
                franchisor_blocklist=block,
                llm_page_critic=llm_page_critic,
            )

    proposals = await asyncio.gather(*(_task(r) for r in rows))
    for p in proposals:
        stats.gemini_called += 1
        stats.candidates_returned += len(p.candidates)
        if p.evidence.found:
            stats.evidence_verified += 1
        if p.final_corp:
            stats.houjin_verified += 1
        if p.accepted:
            stats.accepted += 1
        else:
            stats.rejected.append(f"{p.place_id}:{p.reject_reason}")

    if not dry_run:
        _apply_accepted(db_path, proposals)
    return stats, proposals


async def recruitment_research_many(
    db_path: str | Path,
    *,
    brands: list[str],
    max_stores: int = 20,
    offset: int = 0,
    dry_run: bool = False,
    store_concurrency: int = 2,
    brand_concurrency: int = 3,
    llm_page_critic: bool = False,
) -> tuple[dict[str, RecruitmentStats], list[RecruitmentProposal]]:
    """複数ブランドを横断並列で処理する。"""
    sem = asyncio.Semaphore(max(1, brand_concurrency))
    stats_by_brand: dict[str, RecruitmentStats] = {}
    all_proposals: list[RecruitmentProposal] = []

    async def _run_brand(b: str) -> tuple[str, RecruitmentStats, list[RecruitmentProposal]]:
        async with sem:
            stats, proposals = await recruitment_research_brand(
                db_path,
                brand=b,
                max_stores=max_stores,
                offset=offset,
                dry_run=dry_run,
                concurrency=store_concurrency,
                llm_page_critic=llm_page_critic,
            )
            return b, stats, proposals

    results = await asyncio.gather(*(_run_brand(b) for b in brands))
    for b, stats, proposals in results:
        stats_by_brand[b] = stats
        all_proposals.extend(proposals)
    return stats_by_brand, all_proposals


def _parse_brands(brand: str, brands: str) -> list[str]:
    raw = brands or brand
    if not raw:
        return TARGET_BRANDS
    out: list[str] = []
    for part in raw.split(","):
        b = part.strip()
        if b and b not in out:
            out.append(b)
    return out


def _proposal_to_dict(p: RecruitmentProposal) -> dict[str, Any]:
    d = asdict(p)
    d["candidate_count"] = len(p.candidates)
    return d


def _sidecar_path(out_path: Path, suffix: str) -> Path:
    if out_path.suffix:
        return out_path.with_name(f"{out_path.stem}-{suffix}.csv")
    return out_path.with_name(f"{out_path.name}-{suffix}.csv")


def _candidate_rows(proposal_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for p in proposal_dicts:
        candidates = p.get("candidates") or []
        if not candidates:
            rows.append({
                "brand": p.get("brand", ""),
                "place_id": p.get("place_id", ""),
                "store_name": p.get("store_name", ""),
                "address": p.get("address", ""),
                "phone": p.get("phone", ""),
                "accepted": p.get("accepted", False),
                "proposal_reject_reason": p.get("reject_reason", ""),
                "candidate_operator": "",
                "candidate_confidence": "",
                "source_type": "",
                "evidence_url": "",
                "final_operator": p.get("final_operator", ""),
                "final_corp": p.get("final_corp", ""),
            })
            continue
        for c in candidates:
            urls = c.get("evidence_urls") or [""]
            for url in urls:
                rows.append({
                    "brand": p.get("brand", ""),
                    "place_id": p.get("place_id", ""),
                    "store_name": p.get("store_name", ""),
                    "address": p.get("address", ""),
                    "phone": p.get("phone", ""),
                    "accepted": p.get("accepted", False),
                    "proposal_reject_reason": p.get("reject_reason", ""),
                    "candidate_operator": c.get("operator_name", ""),
                    "candidate_confidence": c.get("confidence", ""),
                    "source_type": c.get("source_type", ""),
                    "evidence_url": url,
                    "final_operator": p.get("final_operator", ""),
                    "final_corp": p.get("final_corp", ""),
                })
    return rows


def _attempt_rows(proposal_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for p in proposal_dicts:
        attempts = p.get("evidence_attempts") or []
        if attempts:
            for a in attempts:
                rows.append({
                    "brand": p.get("brand", ""),
                    "place_id": p.get("place_id", ""),
                    "store_name": p.get("store_name", ""),
                    "address": p.get("address", ""),
                    "phone": p.get("phone", ""),
                    "accepted": p.get("accepted", False),
                    "proposal_reject_reason": p.get("reject_reason", ""),
                    "candidate_operator": a.get("candidate_operator", ""),
                    "candidate_confidence": a.get("candidate_confidence", ""),
                    "source_type": a.get("source_type", ""),
                    "evidence_url": a.get("url", ""),
                    "fetched": a.get("fetched", False),
                    "found": a.get("found", False),
                    "attempt_reject_reason": a.get("reject_reason", ""),
                    "matched_store_key": a.get("matched_store_key", ""),
                    "matched_label": a.get("matched_label", ""),
                    "snippet": a.get("snippet", ""),
                })
            continue

        # 旧 JSON には attempt 履歴がない。候補 URL を proposal の reject reason 付きで残す。
        for r in _candidate_rows([p]):
            accepted = bool(p.get("accepted"))
            rows.append({
                **r,
                "fetched": "",
                "found": accepted,
                "attempt_reject_reason": "" if accepted else r.get("proposal_reject_reason", ""),
                "matched_store_key": (p.get("evidence") or {}).get("matched_store_key", ""),
                "matched_label": (p.get("evidence") or {}).get("matched_label", ""),
                "snippet": (p.get("evidence") or {}).get("snippet", ""),
            })
    return rows


def _accepted_rows(proposal_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for p in proposal_dicts:
        if not p.get("accepted"):
            continue
        ev = p.get("evidence") or {}
        rows.append({
            "brand": p.get("brand", ""),
            "place_id": p.get("place_id", ""),
            "store_name": p.get("store_name", ""),
            "address": p.get("address", ""),
            "phone": p.get("phone", ""),
            "final_operator": p.get("final_operator", ""),
            "final_corp": p.get("final_corp", ""),
            "evidence_url": ev.get("url", ""),
            "matched_store_key": ev.get("matched_store_key", ""),
            "matched_label": ev.get("matched_label", ""),
            "snippet": ev.get("snippet", ""),
        })
    return rows


_CANDIDATE_FIELDS = [
    "brand", "place_id", "store_name", "address", "phone", "accepted",
    "proposal_reject_reason", "candidate_operator", "candidate_confidence",
    "source_type", "evidence_url", "final_operator", "final_corp",
]
_ATTEMPT_FIELDS = [
    "brand", "place_id", "store_name", "address", "phone", "accepted",
    "proposal_reject_reason", "candidate_operator", "candidate_confidence",
    "source_type", "evidence_url", "fetched", "found", "attempt_reject_reason",
    "matched_store_key", "matched_label", "snippet",
]
_ACCEPTED_FIELDS = [
    "brand", "place_id", "store_name", "address", "phone", "final_operator",
    "final_corp", "evidence_url", "matched_store_key", "matched_label", "snippet",
]


def _write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    default_fields: list[str] | None = None,
) -> None:
    if not rows:
        if not default_fields:
            path.write_text("", encoding="utf-8")
            return
        with path.open("w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=default_fields).writeheader()
        return
    fields: list[str] = list(default_fields or [])
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def write_recruitment_sidecars(out_path: str | Path, proposal_dicts: list[dict[str, Any]]) -> dict[str, str]:
    """取捨選択用の広めの調査ログを JSON の横に CSV で保存する。

    DB 反映用の verified data ではなく、レビュー用 triage artifact。
    国税庁未一致・fetch 失敗・期限切れ URL も捨てない。
    """
    out = Path(out_path)
    candidate_rows = _candidate_rows(proposal_dicts)
    attempt_rows = _attempt_rows(proposal_dicts)
    failed_rows = [
        r for r in attempt_rows
        if str(r.get("found", "")).lower() not in {"true", "1"}
    ]
    unverified_rows = [
        r for r in candidate_rows
        if str(r.get("accepted", "")).lower() not in {"true", "1"}
    ]
    accepted_rows = _accepted_rows(proposal_dicts)

    paths = {
        "candidates": str(_sidecar_path(out, "candidates")),
        "failed_urls": str(_sidecar_path(out, "failed-urls")),
        "unverified": str(_sidecar_path(out, "unverified")),
        "accepted": str(_sidecar_path(out, "accepted")),
    }
    _write_csv(Path(paths["candidates"]), candidate_rows, default_fields=_CANDIDATE_FIELDS)
    _write_csv(Path(paths["failed_urls"]), failed_rows, default_fields=_ATTEMPT_FIELDS)
    _write_csv(Path(paths["unverified"]), unverified_rows, default_fields=_CANDIDATE_FIELDS)
    _write_csv(Path(paths["accepted"]), accepted_rows, default_fields=_ACCEPTED_FIELDS)
    return paths


def _main() -> None:
    ap = argparse.ArgumentParser(
        description="求人・採用ページ search + 本文 gate + 国税庁 verify で operator 特定"
    )
    ap.add_argument("--brand", default="", help="対象ブランド")
    ap.add_argument("--brands", default="", help="カンマ区切りブランド。空なら14ブランド")
    ap.add_argument("--db", default="var/pizza.sqlite")
    ap.add_argument("--max-stores", type=int, default=20,
                    help="各ブランドのGemini呼出上限。0で全件")
    ap.add_argument("--offset", type=int, default=0,
                    help="各ブランドの未特定店舗リストの先頭から skip する件数")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--concurrency", type=int, default=2)
    ap.add_argument("--brand-concurrency", type=int, default=3)
    ap.add_argument(
        "--llm-page-critic",
        action="store_true",
        help="Scraplingで取得したHTML snippetをLLMに判定させる fallback を使う",
    )
    ap.add_argument("--out", default="", help="proposal JSON 出力")
    ap.add_argument(
        "--export-sidecars-from",
        default="",
        help="既存 proposal JSON から candidates/failed-urls/unverified CSV だけを再生成",
    )
    ap.add_argument(
        "--apply-from",
        default="",
        help="既存 proposal JSON の accepted row だけを DB に反映",
    )
    args = ap.parse_args()

    if args.apply_from:
        applied = apply_accepted_from_proposal_json(args.db, args.apply_from)
        print(f"✅ applied accepted proposals: {applied}")
        return

    if args.export_sidecars_from:
        src = Path(args.export_sidecars_from)
        data = json.loads(src.read_text(encoding="utf-8"))
        proposal_dicts = data.get("proposals") or []
        paths = write_recruitment_sidecars(src, proposal_dicts)
        print(f"📄 candidates: {paths['candidates']}")
        print(f"📄 failed URLs: {paths['failed_urls']}")
        print(f"📄 unverified: {paths['unverified']}")
        print(f"📄 accepted: {paths['accepted']}")
        return

    if not Path(args.db).exists():
        raise SystemExit(f"db not found: {args.db}")

    brands = _parse_brands(args.brand, args.brands)
    stats_by_brand, all_proposals = asyncio.run(recruitment_research_many(
        args.db,
        brands=brands,
        max_stores=args.max_stores,
        offset=args.offset,
        dry_run=args.dry_run,
        store_concurrency=args.concurrency,
        brand_concurrency=args.brand_concurrency,
        llm_page_critic=args.llm_page_critic,
    ))
    all_stats: dict[str, Any] = {b: asdict(stats_by_brand[b]) for b in brands}
    for b in brands:
        stats = stats_by_brand[b]
        print(f"✅ recruitment-research {'dry-run' if args.dry_run else 'apply'}  brand={b}")
        print(f"   target_stores       = {stats.target_stores}")
        print(f"   candidates_returned = {stats.candidates_returned}")
        print(f"   evidence_verified   = {stats.evidence_verified}")
        print(f"   houjin_verified     = {stats.houjin_verified}")
        print(f"   ACCEPTED            = {stats.accepted}")
        for r in stats.rejected[:3]:
            print(f"   reject: {r}")

    if args.out:
        p = Path(args.out)
        p.parent.mkdir(parents=True, exist_ok=True)
        proposal_dicts = [_proposal_to_dict(p) for p in all_proposals]
        with p.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "stats": all_stats,
                    "proposals": proposal_dicts,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"📄 proposals: {args.out}")
        paths = write_recruitment_sidecars(p, proposal_dicts)
        print(f"📄 candidates: {paths['candidates']}")
        print(f"📄 failed URLs: {paths['failed_urls']}")
        print(f"📄 unverified: {paths['unverified']}")
        print(f"📄 accepted: {paths['accepted']}")


if __name__ == "__main__":
    _main()
