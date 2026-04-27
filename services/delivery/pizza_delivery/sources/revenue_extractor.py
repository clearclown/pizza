"""売上高を HTML から決定論抽出 (Phase 25)。

対象レイアウト:
  1. 『売上高: 846億円』型 (inline)
  2. 『売上高 2024年3月期 846億円 / 2023年3月期 798億円』型 (2 期併記)
  3. 表形式 <table> で年度 × 金額 (百万円 / 億円) の行列

単位正規化:
  - 『億円』 → × 10^8
  - 『百万円』 → × 10^6
  - 『千円』 → × 10^3
  - 単位記号なし or 『円』 → そのまま

返却: RevenueFinding(current_jpy, previous_jpy, observed_at)。
当期/前期は「年度が大きい方が current」のシンプルルール。
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class RevenueFinding:
    current_jpy: int = 0
    previous_jpy: int = 0
    observed_at: str = ""  # "2024-03期" 等
    source_url: str = ""

    @property
    def empty(self) -> bool:
        return self.current_jpy == 0 and self.previous_jpy == 0


_UNIT_MULT = {
    "億円": 10**8,
    "百万円": 10**6,
    "千円": 10**3,
    "万円": 10**4,
    "円": 1,
}


def _normalize_amount(amount_str: str, unit: str) -> int:
    """"1,234" + "億円" → 123_400_000_000 円。"""
    s = amount_str.replace(",", "").replace(",", "").strip()
    if not s.replace(".", "").isdigit():
        return 0
    try:
        val = float(s)
    except ValueError:
        return 0
    mult = _UNIT_MULT.get(unit, 1)
    return int(val * mult)


# 「売上高 1,234 百万円」 または 「売上高: 846億円」
# 途中に年度 (2024年3月期) が挟まれてもよい。non-greedy で最短 match。
_RE_INLINE_REVENUE = re.compile(
    r"売上(?:高|収益|額)?[:：]"
    r"[^<\n]{0,60}?"
    r"([0-9][0-9,\.]*)\s*"
    r"(億円|百万円|千円|万円|円)"
)
# ラベル無しで直接数値+単位を読む場合 (「売上高 500 百万円」)
_RE_SIMPLE_REVENUE = re.compile(
    r"売上(?:高|収益|額)?[\s　]+"
    r"(?:約[\s　]*)?"
    r"([0-9][0-9,\.]*)\s*"
    r"(億円|百万円|千円|万円|円)"
)

# 「2024年3月期」「2024年度」「令和6年3月期」等を fiscal period として拾う
_RE_FISCAL_PERIOD = re.compile(
    r"((?:20\d{2}|令和\d+|平成\d+)年\s*(?:\d{1,2}月期|度))"
)


def extract_revenue_from_html(html: str, *, source_url: str = "") -> RevenueFinding:
    """HTML から売上高と期を抽出。複数年出現なら年度が新しい方 = current。

    ロジック:
      - fiscal period と inline revenue を順序付きで正規表現 hit
      - 同じ文脈 (距離 200 chars 以内) にペアがあれば (period, amount) として採用
      - 最大 2 件 ((latest, previous)) を返す
    """
    if not html:
        return RevenueFinding(source_url=source_url)

    findings: list[tuple[str, int]] = []  # (period, jpy)
    # 全 revenue match の位置を集める (inline + simple の両方を試す)
    rev_matches = list(_RE_INLINE_REVENUE.finditer(html))
    if not rev_matches:
        rev_matches = list(_RE_SIMPLE_REVENUE.finditer(html))
    if not rev_matches:
        return RevenueFinding(source_url=source_url)

    # 各 revenue match について fiscal period を紐付ける:
    #   1. match 文字列自体 (ラベル〜金額) に period があれば最優先 (「売上高: 2024年3月期 846億円」)
    #   2. 近傍 (前方 200 chars) に period があればそれを使う (表構造)
    for rm in rev_matches:
        amount = _normalize_amount(rm.group(1), rm.group(2))
        if amount <= 0:
            continue
        # 妥当性 filter: 1 億円未満は除外 (FC 本部でそれは考えにくい)
        if amount < 10**8:
            continue
        # 妥当性 filter: 10 兆円超えは桁間違いの可能性 → 捨てる
        if amount > 10**13:
            continue
        # 優先 1: match 文字列内の period
        period = ""
        inside = _RE_FISCAL_PERIOD.search(rm.group(0))
        if inside:
            period = inside.group(1)
        else:
            # 優先 2: 前方 200 chars の period
            context = html[max(0, rm.start() - 200): rm.start()]
            last = None
            for pm in _RE_FISCAL_PERIOD.finditer(context):
                last = pm
            if last:
                period = last.group(1)
        findings.append((period, amount))

    if not findings:
        return RevenueFinding(source_url=source_url)

    # 同じ period に複数 match あれば最後を優先 (HTML 下部にまとまった表がある前提)
    seen_periods: dict[str, int] = {}
    order: list[str] = []
    for period, amount in findings:
        key = period or f"__unkey_{len(seen_periods)}__"
        if key not in seen_periods:
            order.append(key)
        seen_periods[key] = amount

    # period で sort (新しい年が先頭) — 数字部分で昇順→降順
    def _year_key(p: str) -> int:
        m = re.search(r"20(\d{2})", p)
        if m:
            return int(m.group(1))
        m = re.search(r"令和(\d+)", p)
        if m:
            return 18 + int(m.group(1))  # 令和元 (2019) → 19
        m = re.search(r"平成(\d+)", p)
        if m:
            return int(m.group(1)) - 12  # 平成31 → 19
        return -1

    items = sorted(
        ((p, seen_periods[p]) for p in order),
        key=lambda x: _year_key(x[0]),
        reverse=True,
    )

    out = RevenueFinding(source_url=source_url)
    if len(items) >= 1:
        out.current_jpy = items[0][1]
        out.observed_at = items[0][0].split("_")[0] if not items[0][0].startswith("__") else ""
    if len(items) >= 2:
        out.previous_jpy = items[1][1]
    return out
