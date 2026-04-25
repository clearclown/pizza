"""FC 加盟店募集 LP の URL を公式 HP から自動発見 (Phase 25)。

operator_spider._find_store_list_links の pattern を流用しつつ、
anchor text / href 両方で FC 募集系キーワードを認識する。

検出ルール:
  - anchor text に「加盟店募集」「FC 募集」「フランチャイズ募集」「オーナー募集」等
  - href に "/franchise/" "/fc/" "/recruit/owner" "/contract/" 等

複数 hit 時は confidence 上位 1 件を返す (LP を 1 URL 決めればよいので)。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urljoin


@dataclass
class FCRecruitmentFinding:
    url: str = ""
    anchor_text: str = ""
    confidence: float = 0.0
    source_url: str = ""

    @property
    def empty(self) -> bool:
        return not self.url


_ANCHOR_KEYWORDS_HIGH = (
    "加盟店募集", "フランチャイズ募集", "FC募集", "FC 募集",
    "オーナー募集", "フランチャイジー募集", "加盟店オーナー",
    "独立開業", "パートナー募集", "開業をお考えの",
)
_ANCHOR_KEYWORDS_MED = (
    "加盟店", "フランチャイズ", "オーナー", "開業",
    "FC", "フランチャイジー",
)
_HREF_HINTS_HIGH = (
    "/franchise/", "/fc/", "/recruit/owner",
    "/recruit/franchise", "/contract/", "/join/",
    "/owner/", "/partner/",
)
_HREF_HINTS_MED = (
    "franchise", "owner", "recruit",
)

# 誤 match を避けるブロックキーワード (採用情報 LP ではない)
_ANCHOR_BLOCKLIST = (
    "新卒採用", "中途採用", "社員募集", "アルバイト", "パート",
    "求人情報",
)


def extract_fc_recruitment_url(
    html: str, *, base_url: str = ""
) -> FCRecruitmentFinding:
    """HTML の <a> を走査し FC 募集 LP を 1 件特定する。

    スコアリング:
      anchor_text HIGH match +0.5, MED match +0.2
      href HIGH match +0.3, MED match +0.1
      anchor_text blocklist match → skip
    最高 confidence の 1 件を返す。
    """
    if not html:
        return FCRecruitmentFinding(source_url=base_url)

    link_re = re.compile(
        r'<a\s+[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    best: FCRecruitmentFinding = FCRecruitmentFinding(source_url=base_url)
    for m in link_re.finditer(html):
        href = m.group(1).strip()
        anchor = re.sub(r"\s+", " ", m.group(2).strip())
        if not href or not anchor:
            continue
        if href.startswith("#") or href.startswith("mailto:"):
            continue
        if any(b in anchor for b in _ANCHOR_BLOCKLIST):
            continue

        score = 0.0
        if any(k in anchor for k in _ANCHOR_KEYWORDS_HIGH):
            score += 0.5
        elif any(k in anchor for k in _ANCHOR_KEYWORDS_MED):
            score += 0.2
        href_lower = href.lower()
        if any(h in href_lower for h in _HREF_HINTS_HIGH):
            score += 0.3
        elif any(h in href_lower for h in _HREF_HINTS_MED):
            score += 0.1

        if score < 0.3:
            continue
        if score <= best.confidence:
            continue
        abs_url = urljoin(base_url, href) if base_url else href
        best = FCRecruitmentFinding(
            url=abs_url,
            anchor_text=anchor[:80],
            confidence=score,
            source_url=base_url,
        )
    return best
