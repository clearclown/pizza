"""registry 登録の各 Mos 運営社の公式 URL を OperatorSpider で fetch し、
東京都の店舗記述 (住所) を抽出する。Places API の false positive を避け、
operator 側の公式情報から「東京都モス店舗を持つ事業会社」を確定するため。
"""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path

import httpx

# 各 operator の公式 URL (店舗 list or トップページ)
# registry + Web search agent 調査結果から抽出
CANDIDATES = [
    ("株式会社モスストアカンパニー", "3010701019707", "https://mosstorecompany.jp/"),
    ("株式会社ヴィアン", "5010401003406", "https://www.viens.co.jp/company/outline.html"),
    ("株式会社大和フーヅ", "5010401089998", "https://www.ymtfds.co.jp/ymfd/corporate/"),
    ("株式会社ありがとうサービス", "2500001012603", "https://www.arigatou-s.com/enterprise/"),
    ("株式会社フジタコーポレーション", "9430001054035", "https://www.fujitacorp.co.jp/main/shop-cat/02mosburger/"),
    ("株式会社三栄本社", "7390001000722", "http://www.saneihonsha.co.jp/company/"),
    ("タイホウコーポレーション株式会社", "1180001037667", "https://www.taiho-group.co.jp/company/"),
    ("株式会社フレックス", "7120101034003", "https://www.flex1980.co.jp/aboutus/company/"),
    ("株式会社ファミリーフード", "6240001031857", "https://familyfood.jp/"),
    ("株式会社みちのくジャパン", "8400001006262", "https://www.michinoku-japan.com/"),
    ("株式会社山浩商事", "1360001012164", "https://www.yamako-s.co.jp/eating-out.php"),
    ("株式会社近鉄リテーリング", "4120001023117", "https://kintetsu-retailing.co.jp/company/organization/"),
    ("有限会社タイホウグループ", "5130002005624", "https://www.taiho-group.co.jp/company/"),
]

TOKYO_RE = re.compile(r"東京都[ぁ-ん一-龯A-Za-z０-９0-9\-－ー ]+")
TOKYO_PREFIX_RE = re.compile(r"東京都(?:千代田区|中央区|港区|新宿区|文京区|台東区|墨田区|江東区|品川区|目黒区|大田区|世田谷区|渋谷区|中野区|杉並区|豊島区|北区|荒川区|板橋区|練馬区|足立区|葛飾区|江戸川区|八王子市|立川市|武蔵野市|三鷹市|青梅市|府中市|昭島市|調布市|町田市|小金井市|小平市|日野市|東村山市|国分寺市|国立市|福生市|狛江市|東大和市|清瀬市|東久留米市|武蔵村山市|多摩市|稲城市|羽村市|あきる野市|西東京市|瑞穂町|日の出町|檜原村|奥多摩町|大島町|利島村|新島村|神津島村|三宅村|御蔵島村|八丈町|青ヶ島村|小笠原村)")


async def fetch(url: str, timeout: float = 20.0) -> str:
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 PI-ZZA-research/1.0"})
            r.raise_for_status()
            return r.text
    except Exception as e:
        return f"FETCH_ERROR: {e}"


def scan_tokyo_mentions(html: str) -> tuple[int, list[str]]:
    """HTML から『東京都XX市区』住所風の文字列を抽出、最初の 10 件返す。"""
    # script/style を除去 (noise 減らし)
    cleaned = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL|re.IGNORECASE)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", "", cleaned, flags=re.DOTALL|re.IGNORECASE)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    matches = TOKYO_PREFIX_RE.findall(cleaned)
    # 重複除去
    seen = set()
    uniq: list[str] = []
    for m in matches:
        # match は prefix だけなので、前後 30 文字を切って住所候補化
        for full in TOKYO_RE.findall(cleaned):
            if full.startswith(m) and full not in seen:
                seen.add(full)
                uniq.append(full[:80])
                break
    return len(uniq), uniq[:10]


async def main() -> None:
    print("🕷️  OperatorSpider — 各 FC 公式 URL から東京都住所記述を抽出")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    results = []
    for name, corp, url in CANDIDATES:
        html = await fetch(url)
        if html.startswith("FETCH_ERROR:"):
            n, exs = 0, []
            err = html
        else:
            n, exs = scan_tokyo_mentions(html)
            err = ""
        if n > 0:
            print(f"  ✅ {name:30s} 東京都住所 {n:2d} 件  {url}")
            for ex in exs[:3]:
                print(f"      • {ex}")
        elif err:
            print(f"  ⚠️  {name:30s} fetch 失敗  {url}  {err[:80]}")
        else:
            print(f"  ❌ {name:30s} 東京都住所 0 件  {url}")
        results.append((name, corp, url, n, exs, err))

    print()
    print("━━━━ 結論: 東京都住所を公式サイトに記述している事業会社 ━━━━")
    for name, corp, url, n, exs, err in results:
        if n > 0:
            print(f"  {n:3d} 件  {name}  ({corp})")


if __name__ == "__main__":
    asyncio.run(main())
