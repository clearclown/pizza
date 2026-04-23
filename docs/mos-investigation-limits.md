# Mos バーガー調査: PI-ZZA 現設計の限界と対応方針 (2026-04-23)

## 🎯 調査対象

東京都の モスバーガー 加盟店運営会社 (franchisee) を全件洗い出す。

## ❌ 現状の限界 (直接 scrape 困難)

### 公式サイト構造
| URL | 状態 | 取得可能性 |
|---|---|---|
| `www.mos.co.jp` | JS redirect → `www.mos.jp` (httpx follow 不可) | ❌ |
| `www.mos.jp/` | HTML 取得可、トップ page | △ 加盟店 list なし |
| `www.mos.jp/shop/` | 店舗検索 UI (SPA, JS 動的) | ❌ |
| `www.mos.jp/shop/detail/?shop_cd=XXXXX` | 個別店舗 detail (SPA) | ❌ |
| `mosstorecompany.jp/` | 100% 子会社 | △ 店舗 list なし |

OperatorSpider 実走結果: **全 URL で候補 0 件**。公開 page に加盟店住所 list が存在しない。

### per_store 抽出の結果
東京都 377 店舗中:
- `株式会社モスフードサービス` (本部): 86 件検出
- 個別 franchisee: **0 件**

理由: 店舗 detail ページが JS rendering で HTML に operator 情報が含まれない。
Phase 7 の親ドメイン昇格 fetch を使っても本部名しか取れない。

### Places 実データのノイズ

```
公式 URL パターン (東京都 377 店):
  https://www.burgerking.co.jp/          6 件 ← バーガーキング URL 混入
  https://www.mos.jp/shop/detail/?...    300+ 件
  https://www.mos.jp/                    2 件
  (空文字列)                              4 件
```

**バーガーキング URL の混入 (6 件)**:
- Places 側の登録データノイズ (閉店後未更新 / 併設店 / 登録誤り)
- Layer A brand filter は `displayName` ベースで判定するので、
  「店舗名=モスバーガー、URL=バーガーキング」のケースは検出不可
- **Phase 18 候補**: official_url の ドメインでも brand filter する二次レイヤ

## ✅ 唯一の情報源: Web search agent

mos 加盟店運営会社の公開情報は以下に限定:
1. ビジネスチャンス誌メガジーランキング 2024
2. モスフード IR (有価証券報告書で大口 FC の記載)
3. 各 FC 会社の自社サイト (モスバーガー運営と明記、求人などで公表)
4. 業界ブログ (marketingxstrategy.com 等)

Web search agent に以上を横断的に調査させて registry へ追記する loop が
Phase 17.4 (Registry 自動拡充) の中核。

## 🏗 Phase 18 候補 (本 phase 外)

1. **URL ドメイン ベースの二次 brand filter** — `matchesBrand` を URL でも検証
2. **SPA 対応 browser-use scrape** — 既存 `_default_browser_agent` を OperatorSpider に注入
   (Google Maps Platform ToS §3.2.3 違反回避、**公式サイトは scrape OK**)
3. **Mos 個別店舗の official_url (`shop_cd=XXXXX`) を Places details API で取得**
   して対応する phone / website_uri を参照し、個別 operator 情報へのリンクを得る

## 🎬 Auto loop の有効性確認 (Phase 17.4 出力)

```bash
$ ./bin/pizza registry-expand --brand "モスバーガー" --min-stores 2
✅ wrote 1 candidates
  株式会社モスフードサービス  (86 店舗)
```

- 自動で 1 候補 = モスフードサービス (本部) 抽出
- 人間レビュー step で「本部なので registry には入れない、operator_type=franchisor」と判定
- Web search agent の結果を手動 review → franchisee_registry.yaml へ追記 → migrate --with-registry で seed

**Loop は成立** しており、この Loop を agent で完全自動化するのが将来の進化。

## 📊 本件の Final Status

| 指標 | 値 |
|---|---|
| Mos 東京都 bake 店舗数 | 377 |
| registry 突合 | 0% (モスストアカンパニーは全国 209 店 dummy、Places 実店舗と place_id 一致せず) |
| per_store operator 検出 | モスフードサービス (本部) 86 件のみ |
| Layer A 混入 | 6 件 (BK 併設 / URL 誤登録) — 次 phase で URL filter 追加 |
| registry 拡充候補 | 自動抽出 1 社 (本部、人間レビュー除外対象) |
| Web agent 結果待ち | 進行中、合計 200-250 KB ログ |

**正直な結論**: PI-ZZA の現アーキテクチャで Mos のような「公開情報限定 + SPA 公式」 ブランドは **直接** 完全洗い出し不可。Web search agent の外部知識統合が必須。
