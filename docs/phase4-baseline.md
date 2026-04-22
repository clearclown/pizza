# Phase 4 Baseline 測定結果

**測定日時**: 2026-04 (Phase 4 Step 2)
**Provider / Model**: Gemini 2.5 Flash
**Prompt**: judge.yaml v2 (Phase 3)
**Golden**: 30 件 (Phase 4 v2 schema)

## 3 軸 Accuracy

| Axis | Count | Accuracy | 目標 (Phase 4) |
|---|---|---|---|
| operation_type | 26/30 | **86.7%** | ≥ 95% |
| franchisor_name | 24/30 | **80.0%** | ≥ 80% (達成) |
| **franchisee_name** ⭐ | **0/0** | **測定不能** | 0 → 目標 ≥ 50% |

### 所見

1. **operation_type**: Phase 3 の 97% から低下した。原因は **mixed 値の追加** に v2 プロンプトが対応していなかったこと (4 件の誤判定は全て `mixed → direct/franchisee` のミス):
   - いきなりステーキ: true=mixed → pred=franchisee
   - マクドナルド: true=mixed → pred=direct
   - 吉野家: true=mixed → pred=direct
   - ビッグエコー: true=mixed → pred=franchisee

2. **franchisor_name**: 表記揺れと親会社/ブランド区別の課題:
   - ジーユー: true=「株式会社ジーユー」 vs pred=「株式会社ファーストリテイリング」 (親会社名を返した)
   - H&M: 全角半角空白・中黒の違い ("ヘネス アンド マウリッツ" vs "ヘネス・アンド・マウリッツ")
   - エニタイムフィットネス: true=「株式会社ファストフィットネスジャパン」 vs pred=「株式会社Fast Fitness Japan」 (カタカナ vs 英語)
   - ガスト: pred が空 (LLM が franchisor を抽出しなかった)
   - ロイヤルホスト: true=「ロイヤルホールディングス株式会社」 vs pred=「ロイヤルホスト株式会社」 (持株会社 vs 事業会社)

3. **franchisee_name = 測定不能**: golden CSV の全 30 行で `true_franchisee_name` が空。これは **手作業で調べる限界** を示している:
   - コンビニ (セブン, ファミマ, ローソン) の個別 franchisee は中小企業が多く、store locator を訪問しないと分からない
   - 24h ジム (エニタイム) の個別運営会社も同様
   - この情報こそ **browser-use で自律取得すべきもの** — Phase 4 Case C の本質

## 結論: Phase 4 Step 3 は **Case B + C** の併用へ

**Step 3-B (軽改善)**: プロンプト v3 に "mixed" ガイダンス追加 → operation_type を 95%+ に戻す

**Step 3-C (本線)**: browser-use で store-locator / 会社概要ページ を巡回する専用 Agent を実装
- golden に franchisee_name を後付けで充実させる道も並行で可能に
- **これが PI-ZZA の原点「自律ブラウジング」の真の発揮**

## 数値記録

```
==================================================================
Provider: gemini  Model: gemini-2.5-flash
==================================================================
[AXIS 1] operation_type : 26/30 = 86.7%
[AXIS 2] franchisor_name: 24/30 = 80.0%
[AXIS 3] franchisee_name: 0/0   = 0.0%  ⭐ Phase 4 KPI
==================================================================

❌ operation_type の誤判定 (全て mixed の取り違え):
  いきなりステーキ      true=mixed  pred=franchisee
  マクドナルド         true=mixed  pred=direct
  吉野家            true=mixed  pred=direct
  ビッグエコー         true=mixed  pred=franchisee

❌ franchisor_name の不一致 (6 件):
  ジーユー           true='株式会社ジーユー'  pred='株式会社ファーストリテイリング'
  H&M            true='エイチ アンド エム...'  pred='株式会社H&Mヘネス・アンド・マウリッツ・ジャパン'
  エニタイムフィットネス x2  カタカナ vs 英語表記
  ガスト            pred='' (抽出失敗)
  ロイヤルホスト        true='HD' vs pred='事業会社'

franchisee_name はラベルが無いため測定できず。
→ browser-use で真値を取得する Step 3-C へ進む。
==================================================================
```

## Cost & Time

- 所要: 3 分 9 秒 (30 件 × Gemini Flash)
- コスト: 実質 $0 (Gemini Flash 無料枠)
