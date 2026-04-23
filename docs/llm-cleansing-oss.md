# LLM データクレンジング OSS 調査 (2026-04-23)

> 「機械的なデータクレンジングでもいいけど、llm を噛ませて
>  クレンジングする oss なども探して確認をしてみて欲しい」

PI-ZZA の operator 正規化を強化するための OSS 候補を調査。既存の決定論的
正規化 (株式会社/㈱ 統一、bi-gram 類似度等) と国税庁法人番号 API
実在確認の間に挟む / 置き換える候補として評価。

## 候補一覧

| # | 名前 | 主機能 | PI-ZZA での使いどころ | License | 最終コミット | ⭐ |
|---|---|---|---|---|---|---|
| 1 | [**Zingg**](https://github.com/zinggAI/zingg) | ML ベースの entity resolution・名寄せ・MDM。active learning、Spark/Snowflake/Databricks 対応、LangChain 連携ガイドあり | **franchise operator 名寄せのコア**。決定論的正規化の出力を Zingg の学習データとし、曖昧ペアは LLM (LangChain 経由) に委譲 | AGPL-3.0 | 2026-04-21 | 1,185 |
| 2 | [**Splink**](https://github.com/moj-analytical-services/splink) | Fellegi-Sunter の確率的 record linkage。DuckDB/Spark/Athena backend、1 億件規模可 | 法人番号 API 確認前の候補ペア生成。解釈性が高く BI 監査要件と相性◎。LLM は境界値ペアの gatekeeper | MIT | 2026-04-22 | 2,097 |
| 3 | [**databonsai**](https://github.com/databonsai/databonsai) | LLM で categorize/transform/extract。OpenAI・**Anthropic プロバイダ組込** | 自由記述 operator 文字列 → 正規名・本店・法人番号候補の一発抽出。最小依存 | MIT | 2024-06-26 (停滞) | 489 |
| 4 | [**CleanAgent**](https://github.com/sfu-db/CleanAgent) | VLDB 2025 論文実装。マルチエージェントで clean_* 関数自動生成 | 新 franchise CSV の EDA 自動化 (列意味推定 → クレンジングコード自動生成) | research | 2025-06 | 74 |
| 5 | [**GoldenMatch**](https://github.com/benzsevern/goldenmatch) | Zero-config entity resolution、Polars ネイティブ、7,800 rec/s、**MCP/REST/A2A server 付き**、LLM scorer 同梱 | **Claude Code / Claude Desktop から MCP 経由でリアルタイム照会**。Golden Suite (Check/Flow/Match) を pipeline に | MIT | 2026-04-15 | 34 |
| 6 | [**Japanese-Company-Lexicon (JCLdic)**](https://github.com/chakki-works/Japanese-Company-Lexicon) | TIS 公開の **日本法人名辞書** + alias 生成 (株式会社/㈱/(株) 等を網羅) | 決定論層を JCLdic で増強。LLM プロンプトの few-shot に流用 | MIT | 2023-07 (辞書は安定) | 100 |

## 推奨構成: 3 層ハイブリッド

```
Layer A (現行): 決定論正規化 (株式会社/㈱、Layer A KB)
     ↓
Layer B (追加候補): JCLdic 辞書 lookup で alias 吸収
     ↓
Layer C (現行): ExpertPanel (Gemini Flash×2 + Claude critic)
     ↓
Layer D (現行): 国税庁法人番号 API 実在確認
     ↓
Layer E (追加候補): Zingg の active learning で
                    「新しい operator」vs「既知の operator」の名寄せ判定
                    境界ペアは Claude MCP (GoldenMatch) にリアルタイム照会
```

### 採用優先順位

| 段階 | 導入 | 工数 | 効果 |
|---|---|---|---|
| **即** | JCLdic (静的辞書 download + lookup 関数) | 0.5 日 | ★★★ 日本法人名の表記揺れを lexicon で吸収、LLM 不要で精度 up |
| **短期** | databonsai (Anthropic 直接) | 1 日 | ★★ 既存 Claude Critic と統合、自由記述 → 構造化 1 関数 |
| **中期** | Splink (Python) | 3-5 日 | ★★★ 確率的 record linkage、法人番号 API 引き当て前の候補生成 |
| **長期** | Zingg + LangChain / GoldenMatch MCP | 1-2 週 | ★★★★ 本格的な name resolution engine、Claude との連携が自然 |

### PI-ZZA の採用判断

1. **JCLdic は採用 (低コスト高効果)**: 日本の法人名 alias 辞書は一次データ資産として独立価値。既存 `pizza_delivery/normalize.py` の `_LEGACY_SUFFIXES` を JCLdic エントリと照合することで lexicon 化できる
2. **Splink は候補ペア生成に採用検討**: 法人番号 API の前段で「既存 operator と類似する候補」の列挙に使える。PRs Python、MIT
3. **Zingg vs GoldenMatch**: Zingg は AGPL-3.0 が注意点 (PI-ZZA が公開 OSS なら整合、SaaS 化時は非公開サービス境界要精査)。GoldenMatch は MIT + MCP server 付きで Claude Code から直接呼べる。PI-ZZA は将来 Streamlit + Claude MCP の対話的リサーチを想定するなら GoldenMatch が有利
4. **databonsai / CleanAgent は非推奨**:
   - databonsai は 2024-06 以降停滞。ラッパ自作の方が保守性高
   - CleanAgent は論文実装でプロダクション品質不足

### 次のアクション候補

1. `pizza_delivery/lexicon/jcldic.py` — JCLdic を download + lookup する薄いラッパ
2. `pizza_delivery/lexicon/__init__.py` で既存 normalize.py と統合
3. 効果検証 — golden dataset (30 行) で before/after 一致率を測る
4. Streamlit Review タブに「JCLdic 認識候補」と「LLM overrule 理由」を並べて表示

## 参考

- [Zingg + LangChain ガイド](https://www.zingg.ai/documentation-article/enhancing-llm-applications-with-identity-resolution-using-zingg-and-langchain)
- [CleanAgent VLDB 2025 paper](https://arxiv.org/abs/2403.08291)
- [GoldenMatch MCP integration](https://github.com/benzsevern/goldenpipe)
- [Awesome Entity Resolution](https://github.com/OlivierBinette/Awesome-Entity-Resolution)
