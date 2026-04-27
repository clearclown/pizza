# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
自動更新は [release-please](https://github.com/googleapis/release-please) が担います。

## 1.0.0 (2026-04-27)


### Features

* **delivery:** add mock gRPC server for Phase 1 pipeline integration ([cab2158](https://github.com/clearclown/pizza/commit/cab2158f07b45a07f16c0b453cd155d0d12baf6b))
* **delivery:** browser fallback when LLM confidence is low ([856e637](https://github.com/clearclown/pizza/commit/856e637af210f48afca6563dbf6d9ebfbe664fef))
* **dough:** implement M1 Seed — Places API (New) client + gRPC server ([dff18b2](https://github.com/clearclown/pizza/commit/dff18b22afd3270b479e1d47196d986cb9155b3a))
* **e2e:** Phase 3 integration test + mega-franchisee walkthrough docs ([6d03904](https://github.com/clearclown/pizza/commit/6d03904ad10d7aacf5d1525dafc9fbcfae7a7e81))
* **enrich:** phone 無し店舗 → 公式 URL 直接訪問 fallback 追加 ([c6874e2](https://github.com/clearclown/pizza/commit/c6874e2a17162ddebbe14096ce10d8ac6f17450b))
* **evidence:** EvidenceCollector for deterministic data extraction ([229394a](https://github.com/clearclown/pizza/commit/229394a5fcf04e5d29cc63126dd72417ddc4acd7))
* export operator-centric franchisee master ([baf1d7e](https://github.com/clearclown/pizza/commit/baf1d7e810bae46af7149a080629ec1aec2cd55c))
* **firecrawl:** self-host nuq schema 自力解決 (reverse engineered) ([8a5b475](https://github.com/clearclown/pizza/commit/8a5b475eb6ccdc0ddf6520d050844fc2ef270445))
* **gen:** generate protobuf bindings for go and python ([e3f0e90](https://github.com/clearclown/pizza/commit/e3f0e905c3a9a3e40cde0da60b2a366645bbd30b))
* **internal:** Phase 1 Green — grid, scoring, slice, retry, parser all pass ([36c946a](https://github.com/clearclown/pizza/commit/36c946aa1eb1007fae937c96637fd6201f4ac16f))
* **judge+validate-judge:** skip_domains追加 (EDINET APIエンドポイントをHEADチェック除外) ([0e246ae](https://github.com/clearclown/pizza/commit/0e246aed5044230ba20c21e7f3a0fa5fd1ff64c9))
* **judge:** add judge.yaml, golden datasets, and validate-judge CLI (PR[#9](https://github.com/clearclown/pizza/issues/9)) ([5da12be](https://github.com/clearclown/pizza/commit/5da12bed03c44d0c1163e5d0b2499197ce021801))
* **judge:** hard_rules セクション追加 (gmaps_requires_l3 + unknown_source_rejected) ([c512adb](https://github.com/clearclown/pizza/commit/c512adb8040202dfb51c4514cd7aca28dddd2222))
* **judge:** judge.yaml, golden datasets, and validate-judge CLI ([d138d53](https://github.com/clearclown/pizza/commit/d138d5337fd09decc371af1f1c50f9e6e1b80fb1))
* **judge:** judge.yaml, golden datasets, and validate-judge CLI (PR[#9](https://github.com/clearclown/pizza/issues/9)) ([d138d53](https://github.com/clearclown/pizza/commit/d138d5337fd09decc371af1f1c50f9e6e1b80fb1))
* **judge:** refuse_phrases 補完 — 体言止め形 + 英語フレーズ追加 ([58dcf18](https://github.com/clearclown/pizza/commit/58dcf18575aef300c64bb7b3c862908a2138a67c))
* **judge:** source_priority gmaps_cluster に standalone_max: 0.0 追加 ([8c06574](https://github.com/clearclown/pizza/commit/8c06574702e1484edd0b43112502551b02419a6b))
* **judge:** system_prompt キー追加 (hallucination_instruction の別名) ([c3df934](https://github.com/clearclown/pizza/commit/c3df934a5f65cfbf3104b4d7763c9121b8975367))
* **judge:** validation に count_source not in source_priority → error 追加 ([e12e238](https://github.com/clearclown/pizza/commit/e12e238f9ab3f3aead29e9207ef2960e2a3d9add))
* **layer-d:** VerifyPipeline 3 経路 fallback + pizza houjin-import/search CLI ([e4ba19f](https://github.com/clearclown/pizza/commit/e4ba19f5f1e8085126e4ea977193ebafd39ea6fc))
* **layer-d:** 国税庁 CSV + gBizINFO API でキーレス/即時の Layer D 代替 ([7e3f71d](https://github.com/clearclown/pizza/commit/7e3f71da92c7aa439f3fe446a448b810ddf5fd5e))
* M4 Box SQLite + area→polygon + Oven.Bake + CLI end-to-end ([566b922](https://github.com/clearclown/pizza/commit/566b922177d7c9db2e971be3ed11d7962e7d58c2))
* **megajii:** add all-brand fc exports ([834cc96](https://github.com/clearclown/pizza/commit/834cc96750894e9c88aa51ef507e9f9484fb1b2f))
* **megajii:** add brand fill-rate audit ([35adcb2](https://github.com/clearclown/pizza/commit/35adcb22033485984353d7689b0a254a5c511f76))
* **megajii:** expand non-google fc operator export ([609e4bc](https://github.com/clearclown/pizza/commit/609e4bc94bd806b51ae610f5b47f804443d29418))
* **megajii:** expand official operator coverage ([6d222a4](https://github.com/clearclown/pizza/commit/6d222a47500577db6bc4a05c5e983ca7bceee70d))
* **megajii:** export extended fc brand lists ([99f32dd](https://github.com/clearclown/pizza/commit/99f32dd9c69cdbad98e93c8e1a4bab61fde13bcd))
* Phase 2 core — browser-use LLM judgement, Streamlit UI, golden dataset ([ca96491](https://github.com/clearclown/pizza/commit/ca96491c10ec719664246b08248868cebb0be3a2))
* **phase17.1:** OSM Overpass API client (recall 補完用) ([5859067](https://github.com/clearclown/pizza/commit/58590677bd17e967ae377ffced48f619e8048e29))
* **phase17:** CoverMap bake 統合 + e-Stat recall + Registry 自動拡充 ([a67586e](https://github.com/clearclown/pizza/commit/a67586e718cc35babd5ef259509439f076c918be))
* **phase17:** pizza registry-expand CLI 露出 ([f05d4c4](https://github.com/clearclown/pizza/commit/f05d4c4f66621d006b6e1a5b550409809a033763))
* **phase18-19:** URL ドメイン二次 filter + 事業会社主語のクロスブランド集計 ([0730496](https://github.com/clearclown/pizza/commit/0730496a0487fdbf5b68050d940712e31de4fc3d))
* **phase19:** megafranchisee に operator 名 normalize / sort-by brands 追加 ([8ad587c](https://github.com/clearclown/pizza/commit/8ad587c5126fdc30ba0b5dc5e1d67ac49e32ee20))
* **phase20:** BC2024 多業態メガジー 23 社を registry に追加 ([a314101](https://github.com/clearclown/pizza/commit/a3141015ea738e01e43d761e971f28622afcb7b0))
* **phase20:** multi_brand_operators YAML section + seed loader ([8a97d21](https://github.com/clearclown/pizza/commit/8a97d21a5f41497268c55b25f33113be1c52e2d0))
* **phase21:** asyncio event loop 修正 + pure pipeline による golden CSV ([6ac30a7](https://github.com/clearclown/pizza/commit/6ac30a79513a6402662fd6e5bfabbf02b1d0e336))
* **phase22:** CLAUDE.md / evaluator (supervised loop) / 高速 search_by_name / hydrate merge ([2eb2eec](https://github.com/clearclown/pizza/commit/2eb2eecc5dae2c478a5477ecb5d074c349cc6cf2))
* **phase22:** ORM 集約 + JFA 自動取込 + 3 ソース統合 + LLM クレンジング ([11dd7f6](https://github.com/clearclown/pizza/commit/11dd7f6359a97bbd1568734354e7f21e486e592b))
* **phase23:** pizza enrich — Places Details + browser-use 一括 operator 特定 ([72cdd85](https://github.com/clearclown/pizza/commit/72cdd85f85f546d5754e28b8426ed6cd3a4c9232))
* **phase23:** Places Details + browser-use scraper で operator 特定を強化 ([1ac8899](https://github.com/clearclown/pizza/commit/1ac88999776f24a0b1a7b6a7607255da1c5c1c6c))
* **phase24:** Scrapling 導入 — browser-use 遅延問題の代替 ([fccf632](https://github.com/clearclown/pizza/commit/fccf632e527ac069d817a7d8c08e880fe6af0f31))
* **phase3:** plan, golden expansion to 30, and live accuracy benchmark ([dacca74](https://github.com/clearclown/pizza/commit/dacca748375243f699ec4d2cf1f4710c954932bb))
* **phase4/phase5:** judge_by_evidence + prompt v4 + research-pipeline pivot ([c536beb](https://github.com/clearclown/pizza/commit/c536beb2c405dbfd0d8c0142cbc09c7465d7307c))
* **phase4:** P4-1 business-entity definitions — franchisor vs franchisee split ([e2de778](https://github.com/clearclown/pizza/commit/e2de7788748376eb64c7d24c7b138ac0cc553e5c))
* **phase4:** Step 2 + 3B done + Evidence-based pivot ([205cd3a](https://github.com/clearclown/pizza/commit/205cd3adea5877d6574643225f0c024e96098532))
* **phase5-16:** Panel/Critic/Audit/OperatorSpider/Territory 一式 ([1d7f0fc](https://github.com/clearclown/pizza/commit/1d7f0fcb3341f5b75fd9ee497800c8af1cc30c81))
* **phase5:** PlacesClient (Python) + Phase 5 status 総括 ([277b7c3](https://github.com/clearclown/pizza/commit/277b7c3df74037d56f59b615068b3e8b61ee3e65))
* **phase5:** Step A — PerStoreExtractor (per-store deterministic extraction) ([8981155](https://github.com/clearclown/pizza/commit/898115556337d2b2ee0de3ff7dd1f0fa9b403402))
* **phase5:** Step B — OperatorLedger (SQLite operator_stores + store_evidence) ([9e2fe07](https://github.com/clearclown/pizza/commit/9e2fe07dd5fcf055f5efc8905b451046991339dd))
* **phase5:** Step C — ChainDiscovery (芋づる式 operator grouping) ([143cfd7](https://github.com/clearclown/pizza/commit/143cfd72a6ff1623a5667bdd67463d404beef7f1))
* **phase5:** Steps 4,D,E,F — normalize + CrossVerifier + ResearchPipeline + CLI ([fb36042](https://github.com/clearclown/pizza/commit/fb36042530a9ba93e4ee17defe43079f620acc46))
* **phase6-13:** 4 層検証 + Adaptive quad-tree + Territory knowledge ([3dac0f9](https://github.com/clearclown/pizza/commit/3dac0f9cdc7d01379b732fb88227b72d855616ff))
* **pizza:** wire --with-judge to connect delivery-service gRPC ([8064fd7](https://github.com/clearclown/pizza/commit/8064fd77531b326a4faaad571e7f6ca7beb845d2))
* **prompt:** judge.yaml v2 — 96.7% accuracy on Gemini 2.5 Flash ([ee38716](https://github.com/clearclown/pizza/commit/ee38716038c2d5f5f59ecc5a621f0b7a43838642))
* **toppings:** add Firecrawl REST client with docker/saas mode switch ([bfa0e54](https://github.com/clearclown/pizza/commit/bfa0e5473dd136540a45798d64f09efcddefa5ee))
* **validate-judge:** source_has_claim 別追加検証 (true/false/partial) ([b88840a](https://github.com/clearclown/pizza/commit/b88840a3ccf0a623ed0287db0822b00c387ac0fb))
* **validate-judge:** unknown_source_rejected ルール追加 (source_priority外はエラー) ([683d156](https://github.com/clearclown/pizza/commit/683d156447f1bd8cdad2594fef416a0391f6b592))
* **verifier:** Go実装の国税庁法人番号APIクライアント (Layer D) ([619d089](https://github.com/clearclown/pizza/commit/619d0893e0150d7c9c6b1d9d1b5b618f80a590b3))


### Bug Fixes

* **deploy:** correct Firecrawl image path and split into independent compose ([7e09ed9](https://github.com/clearclown/pizza/commit/7e09ed920bb334ea2076da7753bcf044b14c8e83))
* **enrich:** browser-use 0.12.6 + franchisor-only store を再 enrich 対象化 ([2741dc1](https://github.com/clearclown/pizza/commit/2741dc182f597aac35c55f78966cf2fce4e84d67))
* **golden+validate-judge:** corporate_name_from_source カラム追加、CSV列インデックス修正 ([8d7fa30](https://github.com/clearclown/pizza/commit/8d7fa30fd87e4ba6e4ddaebdaf935cf5dc5545be))
* **jfa:** 業種説明を brand に混入させない parser + 英語サフィックス除去 ([68b483c](https://github.com/clearclown/pizza/commit/68b483c7f43b0b9905dec3a91156da86c450544e))
* **megajii:** collapse target link duplicates ([729c963](https://github.com/clearclown/pizza/commit/729c963aeb9409a6dcb7ad2f66e1c90d0639abbd))
* **megajii:** fact-check mos burger links ([4d966fc](https://github.com/clearclown/pizza/commit/4d966fc361a866a706ca5dee03bb8e46b48d354c))
* **megajii:** normalize target brand industries ([32962b9](https://github.com/clearclown/pizza/commit/32962b94bd64c34829c3f3b76240c81560b5d648))
* **megajii:** scope ranking exports to 14 target brands ([5721c23](https://github.com/clearclown/pizza/commit/5721c23f327ce1f5e4af997428c3a178c7937ad8))
* **orm:** OperatorCompany に Phase 25 列の mapped_column 追加 + spider 復活 ([96b7047](https://github.com/clearclown/pizza/commit/96b70479d563277ef2bbe28a8fa9d13abec24472))
* **phase21:** Firecrawl VM OOM 解消 + waitFor SPA 対応 + Panel browser fallback ([c5d325b](https://github.com/clearclown/pizza/commit/c5d325b0a57058c260b06dd4b323fb92faef8ebc))
* **proto:** pin buf python plugin to v31 to match grpcio-tools protobuf&lt;7 constraint ([c4d2c8d](https://github.com/clearclown/pizza/commit/c4d2c8dd14b38cdf064f78109c07dad1b295af90))


### Reverts

* LLM ハルシネーションデータの除去 + ハードコード撤廃 ([5e4946d](https://github.com/clearclown/pizza/commit/5e4946d3db6b75fad3f97579ade3828b20dbc167))

## [Unreleased]

### Added
- Phase 0 Foundation scaffolding: documentation, proto contracts, polyglot monorepo layout.
- gRPC service definitions for Seed (M1), Kitchen (M2), Delivery (M3), BI (M4).
- Multi-LLM provider registry (Anthropic / OpenAI / Gemini) for Delivery module.
- TDD Red baseline tests for `internal/grid`, `internal/oven`, `internal/scoring`,
  and `services/delivery/tests/`.
- CI workflows: ci, buf, codeql, release-please, upstream-sync.
- OSS community files: LICENSE (MIT), CODE_OF_CONDUCT, CONTRIBUTING, SECURITY.

[Unreleased]: https://github.com/clearclown/pizza/compare/main...HEAD
