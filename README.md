# PI-ZZA 🍕 — Process Integration & Zonal Search Agent

<p align="center">
  <a href="https://github.com/clearclown/pizza/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/clearclown/pizza/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/clearclown/pizza/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg"></a>
  <img alt="Go" src="https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go&logoColor=white">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white">
  <a href="https://conventionalcommits.org"><img alt="Conventional Commits" src="https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg"></a>
  <img alt="TDD" src="https://img.shields.io/badge/TDD-Red%20%E2%86%92%20Green%20%E2%86%92%20Refactor-brightgreen">
  <a href="https://github.com/clearclown/pizza/pulls"><img alt="PRs Welcome" src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg"></a>
</p>

> **「精度の高いデータ、おまち！」**
> PI-ZZA は、Google Maps の網羅的検索と AI による自律ブラウジングを組み合わせた、次世代のロケーション・インテリジェンス・ツールです。

---

## 🍕 プロジェクト概要

フランチャイズ (FC) 業界における**メガフランチャイジー（20 店舗以上の運営会社）**の特定や、**直営・FC の判別**といった、人間が数週間かけて行う泥臭いリサーチ業務を、AI エージェントが数時間で完遂させることを目的としています。

## 🧩 アーキテクチャ — 4 つのトッピング

```
┌──────────────────────────────────────────────────────────────────┐
│                  🔥 Oven (Go Orchestrator)                        │
│                  cmd/pizza/ — pizza bake ...                      │
└─────────┬────────────┬───────────────┬─────────────┬─────────────┘
          │ gRPC       │ gRPC          │ REST        │ SQLite
          ▼            ▼               ▼             ▼
   ┌───────────┐ ┌─────────────┐ ┌────────────┐ ┌──────────┐
   │ 🫓 Dough  │ │ 🛵 Courier  │ │ 🧀 Kitchen │ │ 📦 Box   │
   │ Seed (Go) │ │ Delivery    │ │ Firecrawl  │ │ BI       │
   │           │ │ (Python)    │ │ (TS/AGPL)  │ │ (Py)     │
   │ M1        │ │ M3          │ │ M2         │ │ M4       │
   └───────────┘ └──────┬──────┘ └────────────┘ └──────────┘
                        │ Multi-LLM
            ┌───────────┼────────────┐
            ▼           ▼            ▼
       Anthropic    OpenAI        Gemini
```

| # | モジュール | 比喩 | 実装言語 | フォーク元 | ライセンス |
|---|---|---|---|---|---|
| **M1** | **Seed** | 🫓 生地 | **Go** | [gosom/google-maps-scraper](https://github.com/gosom/google-maps-scraper) + [googlemaps/google-maps-services-go](https://github.com/googlemaps/google-maps-services-go) | MIT / Apache-2.0 |
| **M2** | **Kitchen** | 🧀 トッピング | **TypeScript** | [mendableai/firecrawl](https://github.com/mendableai/firecrawl) | **AGPL-3.0** (REST 越境で隔離) |
| **M3** | **Delivery** | 🛵 配達 | **Python** | [browser-use/browser-use](https://github.com/browser-use/browser-use) | MIT |
| **M4** | **Box** | 📦 箱 | **Python (Streamlit + SQLite)** | — (自作) | — |

> **多言語共存 (polyglot)**: Go オーケストレータが gRPC で各モジュールを束ねます。フォーク元 OSS は**元言語のまま**保持し、API 境界で接続します。

---

## 🚀 Quick Bake

```bash
# 1. Clone
git clone git@github.com:clearclown/pizza.git
cd pizza

# 2. 環境構築
make bootstrap            # Go / Python (uv) / buf / ツール一式

# 3. 環境変数
cp .env.example .env      # GOOGLE_MAPS_API_KEY, LLM_PROVIDER 等を設定

# 4. gRPC コード生成
make proto

# 5. テスト (TDD: 現在は Red 状態から開始)
make test

# 6. ローカル統合起動
make up                   # firecrawl + dough + delivery を docker compose で

# 7. PI-ZZA を焼く
./bin/pizza bake --query "エニタイムフィットネス" --area "東京都"
```

---

## 🧪 開発フロー — TDD First

本プロジェクトでは **Red → Green → Refactor** を厳守します。

```bash
# 1. 🔴 Red: 失敗するテストだけコミット
git commit -m "test(scoring): add failing test for mega franchisee threshold"

# 2. 🟢 Green: 最小実装でテストを通す
git commit -m "feat(scoring): count stores with 20+ threshold"

# 3. 🔵 Refactor: 構造を整える
git commit -m "refactor(scoring): extract threshold to config"
```

詳細: [CONTRIBUTING.md](./CONTRIBUTING.md) / [docs/tdd-workflow.md](./docs/tdd-workflow.md)

---

## 📁 ディレクトリ構成（抜粋）

```
pizza/
├── api/pizza/v1/        # 🔌 gRPC proto 契約 (buf 管理)
├── cmd/                  # 🏠 バイナリエントリ (pizza, dough-service, delivery-service, box-ui)
├── internal/             # 🍕 Go パッケージ (oven / dough / toppings / courier / box / grid / scoring)
├── services/delivery/    # 🐍 Python browser-use wrapper + Multi-LLM providers
├── gen/                  # 📜 proto 生成物 (go / python / ts)
├── third_party/          # 🍴 upstream OSS のフォーク (git subtree)
├── deploy/               # 🚢 compose.yaml, Dockerfile.*
├── docs/                 # 📖 architecture / tdd / fork-strategy / proto-versioning
├── test/                 # 🧪 E2E (testcontainers-go) + fixtures
└── scripts/              # 🛠 bootstrap.sh / proto.sh / e2e.sh
```

全体像は [ARCHITECTURE.md](./ARCHITECTURE.md) と [docs/architecture.md](./docs/architecture.md) を参照。

---

## 🛠 テックスタック

| Layer | Tool |
|---|---|
| **Orchestrator** | Go 1.22+, gRPC, bufconn, testify, gomock |
| **API 契約** | Protocol Buffers, [buf](https://buf.build) |
| **AI エージェント** | [browser-use](https://github.com/browser-use/browser-use), Anthropic / OpenAI / Gemini SDK |
| **Crawler** | [Firecrawl](https://github.com/mendableai/firecrawl) (REST, セルフホストまたは SaaS) |
| **Maps** | [gosom/google-maps-scraper](https://github.com/gosom/google-maps-scraper), Google Maps Places API |
| **Python** | 3.11+, [uv](https://github.com/astral-sh/uv), pytest, ruff |
| **BI** | Streamlit + SQLite |
| **CI** | GitHub Actions (ci / buf / codeql / release-please / upstream-sync) |
| **Container** | Docker Compose (podman 互換) |

---

## 📚 ドキュメント

- [ARCHITECTURE.md](./ARCHITECTURE.md) — 俯瞰図
- [docs/architecture.md](./docs/architecture.md) — シーケンス図・SQLite スキーマ・gRPC 契約
- [docs/tdd-workflow.md](./docs/tdd-workflow.md) — Red-Green-Refactor 実例（Go/Python）
- [docs/fork-strategy.md](./docs/fork-strategy.md) — git subtree での upstream 同期
- [docs/license-compliance.md](./docs/license-compliance.md) — AGPL Firecrawl の REST 越境隔離
- [docs/proto-versioning.md](./docs/proto-versioning.md) — buf breaking ポリシー
- [開発工程.md](./開発工程.md) — フェーズ別ロードマップ（日本語原本）
- [CONTRIBUTING.md](./CONTRIBUTING.md) — 貢献ガイド
- [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) — 行動規範
- [SECURITY.md](./SECURITY.md) — 脆弱性報告
- [English README](./README.en.md)

---

## 🤝 コントリビュート

プルリクエスト歓迎します！ Red → Green → Refactor の TDD サイクルと [Conventional Commits](https://www.conventionalcommits.org/) に従ってください。

Issue は [こちら](https://github.com/clearclown/pizza/issues)、議論は [Discussions](https://github.com/clearclown/pizza/discussions)。

---

## ⚖️ ライセンス

本プロジェクトは [MIT License](./LICENSE) で公開しています — ユーモアと効率を愛するすべてのエンジニアへ。

フォーク元 OSS のライセンスは各リポジトリに従います。Firecrawl は AGPL-3.0 であり、PI-ZZA 本体とはプロセス境界（REST）で分離されています。詳細: [docs/license-compliance.md](./docs/license-compliance.md)。
