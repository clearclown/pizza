# PI-ZZA Architecture 概要

本ファイルは俯瞰のみ。**詳細は [docs/architecture.md](./docs/architecture.md) を参照してください。**

## 高レベル図

```
┌────────────────────────────────────────────────────────────────────┐
│                      User (CLI / Box-UI)                           │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│  🔥 Oven (Go Orchestrator)  cmd/pizza/main.go                      │
│     internal/oven/ — パイプライン全体                               │
└────┬────────────┬────────────────┬──────────────┬─────────────────┘
     │ gRPC       │ gRPC           │ REST         │ SQLite driver
     ▼            ▼                ▼              ▼
┌─────────┐  ┌────────────┐   ┌────────────┐  ┌─────────────────┐
│ 🫓 Dough│  │ 🛵 Courier │   │ 🧀 Kitchen │  │ 📦 Box storage  │
│ :50051  │  │ :50053     │   │ :3002      │  │ ./var/*.sqlite  │
│ (Go)    │  │ (Python)   │   │ (TS/AGPL)  │  │                 │
│  M1     │  │  M3        │   │  M2        │  │  M4             │
└─────────┘  └─────┬──────┘   └────────────┘  └─────────────────┘
                   │
            Multi-LLM Provider Registry
         ┌─────────┼──────────┐
         ▼         ▼          ▼
    Anthropic  OpenAI     Gemini
```

## データフロー（3 分で読む）

1. **入力**: `pizza bake --query "ブランド名" --area "エリア"`
2. **M1 Seed (Dough)**: エリアを緯度経度メッシュに分割 (`internal/grid/`)、Google Maps から店舗を網羅抽出 → `pizza.v1.SeedService/SearchStoresInGrid` 経由で Orchestrator にストリーム
3. **M2 Kitchen (Toppings)**: 各店舗の公式 URL を Firecrawl REST で Markdown 化（並列）
4. **M3 Delivery (Courier)**: Markdown + コンテキストを Python サーバへ gRPC 送信 → browser-use が LLM と協調して「直営/FC」「運営会社」を判定
5. **M4 Box**: SQLite に保存 → Streamlit で地図可視化・スコアリング・CSV エクスポート

## 言語境界とライセンス

| 境界 | プロトコル | ライセンス分離理由 |
|---|---|---|
| Oven ↔ Dough | gRPC (pizza.v1.SeedService) | 同一 Go プロセスでも可。分離はスケール容易性のため |
| Oven ↔ Kitchen (Firecrawl) | **REST** | **AGPL-3.0 伝播回避のため REST 越境必須** |
| Oven ↔ Courier | gRPC (pizza.v1.DeliveryService) | Python/Go ランタイム境界 |
| Courier ↔ LLM | HTTPS (各プロバイダ SDK) | Anthropic / OpenAI / Gemini を registry で切替 |

詳細根拠は [docs/license-compliance.md](./docs/license-compliance.md)。

## gRPC サービス一覧

- `pizza.v1.SeedService` — `api/pizza/v1/seed.proto`
- `pizza.v1.KitchenService` — `api/pizza/v1/kitchen.proto`（将来 Firecrawl を gRPC 化する場合）
- `pizza.v1.DeliveryService` — `api/pizza/v1/delivery.proto`
- `pizza.v1.BIService` — `api/pizza/v1/bi.proto`

契約詳細と backward-compat ポリシーは [docs/proto-versioning.md](./docs/proto-versioning.md)。

## 次に読むべきドキュメント

- データフロー詳細 / SQLite スキーマ: [docs/architecture.md](./docs/architecture.md)
- TDD 実践: [docs/tdd-workflow.md](./docs/tdd-workflow.md)
- フォーク戦略: [docs/fork-strategy.md](./docs/fork-strategy.md)
