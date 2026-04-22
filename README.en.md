# PI-ZZA 🍕 — Process Integration & Zonal Search Agent

> **"Serving you high-precision data, piping hot!"**
> PI-ZZA is a next-generation location intelligence tool that combines exhaustive Google Maps search with autonomous AI browsing.

[日本語 README](./README.md)

---

## What It Does

Identifies **mega franchisees** (operators running 20+ stores) and classifies **directly-operated vs. franchised** outlets — the kind of multi-week investigative work that PI-ZZA reduces to hours.

## Architecture — 4 Toppings

```
┌──────────────────────────────────────────────────────────────────┐
│                  🔥 Oven (Go Orchestrator)                        │
└─────────┬────────────┬───────────────┬─────────────┬─────────────┘
          │ gRPC       │ gRPC          │ REST        │ SQLite
          ▼            ▼               ▼             ▼
   ┌───────────┐ ┌─────────────┐ ┌────────────┐ ┌──────────┐
   │ 🫓 Dough  │ │ 🛵 Courier  │ │ 🧀 Kitchen │ │ 📦 Box   │
   │ Seed (Go) │ │ Delivery(Py)│ │ Firecrawl  │ │ BI (Py)  │
   └───────────┘ └──────┬──────┘ └────────────┘ └──────────┘
                        │
            Multi-LLM (Anthropic / OpenAI / Gemini)
```

| # | Module | Lang | Upstream fork | License |
|---|---|---|---|---|
| M1 | Seed (dough) | Go | gosom/google-maps-scraper + googlemaps/google-maps-services-go | MIT / Apache-2.0 |
| M2 | Kitchen (toppings) | TypeScript | mendableai/firecrawl | AGPL-3.0 (isolated via REST) |
| M3 | Delivery (courier) | Python | browser-use/browser-use | MIT |
| M4 | Box (BI) | Python (Streamlit + SQLite) | — | — |

## Quick Bake

```bash
git clone git@github.com:clearclown/pizza.git
cd pizza
make bootstrap
cp .env.example .env         # edit keys
make proto
make test
make up
./bin/pizza bake --query "Anytime Fitness" --area "Tokyo"
```

## Dev Workflow — TDD First

We enforce **Red → Green → Refactor** commits. See [CONTRIBUTING.md](./CONTRIBUTING.md) and [docs/tdd-workflow.md](./docs/tdd-workflow.md).

## Docs

- [ARCHITECTURE.md](./ARCHITECTURE.md)
- [docs/architecture.md](./docs/architecture.md) — sequence diagrams, gRPC contracts
- [docs/tdd-workflow.md](./docs/tdd-workflow.md)
- [docs/fork-strategy.md](./docs/fork-strategy.md) — git subtree upstream sync
- [docs/license-compliance.md](./docs/license-compliance.md) — AGPL isolation rationale
- [docs/proto-versioning.md](./docs/proto-versioning.md)

## License

[MIT](./LICENSE) for PI-ZZA itself. Upstream forks retain their own licenses. Firecrawl (AGPL-3.0) is reached via REST only, never linked into the Go binary.
