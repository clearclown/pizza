# pizza-delivery — M3 Courier (Python gRPC server)

browser-use を wrap する Python gRPC サーバ。マルチ LLM プロバイダ (Anthropic / OpenAI / Gemini) に対応。

## 起動

```bash
cd services/delivery
uv sync
LLM_PROVIDER=anthropic ANTHROPIC_API_KEY=... uv run python -m pizza_delivery
```

## テスト (Phase 0: Red)

```bash
uv run pytest
```

## アーキテクチャ

```
gRPC request (StoreContext)
    ↓
pizza_delivery/server.py
    ↓
pizza_delivery/agent.py (browser-use)
    ↓
providers/registry.py → get_provider(env.LLM_PROVIDER)
    ↓
providers/{anthropic, openai, gemini}_provider.py
```

詳細は [../../docs/architecture.md](../../docs/architecture.md)。
