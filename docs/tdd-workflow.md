# TDD Workflow — Red → Green → Refactor

PI-ZZA は **厳格 TDD** で開発されます。本ドキュメントは言語別の実践例を示します。

## 🔴 Red フェーズ — 失敗するテストを先に書く

> 「動かないテスト」ではなく、「機能が無いから失敗するテスト」を書く。

### Go の例

```go
// internal/scoring/mega_test.go
package scoring_test

import (
    "testing"

    "github.com/clearclown/pizza/internal/scoring"
    "github.com/stretchr/testify/assert"
)

func TestIsMegaFranchisee_atAndAboveThreshold(t *testing.T) {
    cases := []struct {
        name      string
        stores    int
        threshold int
        want      bool
    }{
        {"below threshold", 19, 20, false},
        {"at threshold", 20, 20, true},
        {"above threshold", 55, 20, true},
        {"custom threshold 10", 12, 10, true},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            got := scoring.IsMegaFranchisee(tc.stores, tc.threshold)
            assert.Equal(t, tc.want, got)
        })
    }
}
```

この時点で `scoring.IsMegaFranchisee` は **未定義**。`go test ./...` が赤くなる。

### Python の例

```python
# services/delivery/tests/test_providers/test_registry.py
import pytest
from pizza_delivery.providers.registry import get_provider
from pizza_delivery.providers.base import LLMProvider

def test_registry_returns_anthropic_by_default():
    provider = get_provider("anthropic")
    assert isinstance(provider, LLMProvider)
    assert provider.name == "anthropic"

def test_registry_raises_on_unknown():
    with pytest.raises(ValueError, match="unknown"):
        get_provider("nonexistent")
```

`pytest` が赤くなる。

## 🟢 Green フェーズ — 最小実装

動けばよい。美しさは次のフェーズ。

```go
// internal/scoring/scoring.go
package scoring

func IsMegaFranchisee(storeCount, threshold int) bool {
    return storeCount >= threshold
}
```

```python
# pizza_delivery/providers/registry.py
from .anthropic_provider import AnthropicProvider
from .openai_provider import OpenAIProvider
from .gemini_provider import GeminiProvider

_PROVIDERS = {
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
    "gemini": GeminiProvider,
}

def get_provider(name: str):
    if name not in _PROVIDERS:
        raise ValueError(f"unknown provider: {name}")
    return _PROVIDERS[name]()
```

テストが緑になることを `make test` で確認。

## 🔵 Refactor フェーズ — 構造を整える

動作を変えず、読みやすさ・テスト容易性・性能を改善する。

```go
// 閾値を定数化
package scoring

// MegaFranchiseeDefaultThreshold is the default store count for "mega" status.
const MegaFranchiseeDefaultThreshold = 20

func IsMegaFranchisee(storeCount, threshold int) bool {
    if threshold <= 0 {
        threshold = MegaFranchiseeDefaultThreshold
    }
    return storeCount >= threshold
}
```

再度 `make test` で緑を確認。

---

## 🧪 in-memory gRPC テスト (bufconn)

Orchestrator のパイプラインは実ネットワーク無しで統合テストできます。

```go
// internal/oven/pipeline_test.go
package oven_test

import (
    "context"
    "net"
    "testing"

    "github.com/stretchr/testify/require"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/test/bufconn"

    pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

const bufSize = 1024 * 1024

func newBufServer(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
    t.Helper()
    lis := bufconn.Listen(bufSize)
    srv := grpc.NewServer()
    register(srv)
    go func() { _ = srv.Serve(lis) }()
    t.Cleanup(srv.Stop)

    conn, err := grpc.NewClient("passthrough:///bufnet",
        grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
            return lis.Dial()
        }),
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    require.NoError(t, err)
    t.Cleanup(func() { _ = conn.Close() })
    return conn
}
```

## 🐳 E2E テスト (testcontainers-go + docker compose)

```go
// test/e2e/pipeline_test.go
//go:build integration

package e2e_test

import (
    "context"
    "testing"

    "github.com/testcontainers/testcontainers-go/modules/compose"
)

func TestEndToEndPipeline(t *testing.T) {
    ctx := context.Background()
    c, err := compose.NewDockerComposeWith(compose.WithStackFiles("../../deploy/compose.yaml"))
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = c.Down(ctx) })
    if err := c.Up(ctx, compose.Wait(true)); err != nil {
        t.Fatal(err)
    }
    // ... grpcurl 叩いて /v1/SeedService が reachable を確認
}
```

実行: `go test -tags=integration ./test/e2e/...`

---

## コミット粒度の目安

- 1 つの Red コミット = 1 つの failing test
- 複数テストを一度に赤くしない（差分を見やすく）
- Green コミットで実装し、Refactor コミットで整形する
- **Red と Green を同じコミットにしない** — 履歴が TDD の証明にならなくなる
