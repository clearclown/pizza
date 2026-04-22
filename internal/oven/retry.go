// retry.go — GCP API / LLM API のレート制限時の指数バックオフ制御。
//
// 開発工程.md §3.2 API Backoff Test 対応。
// Phase 0: 未実装。Phase 1 で実装。
package oven

import (
	"context"
	"time"
)

// Backoff は retry 間隔を計算する。
type Backoff struct {
	BaseDelay time.Duration // 初回待機時間
	MaxDelay  time.Duration // 上限
	Factor    float64       // 指数係数 (通常 2.0)
	Jitter    float64       // ランダムジッタ [0.0, 1.0]
}

// DefaultBackoff は 500ms → 最大 30s、factor 2.0、jitter 0.2。
func DefaultBackoff() Backoff {
	return Backoff{
		BaseDelay: 500 * time.Millisecond,
		MaxDelay:  30 * time.Second,
		Factor:    2.0,
		Jitter:    0.2,
	}
}

// DelayFor は attempt 番目 (0-indexed) の待機時間を返す。
// Phase 0: 未実装。常に 0 を返す。
func (b Backoff) DelayFor(attempt int) time.Duration {
	_ = attempt
	return 0
}

// Retry は fn を失敗時に指数バックオフでリトライする。
// IsRetryable が true のエラーのみリトライし、ctx がキャンセルされたら即座に返す。
// Phase 0: 未実装。
func Retry(ctx context.Context, b Backoff, maxAttempts int, fn func() error, isRetryable func(error) bool) error {
	_ = ctx
	_ = b
	_ = maxAttempts
	_ = fn
	_ = isRetryable
	return ErrNotImplemented
}
