// retry.go — GCP API / LLM API のレート制限時の指数バックオフ制御。
// 開発工程.md §3.2 API Backoff Test 対応。
package oven

import (
	"context"
	"math"
	"math/rand/v2"
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
// 計算: base * factor^attempt + jitter、MaxDelay でキャップ。
func (b Backoff) DelayFor(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	factor := b.Factor
	if factor <= 0 {
		factor = 2.0
	}
	base := float64(b.BaseDelay)
	if base <= 0 {
		base = float64(100 * time.Millisecond)
	}
	d := base * math.Pow(factor, float64(attempt))
	// ジッタ: ±jitter * d
	if b.Jitter > 0 {
		d += (rand.Float64()*2 - 1) * b.Jitter * d
	}
	if d < 0 {
		d = 0
	}
	if b.MaxDelay > 0 && d > float64(b.MaxDelay) {
		d = float64(b.MaxDelay)
	}
	return time.Duration(d)
}

// Retry は fn を失敗時に指数バックオフでリトライする。
//   - isRetryable が true のエラーのみリトライする
//   - maxAttempts 回まで試行 (1 回目は即座に、以降は DelayFor(i-1) 待機)
//   - ctx がキャンセルされたら即座に ctx.Err() を返す
func Retry(
	ctx context.Context,
	b Backoff,
	maxAttempts int,
	fn func() error,
	isRetryable func(error) bool,
) error {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if isRetryable == nil {
		isRetryable = func(error) bool { return true }
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if attempt > 0 {
			d := b.DelayFor(attempt - 1)
			timer := time.NewTimer(d)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return err
		}
	}
	return lastErr
}
