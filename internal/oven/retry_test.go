// 🔴 Red phase test — 開発工程.md §3.2 API Backoff Test 相当。
package oven_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/clearclown/pizza/internal/oven"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackoff_DelayFor_grows(t *testing.T) {
	t.Parallel()
	b := oven.DefaultBackoff()
	d0 := b.DelayFor(0)
	d1 := b.DelayFor(1)
	d2 := b.DelayFor(2)
	d10 := b.DelayFor(10)

	// 増加していく (単調増加までは要求しない — jitter があるので幅で確認)
	assert.Less(t, d0, d1)
	assert.Less(t, d1, d2)
	// 上限でキャップされる
	assert.LessOrEqual(t, d10, b.MaxDelay)
}

func TestRetry_succeedsAfterTransientFailures(t *testing.T) {
	t.Parallel()

	var tries int
	transient := errors.New("rate limit exceeded")
	fn := func() error {
		tries++
		if tries < 3 {
			return transient
		}
		return nil
	}

	err := oven.Retry(
		context.Background(),
		oven.Backoff{BaseDelay: time.Microsecond, MaxDelay: time.Microsecond, Factor: 2.0},
		5,
		fn,
		func(e error) bool { return errors.Is(e, transient) },
	)
	require.NoError(t, err)
	assert.Equal(t, 3, tries, "should stop retrying once fn succeeds")
}

func TestRetry_giveUpAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	hard := errors.New("permanent")
	err := oven.Retry(
		context.Background(),
		oven.Backoff{BaseDelay: time.Microsecond, MaxDelay: time.Microsecond},
		3,
		func() error { return hard },
		func(e error) bool { return errors.Is(e, hard) },
	)
	assert.ErrorIs(t, err, hard)
}

func TestRetry_respectsContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := oven.Retry(
		ctx,
		oven.DefaultBackoff(),
		5,
		func() error { return errors.New("transient") },
		func(error) bool { return true },
	)
	assert.ErrorIs(t, err, context.Canceled)
}
