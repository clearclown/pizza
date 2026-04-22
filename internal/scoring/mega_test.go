// 🔴 Red phase test — Phase 1 で Green 化する。
//
// 開発工程.md §3.1 — 20 店舗以上でメガジー判定が true になること。
package scoring_test

import (
	"testing"

	"github.com/clearclown/pizza/internal/scoring"
	"github.com/stretchr/testify/assert"
)

func TestIsMegaFranchisee_tableDriven(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		stores    int
		threshold int
		want      bool
	}{
		{"zero stores", 0, 20, false},
		{"below threshold", 19, 20, false},
		{"at threshold", 20, 20, true},
		{"above threshold", 55, 20, true},
		{"custom threshold 10 — 12 stores", 12, 10, true},
		{"custom threshold 10 — 9 stores", 9, 10, false},
		{"zero threshold uses default 20 — 20 stores", 20, 0, true},
		{"negative threshold uses default 20 — 19 stores", 19, -5, false},
		{"negative threshold uses default 20 — 25 stores", 25, -5, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := scoring.IsMegaFranchisee(tc.stores, tc.threshold)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMegaFranchiseeDefaultThreshold_is20(t *testing.T) {
	t.Parallel()
	// 開発工程.md §3.1 Mega Franchisee = 20 stores or more.
	assert.Equal(t, 20, scoring.MegaFranchiseeDefaultThreshold)
}
