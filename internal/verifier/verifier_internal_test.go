package verifier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ─── IsActive ──────────────────────────────────────────────────────────────

func TestCorporation_IsActive(t *testing.T) {
	tests := []struct {
		name string
		corp Corporation
		want bool
	}{
		{"新規登録(01) → 現役", Corporation{Process: "01"}, true},
		{"商号変更(11) → 現役", Corporation{Process: "11"}, true},
		{"本店移転(12) → 現役", Corporation{Process: "12"}, true},
		{"代表者変更(13) → 現役", Corporation{Process: "13"}, true},
		{"組織変更(21) → 現役", Corporation{Process: "21"}, true},
		{"組織変更(22) → 現役", Corporation{Process: "22"}, true},
		{"国外(31) → 現役", Corporation{Process: "31"}, true},
		{"吸収合併消滅(71) → 廃業", Corporation{Process: "71"}, false},
		{"解散(72) → 廃業", Corporation{Process: "72"}, false},
		{"不明(99) → 廃業", Corporation{Process: "99"}, false},
		{"空文字 → 廃業", Corporation{Process: ""}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.corp.IsActive())
		})
	}
}

// ─── NewWithDB ─────────────────────────────────────────────────────────────

func TestNewWithDB_NotFound(t *testing.T) {
	_, err := NewWithDB("/nonexistent/path/registry.sqlite")
	assert.ErrorIs(t, err, ErrDBNotFound)
}

func TestNew_NoDefaultDB(t *testing.T) {
	// デフォルトパスが存在しない環境 (CI等) では ErrDBNotFound
	t.Setenv("PIZZA_ROOT", "/nonexistent")
	_, err := New()
	assert.ErrorIs(t, err, ErrDBNotFound)
}

// ─── canonicalKey ──────────────────────────────────────────────────────────

func TestCanonicalKey(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"株式会社テスト", "テスト"},
		{"㈱テスト", "テスト"},
		{"（株）テスト", "テスト"},
		{"テスト株式会社", "テスト"},
		{"合同会社テスト運営", "テスト運営"},
		{"テスト", "テスト"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, canonicalKey(tt.input))
		})
	}
}

// ─── nameSimilarity ────────────────────────────────────────────────────────

func TestNameSimilarity(t *testing.T) {
	tests := []struct {
		a, b      string
		wantRange [2]float64
	}{
		{
			"株式会社テスト運営",
			"株式会社テスト運営",
			[2]float64{1.0, 1.0}, // 完全一致
		},
		{
			"テスト運営",
			"株式会社テスト運営",
			[2]float64{0.85, 1.0}, // 包含関係 → 0.9
		},
		{
			"",
			"株式会社テスト",
			[2]float64{0.0, 0.0}, // 空文字 → 0.0
		},
	}
	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			score := nameSimilarity(tt.a, tt.b)
			assert.GreaterOrEqual(t, score, tt.wantRange[0])
			assert.LessOrEqual(t, score, tt.wantRange[1])
		})
	}
}

// ─── bigramJaccard ─────────────────────────────────────────────────────────

func TestBigramJaccard(t *testing.T) {
	assert.InDelta(t, 1.0, bigramJaccard("テスト運営", "テスト運営"), 0.001)
	assert.InDelta(t, 0.0, bigramJaccard("あいう", "えおか"), 0.001)
	assert.InDelta(t, 0.0, bigramJaccard("あ", "あいう"), 0.001)
}
