package verifier

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── IsActive ──────────────────────────────────────────────────────────────

func TestCorporation_IsActive(t *testing.T) {
	tests := []struct {
		name string
		corp Corporation
		want bool
	}{
		{"新規登録 → 現役", Corporation{Process: "01", CloseDate: ""}, true},
		{"商号変更 → 現役", Corporation{Process: "11", CloseDate: ""}, true},
		{"廃業日あり → 廃業済み", Corporation{Process: "01", CloseDate: "2024-03-31"}, false},
		{"不明プロセス → 非アクティブ", Corporation{Process: "99", CloseDate: ""}, false},
		{"吸収合併消滅71 → 廃業済み", Corporation{Process: "71", CloseDate: ""}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.corp.IsActive())
		})
	}
}

// ─── New / NewWithAppID ────────────────────────────────────────────────────

func TestNew_NoAppID(t *testing.T) {
	t.Setenv("HOUJIN_BANGOU_APP_ID", "")
	_, err := New()
	assert.ErrorIs(t, err, ErrNoAppID)
}

func TestNew_WithAppID(t *testing.T) {
	t.Setenv("HOUJIN_BANGOU_APP_ID", "test-app-id")
	c, err := New()
	require.NoError(t, err)
	assert.Equal(t, "test-app-id", c.appID)
}

func TestNewWithAppID(t *testing.T) {
	c := NewWithAppID("test-token")
	assert.NotNil(t, c)
	assert.Equal(t, "test-token", c.appID)
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
		wantRange [2]float64 // [min, max]
	}{
		{
			"株式会社Fast Fitness Japan",
			"株式会社ファストフィットネスジャパン",
			[2]float64{0.0, 0.3}, // カナ vs アルファベット → 低スコア
		},
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

// ─── parseXMLResponse ──────────────────────────────────────────────────────

func TestParseXMLResponse(t *testing.T) {
	xmlBody := `<?xml version="1.0" encoding="UTF-8"?>
<corporateInfoList>
  <lastUpdateDate>2026-04-24</lastUpdateDate>
  <count>1</count>
  <corporateInfo>
    <corporateNumber>1234567890123</corporateNumber>
    <name>株式会社テスト運営</name>
    <prefectureName>東京都</prefectureName>
    <cityName>渋谷区</cityName>
    <streetNumber>道玄坂1-1-1</streetNumber>
    <process>01</process>
    <updateDate>2026-04-01</updateDate>
    <closeDate></closeDate>
    <closeCause></closeCause>
  </corporateInfo>
</corporateInfoList>`

	corps, err := parseXMLResponse(strings.NewReader(xmlBody))
	require.NoError(t, err)
	require.Len(t, corps, 1)

	c := corps[0]
	assert.Equal(t, "1234567890123", c.CorporateNumber)
	assert.Equal(t, "株式会社テスト運営", c.Name)
	assert.Equal(t, "東京都渋谷区道玄坂1-1-1", c.Address)
	assert.Equal(t, "01", c.Process)
	assert.True(t, c.IsActive())
}

func TestParseXMLResponse_Empty(t *testing.T) {
	xmlBody := `<?xml version="1.0" encoding="UTF-8"?>
<corporateInfoList>
  <lastUpdateDate>2026-04-24</lastUpdateDate>
  <count>0</count>
</corporateInfoList>`

	corps, err := parseXMLResponse(strings.NewReader(xmlBody))
	require.NoError(t, err)
	assert.Empty(t, corps)
}

func TestParseXMLResponse_InvalidXML(t *testing.T) {
	_, err := parseXMLResponse(strings.NewReader("<broken xml"))
	assert.Error(t, err)
}

// ─── bigramJaccard ─────────────────────────────────────────────────────────

func TestBigramJaccard(t *testing.T) {
	// 同一文字列 → 1.0
	assert.InDelta(t, 1.0, bigramJaccard("テスト運営", "テスト運営"), 0.001)
	// 完全無関係 → 0.0
	assert.InDelta(t, 0.0, bigramJaccard("あいう", "えおか"), 0.001)
	// 1文字以下 → 0.0
	assert.InDelta(t, 0.0, bigramJaccard("あ", "あいう"), 0.001)
}
