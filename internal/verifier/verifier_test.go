package verifier_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/clearclown/pizza/internal/verifier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildXMLResponse はテスト用のXMLレスポンスを生成する。
func buildXMLResponse(count int, corps []map[string]string) []byte {
	type corp struct {
		CorporateNumber string `xml:"corporateNumber"`
		Name            string `xml:"name"`
		PrefectureName  string `xml:"prefectureName"`
		CityName        string `xml:"cityName"`
		StreetNumber    string `xml:"streetNumber"`
		Process         string `xml:"process"`
		UpdateDate      string `xml:"updateDate"`
		CloseDate       string `xml:"closeDate"`
		CloseCause      string `xml:"closeCause"`
	}
	type root struct {
		XMLName        xml.Name `xml:"corporateInfoList"`
		LastUpdateDate string   `xml:"lastUpdateDate"`
		Count          int      `xml:"count"`
		Corps          []corp   `xml:"corporateInfo"`
	}
	r := root{LastUpdateDate: "2026-04-24", Count: count}
	for _, c := range corps {
		r.Corps = append(r.Corps, corp{
			CorporateNumber: c["number"],
			Name:            c["name"],
			PrefectureName:  c["pref"],
			CityName:        c["city"],
			StreetNumber:    c["street"],
			Process:         c["process"],
			UpdateDate:      "2026-04-01",
			CloseDate:       c["closeDate"],
		})
	}
	b, _ := xml.Marshal(r)
	return append([]byte(xml.Header), b...)
}

func newMockServer(t *testing.T, body []byte, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
		w.WriteHeader(status)
		w.Write(body)
	}))
}

// ─── SearchByName ──────────────────────────────────────────────────────────

func TestSearchByName_Found(t *testing.T) {
	body := buildXMLResponse(1, []map[string]string{
		{
			"number":  "1234567890123",
			"name":    "株式会社テスト運営",
			"pref":    "東京都",
			"city":    "渋谷区",
			"street":  "道玄坂1-1-1",
			"process": "01",
		},
	})
	srv := newMockServer(t, body, http.StatusOK)
	defer srv.Close()

	c := verifier.NewWithAppID("dummy")
	c.SetBaseURL(srv.URL) // テスト用フック

	corps, err := c.SearchByName(context.Background(), "テスト運営")
	require.NoError(t, err)
	require.Len(t, corps, 1)
	assert.Equal(t, "1234567890123", corps[0].CorporateNumber)
	assert.Equal(t, "株式会社テスト運営", corps[0].Name)
	assert.True(t, corps[0].IsActive())
}

func TestSearchByName_APIError(t *testing.T) {
	srv := newMockServer(t, []byte("Internal Server Error"), http.StatusInternalServerError)
	defer srv.Close()

	c := verifier.NewWithAppID("dummy")
	c.SetBaseURL(srv.URL)

	_, err := c.SearchByName(context.Background(), "テスト")
	assert.Error(t, err)
}

// ─── Verify ────────────────────────────────────────────────────────────────

func TestVerify_Found(t *testing.T) {
	body := buildXMLResponse(1, []map[string]string{
		{
			"number":  "1234567890123",
			"name":    "株式会社テスト運営",
			"pref":    "東京都",
			"city":    "渋谷区",
			"street":  "道玄坂1-1-1",
			"process": "01",
		},
	})
	srv := newMockServer(t, body, http.StatusOK)
	defer srv.Close()

	c := verifier.NewWithAppID("dummy")
	c.SetBaseURL(srv.URL)

	result := c.Verify(context.Background(), "テスト運営")
	assert.True(t, result.IsVerified)
	assert.Equal(t, "株式会社テスト運営", result.OfficialName)
	assert.Equal(t, "1234567890123", result.CorporateNumber)
	assert.True(t, result.IsActive)
	assert.Equal(t, "テスト運営", result.InputName)
}

func TestVerify_NotFound(t *testing.T) {
	body := buildXMLResponse(0, nil)
	srv := newMockServer(t, body, http.StatusOK)
	defer srv.Close()

	c := verifier.NewWithAppID("dummy")
	c.SetBaseURL(srv.URL)

	result := c.Verify(context.Background(), "存在しない会社XYZ")
	assert.False(t, result.IsVerified)
	assert.Equal(t, "存在しない会社XYZ", result.InputName)
}

func TestVerify_APIError_ReturnsFalse(t *testing.T) {
	// APIエラー時はパニックせず IsVerified=false を返す
	srv := newMockServer(t, []byte("error"), http.StatusInternalServerError)
	defer srv.Close()

	c := verifier.NewWithAppID("dummy")
	c.SetBaseURL(srv.URL)

	result := c.Verify(context.Background(), "テスト")
	assert.False(t, result.IsVerified)
}

// ─── Live test (skip unless -tags live) ────────────────────────────────────

func TestVerify_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode: skip live test")
	}
	t.Skip("live test: set -tags live and HOUJIN_BANGOU_APP_ID to run")
}
