// 🔴 → 🟢 PlacesClient のテスト。httptest でモック応答を返し、
// 実 API に依存しない Green ループを回す。
package dough_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/dough"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fakePlacesJSON = `{
  "places": [
    {
      "id": "ChIJPLACE1",
      "displayName": {"text": "エニタイムフィットネス 新宿6丁目店", "languageCode": "ja"},
      "formattedAddress": "日本、東京都新宿区新宿6丁目",
      "location": {"latitude": 35.6935, "longitude": 139.7075},
      "nationalPhoneNumber": "03-1234-5678",
      "websiteUri": "https://www.anytimefitness.co.jp/shinjuku6/"
    },
    {
      "id": "ChIJPLACE2",
      "displayName": {"text": "エニタイムフィットネス 西新宿店", "languageCode": "ja"},
      "formattedAddress": "東京都新宿区西新宿",
      "location": {"latitude": 35.6880, "longitude": 139.6948}
    }
  ],
  "nextPageToken": ""
}`

func newFakePlacesServer(t *testing.T, expectedAPIKey string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/places:searchText", r.URL.Path)
		assert.Equal(t, expectedAPIKey, r.Header.Get("X-Goog-Api-Key"))
		assert.NotEmpty(t, r.Header.Get("X-Goog-FieldMask"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fakePlacesJSON))
	}))
}

func TestPlacesClient_SearchText_parsesResponse(t *testing.T) {
	t.Parallel()
	srv := newFakePlacesServer(t, "test-api-key")
	defer srv.Close()

	c := &dough.PlacesClient{APIKey: "test-api-key", BaseURL: srv.URL}
	resp, err := c.SearchText(context.Background(), &dough.SearchTextRequest{
		TextQuery:      "エニタイムフィットネス 新宿",
		LanguageCode:   "ja",
		MaxResultCount: 20,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Places, 2)
	assert.Equal(t, "ChIJPLACE1", resp.Places[0].ID)
	assert.Equal(t, "エニタイムフィットネス 新宿6丁目店", resp.Places[0].DisplayName.Text)
	assert.InDelta(t, 35.6935, resp.Places[0].Location.Latitude, 0.0001)
	assert.Equal(t, "", resp.NextPageToken)
}

func TestPlacesClient_SearchText_requiresAPIKey(t *testing.T) {
	t.Parallel()
	c := &dough.PlacesClient{APIKey: "", BaseURL: "http://unused"}
	_, err := c.SearchText(context.Background(), &dough.SearchTextRequest{TextQuery: "x"})
	assert.ErrorContains(t, err, "no API key")
}

func TestPlacesClient_SearchText_requiresTextQuery(t *testing.T) {
	t.Parallel()
	c := &dough.PlacesClient{APIKey: "k"}
	_, err := c.SearchText(context.Background(), &dough.SearchTextRequest{})
	assert.ErrorContains(t, err, "TextQuery")
}

func TestPlacesClient_SearchText_mapsNon2xxToErrAPI(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"forbidden"}`))
	}))
	defer srv.Close()

	c := &dough.PlacesClient{APIKey: "k", BaseURL: srv.URL}
	_, err := c.SearchText(context.Background(), &dough.SearchTextRequest{TextQuery: "x"})
	require.Error(t, err)
	apiErr, ok := err.(*dough.ErrAPI)
	require.True(t, ok, "error should be *dough.ErrAPI: %T %v", err, err)
	assert.Equal(t, 403, apiErr.Status)
	assert.True(t, strings.Contains(apiErr.Body, "forbidden"))
}

func TestPlaceRaw_ToStore_mapsAllFields(t *testing.T) {
	t.Parallel()
	p := dough.PlaceRaw{
		ID:                  "ChIJ_ABC",
		DisplayName:         dough.DisplayName{Text: "テスト店舗"},
		FormattedAddress:    "東京都",
		Location:            dough.Location{Latitude: 35.1, Longitude: 139.1},
		NationalPhoneNumber: "03-0000-0000",
		WebsiteURI:          "https://example.com",
	}
	store := p.ToStore("ブランドA", "cell-00007")
	assert.Equal(t, "ChIJ_ABC", store.GetPlaceId())
	assert.Equal(t, "ブランドA", store.GetBrand())
	assert.Equal(t, "テスト店舗", store.GetName())
	assert.Equal(t, "東京都", store.GetAddress())
	assert.Equal(t, "cell-00007", store.GetGridCellId())
	assert.Equal(t, "03-0000-0000", store.GetPhone())
	assert.Equal(t, "https://example.com", store.GetOfficialUrl())
	assert.InDelta(t, 35.1, store.GetLocation().GetLat(), 0.0001)
	assert.InDelta(t, 139.1, store.GetLocation().GetLng(), 0.0001)

	// pb.Store の型チェック
	var _ *pb.Store = store
}

func TestPlaceRaw_ToStore_fallsBackToInternationalPhone(t *testing.T) {
	t.Parallel()
	p := dough.PlaceRaw{
		ID:                       "x",
		InternationalPhoneNumber: "+81 3-0000-0000",
	}
	store := p.ToStore("", "")
	assert.Equal(t, "+81 3-0000-0000", store.GetPhone())
}
