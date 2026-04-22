package toppings_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/clearclown/pizza/internal/toppings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromMode_docker(t *testing.T) {
	t.Parallel()
	c, err := toppings.NewFromMode("docker", "", "")
	require.NoError(t, err)
	assert.Equal(t, toppings.DefaultDockerURL, c.BaseURL)
}

func TestNewFromMode_saasRequiresKey(t *testing.T) {
	t.Parallel()
	_, err := toppings.NewFromMode("saas", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FIRECRAWL_API_KEY")
}

func TestNewFromMode_saasWithKey(t *testing.T) {
	t.Parallel()
	c, err := toppings.NewFromMode("saas", "", "fc-secret")
	require.NoError(t, err)
	assert.Equal(t, toppings.DefaultSaaSURL, c.BaseURL)
	assert.Equal(t, "fc-secret", c.APIKey)
}

func TestNewFromMode_rejectsUnknown(t *testing.T) {
	t.Parallel()
	_, err := toppings.NewFromMode("rainbow", "", "")
	assert.ErrorContains(t, err, "docker|saas")
}

func TestClient_Scrape_successReturnsMarkdownDoc(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/scrape", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req toppings.ScrapeRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		assert.Equal(t, "https://example.com", req.URL)
		assert.Contains(t, req.Formats, "markdown")

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"success": true,
			"data": {
				"markdown": "# Example\n\nhello",
				"metadata": {"title": "Example", "lang": "en"}
			}
		}`))
	}))
	defer srv.Close()

	c := &toppings.Client{BaseURL: srv.URL}
	doc, err := c.Scrape(context.Background(), "https://example.com")
	require.NoError(t, err)
	require.NotNil(t, doc)
	assert.Equal(t, "https://example.com", doc.GetUrl())
	assert.Contains(t, doc.GetMarkdown(), "# Example")
	assert.Equal(t, "Example", doc.GetTitle())
	assert.Equal(t, "en", doc.GetMetadata()["lang"])
	assert.Greater(t, doc.GetFetchedAtUnix(), int64(0))
}

func TestClient_Scrape_saasSendsBearer(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer my-key", r.Header.Get("Authorization"))
		w.Write([]byte(`{"success":true,"data":{"markdown":"ok"}}`))
	}))
	defer srv.Close()

	c := &toppings.Client{BaseURL: srv.URL, APIKey: "my-key"}
	_, err := c.Scrape(context.Background(), "https://example.com")
	require.NoError(t, err)
}

func TestClient_Scrape_dockerOmitsBearer(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		w.Write([]byte(`{"success":true,"data":{"markdown":"ok"}}`))
	}))
	defer srv.Close()

	c := &toppings.Client{BaseURL: srv.URL}
	_, err := c.Scrape(context.Background(), "https://example.com")
	require.NoError(t, err)
}

func TestClient_Scrape_non2xxIsErrClient(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limit"}`))
	}))
	defer srv.Close()

	c := &toppings.Client{BaseURL: srv.URL}
	_, err := c.Scrape(context.Background(), "https://x")
	require.Error(t, err)
	var ec *toppings.ErrClient
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, 429, ec.Status)
}

func TestClient_Scrape_successFalseReturnsError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"success":false,"error":"blocked"}`))
	}))
	defer srv.Close()

	c := &toppings.Client{BaseURL: srv.URL}
	_, err := c.Scrape(context.Background(), "https://x")
	assert.ErrorContains(t, err, "blocked")
}
