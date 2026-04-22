package toppings

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// DefaultSaaSURL は Firecrawl Cloud の公式エンドポイント。
const DefaultSaaSURL = "https://api.firecrawl.dev"

// DefaultDockerURL はセルフホスト Docker 起動時の標準エンドポイント。
const DefaultDockerURL = "http://localhost:3002"

// ErrClient は Firecrawl API が非 2xx を返した際のエラー。
type ErrClient struct {
	Status int
	Body   string
}

func (e *ErrClient) Error() string {
	return fmt.Sprintf("firecrawl: status=%d body=%s", e.Status, e.Body)
}

// Client は Firecrawl REST API ラッパ (docker / saas 両対応)。
type Client struct {
	// BaseURL: docker=http://localhost:3002, saas=https://api.firecrawl.dev
	BaseURL string
	// APIKey: saas モード時のみ必要 (Bearer authorization)
	APIKey string
	// TimeoutMs: 1 スクレイプ当たりのサーバ側タイムアウト (default 30000)
	TimeoutMs int32
	// HTTPClient: 差し替え可能 (httptest 用)
	HTTPClient *http.Client
}

// NewFromMode は FIRECRAWL_MODE の値から Client を組み立てる。
//   - "docker": baseURL default http://localhost:3002、キー不要
//   - "saas":   baseURL default https://api.firecrawl.dev、apiKey 必須
//
// 両モード共通で apiURL を override 可能。
func NewFromMode(mode, apiURL, apiKey string) (*Client, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	c := &Client{APIKey: apiKey}
	switch mode {
	case "docker":
		if apiURL == "" {
			apiURL = DefaultDockerURL
		}
	case "saas":
		if apiURL == "" {
			apiURL = DefaultSaaSURL
		}
		if apiKey == "" {
			return nil, errors.New("toppings: FIRECRAWL_MODE=saas requires FIRECRAWL_API_KEY")
		}
	default:
		return nil, fmt.Errorf("toppings: unknown FIRECRAWL_MODE %q (want docker|saas)", mode)
	}
	c.BaseURL = strings.TrimRight(apiURL, "/")
	return c, nil
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 60 * time.Second}
}

// ScrapeRequest は Firecrawl /v1/scrape への入力。
type ScrapeRequest struct {
	URL             string   `json:"url"`
	Formats         []string `json:"formats,omitempty"`          // ["markdown"] など
	OnlyMainContent bool     `json:"onlyMainContent,omitempty"`
	TimeoutMs       int32    `json:"timeout,omitempty"`           // ms
}

// ScrapeResponse は Firecrawl からの応答 (主要フィールドのみ)。
type ScrapeResponse struct {
	Success bool        `json:"success"`
	Data    *ScrapeData `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// ScrapeData は 1 URL 分のコンテンツ。
type ScrapeData struct {
	Markdown string            `json:"markdown"`
	HTML     string            `json:"html,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Scrape は 1 URL を Markdown 化する。
// pb.MarkdownDoc を組み立てて返す (Kitchen サービスの共通型)。
func (c *Client) Scrape(ctx context.Context, url string) (*pb.MarkdownDoc, error) {
	if c.BaseURL == "" {
		return nil, errors.New("toppings: BaseURL is empty (use NewFromMode)")
	}
	if url == "" {
		return nil, errors.New("toppings: url is required")
	}
	req := ScrapeRequest{
		URL:             url,
		Formats:         []string{"markdown"},
		OnlyMainContent: true,
		TimeoutMs:       c.timeoutMs(),
	}
	body, _ := json.Marshal(req)

	// Firecrawl /v1/scrape or /v2/scrape — v1 の方が docker 既存版で広く動く
	endpoint := c.BaseURL + "/v1/scrape"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)
	}

	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("toppings: http do: %w", err)
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &ErrClient{Status: resp.StatusCode, Body: buf.String()}
	}

	var out ScrapeResponse
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("toppings: decode: %w (body=%s)", err, buf.String())
	}
	if !out.Success || out.Data == nil {
		msg := out.Error
		if msg == "" {
			msg = "unknown"
		}
		return nil, fmt.Errorf("toppings: scrape unsuccessful: %s", msg)
	}
	doc := &pb.MarkdownDoc{
		Url:           url,
		Markdown:      out.Data.Markdown,
		Title:         out.Data.Metadata["title"],
		Metadata:      out.Data.Metadata,
		FetchedAtUnix: time.Now().Unix(),
	}
	return doc, nil
}

func (c *Client) timeoutMs() int32 {
	if c.TimeoutMs > 0 {
		return c.TimeoutMs
	}
	return 30000
}
