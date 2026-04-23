// Package dough は M1 Seed Module — Google Places API (New) から
// 店舗を網羅抽出する Go 実装。
//
// 実装戦略:
//   - Places API (New) は REST のみなので、直接 HTTPS クライアントで叩く
//     (googlemaps/google-maps-services-go は legacy Places API が対象)
//   - Text Search と Nearby Search の両方をサポート
//   - API key は X-Goog-Api-Key ヘッダ、FieldMask で返却フィールドを指定
//   - pageToken でページング (1 回の呼び出しは最大 20 件、最大 60 件まで)
package dough

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// DefaultPlacesBaseURL は Places API (New) のエンドポイント。
const DefaultPlacesBaseURL = "https://places.googleapis.com/v1"

// DefaultFieldMask は Store を組み立てるのに必要な最小フィールド。
// 詳細: https://developers.google.com/maps/documentation/places/web-service/text-search#fieldmask
const DefaultFieldMask = "places.id,places.displayName,places.formattedAddress,places.location,places.nationalPhoneNumber,places.websiteUri,places.internationalPhoneNumber,nextPageToken"

// ErrAPI は Places API が非 2xx を返した際のエラー。
type ErrAPI struct {
	Status int
	Body   string
}

func (e *ErrAPI) Error() string {
	return fmt.Sprintf("places api error: status=%d body=%s", e.Status, e.Body)
}

// PlacesClient は Google Places API (New) の REST クライアント。
//
// Phase 27 bugfix: Multi-key pool 対応。複数の GCP project の key を
// round-robin することで実質 throughput を 2x/3x にスケール。
// APIKeys が指定されていればそちらを優先、空なら APIKey fallback。
type PlacesClient struct {
	APIKey     string        // 単一 key (backward compat)
	APIKeys    []string      // Phase 27: key pool (優先、あれば round-robin)
	BaseURL    string        // 空なら DefaultPlacesBaseURL
	HTTPClient *http.Client  // 空なら &http.Client{Timeout: 30s}
	FieldMask  string        // 空なら DefaultFieldMask
	Language   string        // "ja" など
	Region     string        // "JP" など

	keyIdx uint64            // round-robin カウンタ (atomic)
}

// selectAPIKey は round-robin で次の API key を返す。
// APIKeys が空なら APIKey を返す (既存動作)。
func (c *PlacesClient) selectAPIKey() string {
	if len(c.APIKeys) > 0 {
		// atomic インクリメントで thread-safe round-robin
		n := atomic.AddUint64(&c.keyIdx, 1) - 1
		return c.APIKeys[n%uint64(len(c.APIKeys))]
	}
	return c.APIKey
}

// KeyCount は有効 key 数を返す (設定検証用)。
func (c *PlacesClient) KeyCount() int {
	if len(c.APIKeys) > 0 {
		return len(c.APIKeys)
	}
	if c.APIKey != "" {
		return 1
	}
	return 0
}

func (c *PlacesClient) baseURL() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	return DefaultPlacesBaseURL
}

func (c *PlacesClient) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func (c *PlacesClient) fieldMask() string {
	if c.FieldMask != "" {
		return c.FieldMask
	}
	return DefaultFieldMask
}

// SearchTextRequest は Text Search への入力。
type SearchTextRequest struct {
	// TextQuery は検索文字列 (例: "エニタイムフィットネス 新宿")
	TextQuery string `json:"textQuery"`
	// LanguageCode は結果の言語 (例: "ja")
	LanguageCode string `json:"languageCode,omitempty"`
	// RegionCode は CLDR 地域コード (例: "JP")
	RegionCode string `json:"regionCode,omitempty"`
	// MaxResultCount は 1 回の呼び出しあたりの最大結果 (1-20)
	MaxResultCount int32 `json:"maxResultCount,omitempty"`
	// LocationRestriction はエリア限定 (円 or 矩形)。円を使う場合は LocationBias と排他。
	LocationRestriction *LocationRestriction `json:"locationRestriction,omitempty"`
	// LocationBias は結果をエリア寄りにするヒント。
	LocationBias *LocationBias `json:"locationBias,omitempty"`
	// PageToken はページング用。
	PageToken string `json:"pageToken,omitempty"`
}

// LocationRestriction は検索範囲の限定 (rectangle or circle)。
type LocationRestriction struct {
	Rectangle *Rectangle `json:"rectangle,omitempty"`
}

// LocationBias は検索範囲のヒント。
type LocationBias struct {
	Circle    *Circle    `json:"circle,omitempty"`
	Rectangle *Rectangle `json:"rectangle,omitempty"`
}

// Circle は中心 + 半径の円。
type Circle struct {
	Center Location `json:"center"`
	Radius float64  `json:"radius"` // meter
}

// Rectangle は SW/NE の矩形。
type Rectangle struct {
	Low  Location `json:"low"`
	High Location `json:"high"`
}

// Location は緯度経度。
type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// SearchTextResponse は Places API (New) からの応答 (Store 変換前の素)。
type SearchTextResponse struct {
	Places        []PlaceRaw `json:"places"`
	NextPageToken string     `json:"nextPageToken,omitempty"`
}

// PlaceRaw は Places API の 1 件分の素データ。
type PlaceRaw struct {
	ID                      string      `json:"id"`
	DisplayName             DisplayName `json:"displayName"`
	FormattedAddress        string      `json:"formattedAddress"`
	Location                Location    `json:"location"`
	NationalPhoneNumber     string      `json:"nationalPhoneNumber,omitempty"`
	InternationalPhoneNumber string     `json:"internationalPhoneNumber,omitempty"`
	WebsiteURI              string      `json:"websiteUri,omitempty"`
}

// DisplayName は多言語対応の名前。
type DisplayName struct {
	Text         string `json:"text"`
	LanguageCode string `json:"languageCode,omitempty"`
}

// SearchText は Places API (New) の :searchText エンドポイントを叩く。
// Phase 27: APIKeys が設定されていれば round-robin で複数 key に分散。
func (c *PlacesClient) SearchText(ctx context.Context, req *SearchTextRequest) (*SearchTextResponse, error) {
	apiKey := c.selectAPIKey()
	if apiKey == "" {
		return nil, errors.New("dough: PlacesClient has no API key (APIKey/APIKeys both empty)")
	}
	if req == nil || req.TextQuery == "" {
		return nil, errors.New("dough: SearchText requires non-empty TextQuery")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("dough: marshal request: %w", err)
	}
	url := c.baseURL() + "/places:searchText"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Goog-Api-Key", apiKey)
	httpReq.Header.Set("X-Goog-FieldMask", c.fieldMask())

	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("dough: http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return nil, &ErrAPI{Status: resp.StatusCode, Body: buf.String()}
	}

	var out SearchTextResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("dough: decode response: %w", err)
	}
	return &out, nil
}

// ToStore は PlaceRaw を pb.Store に変換する。
func (p *PlaceRaw) ToStore(brand, gridCellID string) *pb.Store {
	phone := p.NationalPhoneNumber
	if phone == "" {
		phone = p.InternationalPhoneNumber
	}
	return &pb.Store{
		PlaceId:     p.ID,
		Brand:       brand,
		Name:        p.DisplayName.Text,
		Address:     p.FormattedAddress,
		Location:    &pb.LatLng{Lat: p.Location.Latitude, Lng: p.Location.Longitude},
		OfficialUrl: p.WebsiteURI,
		Phone:       phone,
		GridCellId:  gridCellID,
	}
}
