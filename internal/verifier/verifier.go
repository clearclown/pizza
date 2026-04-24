// Package verifier は国税庁法人番号APIを使って企業名を検証・正規化する。
//
// APIドキュメント: https://www.houjin-bangou.nta.go.jp/webapi/
//
// 環境変数:
//   - HOUJIN_BANGOU_APP_ID: 法人番号APIのアプリケーションID（Pythonサービスと共用）
//
// APIレスポンスはXML(type=12)形式。
// Python側の services/delivery/pizza_delivery/houjin_bangou.py と同一ロジックをGoで実装。
package verifier

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// baseURL は法人番号APIのベースURL (v4)。
	baseURL = "https://api.houjin-bangou.nta.go.jp/4"

	// defaultTimeout はHTTPクライアントのデフォルトタイムアウト。
	defaultTimeout = 10 * time.Second
)

// activeProcessCodes はアクティブ（廃業していない）と判断するprocessコード。
// 01: 新規, 11: 商号変更, 12: 本店移転, 13: 代表者変更, 21/22: 組織変更, 31: 国外本店
// 71-72: 吸収合併消滅/解散 → inactive
var activeProcessCodes = map[string]bool{
	"01": true, "11": true, "12": true, "13": true,
	"21": true, "22": true, "31": true,
}

// -- XML レスポンス構造体 --------------------------------------------------

// xmlCorpList は法人番号API XMLレスポンスのルート要素。
type xmlCorpList struct {
	XMLName      xml.Name       `xml:"corporateInfoList"`
	LastUpdate   string         `xml:"lastUpdateDate"`
	Count        int            `xml:"count"`
	Corporations []xmlCorporate `xml:"corporateInfo"`
}

// xmlCorporate は1法人のXML要素。
type xmlCorporate struct {
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

// -- ドメインモデル ----------------------------------------------------------

// Corporation は法人番号APIから取得した法人情報。
type Corporation struct {
	// CorporateNumber は13桁の法人番号。
	CorporateNumber string
	// Name は法人の正式名称。
	Name string
	// Address は都道府県名+市区町村名+番地を結合した住所。
	Address string
	// Process は処理区分コード。
	Process string
	// UpdateDate は更新日。
	UpdateDate string
	// CloseDate は廃業日 (空なら現役)。
	CloseDate string
}

// IsActive は法人が廃業していないかを返す。
func (c *Corporation) IsActive() bool {
	return activeProcessCodes[c.Process] && c.CloseDate == ""
}

// -- エラー定義 -------------------------------------------------------------

// ErrNoAppID は HOUJIN_BANGOU_APP_ID が未設定の場合に返る。
var ErrNoAppID = fmt.Errorf("verifier: HOUJIN_BANGOU_APP_ID が未設定です。https://www.houjin-bangou.nta.go.jp/webapi/ でトークンを取得してください")

// ErrNotFound は検索結果が0件だった場合に返る。
var ErrNotFound = fmt.Errorf("verifier: 法人が見つかりませんでした")

// -- クライアント -----------------------------------------------------------

// Client は法人番号APIクライアント。
type Client struct {
	appID      string
	httpClient *http.Client
	baseURL    string // テスト用にオーバーライド可能
}

// New は環境変数 HOUJIN_BANGOU_APP_ID からIDを読み込んでClientを生成する。
// IDが未設定の場合は ErrNoAppID を返す。
func New() (*Client, error) {
	appID := os.Getenv("HOUJIN_BANGOU_APP_ID")
	if appID == "" {
		return nil, ErrNoAppID
	}
	return NewWithAppID(appID), nil
}

// NewWithAppID は指定したappIDでClientを生成する。テスト・DI用。
func NewWithAppID(appID string) *Client {
	return &Client{
		appID:   appID,
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
}

// SetBaseURL はベースURLを上書きする。テスト用モックサーバー向け。
func (c *Client) SetBaseURL(u string) {
	c.baseURL = u
}

// SearchByName は企業名で法人を検索し、一致する法人一覧を返す。
//
// name: 検索したい企業名（部分一致）
func (c *Client) SearchByName(ctx context.Context, name string) ([]Corporation, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("verifier: 検索名が空です")
	}

	params := url.Values{}
	params.Set("id", c.appID)
	params.Set("name", name)
	params.Set("type", "12") // XML UTF-8
	params.Set("history", "0")

	endpoint := fmt.Sprintf("%s/name?%s", c.baseURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("verifier: リクエスト生成エラー: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("verifier: APIリクエストエラー: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("verifier: APIエラー status=%d body=%s", resp.StatusCode, string(body))
	}

	return parseXMLResponse(resp.Body)
}

// parseXMLResponse はAPIのXMLレスポンスをパースしてCorporation配列を返す。
func parseXMLResponse(r io.Reader) ([]Corporation, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("verifier: レスポンス読み込みエラー: %w", err)
	}

	// UTF-8でない場合の安全対策
	if !utf8.Valid(body) {
		return nil, fmt.Errorf("verifier: レスポンスがUTF-8ではありません")
	}

	var list xmlCorpList
	if err := xml.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("verifier: XMLパースエラー: %w", err)
	}

	corps := make([]Corporation, 0, len(list.Corporations))
	for _, x := range list.Corporations {
		corps = append(corps, Corporation{
			CorporateNumber: x.CorporateNumber,
			Name:            x.Name,
			Address:         x.PrefectureName + x.CityName + x.StreetNumber,
			Process:         x.Process,
			UpdateDate:      x.UpdateDate,
			CloseDate:       x.CloseDate,
		})
	}

	return corps, nil
}

// -- 検証ロジック -----------------------------------------------------------

// VerifyResult は企業名検証の結果。
type VerifyResult struct {
	// InputName は入力された企業名（正規化前）。
	InputName string
	// OfficialName は法人番号APIで確認された正式社名。
	OfficialName string
	// CorporateNumber は13桁の法人番号。
	CorporateNumber string
	// IsVerified は法人番号APIで実在が確認されたか。
	IsVerified bool
	// IsActive は廃業していないか。
	IsActive bool
	// Address は本社所在地。
	Address string
	// NameSimilarity は入力名と正式社名の類似度 [0.0, 1.0]。
	NameSimilarity float64
}

// Verify は企業名を法人番号APIで検証し、正式社名・法人番号を返す。
//
// APIエラー・法人未発見の場合は IsVerified=false で返す（エラーにしない）。
// これにより呼び出し元のパイプラインが中断しない。
func (c *Client) Verify(ctx context.Context, name string) VerifyResult {
	result := VerifyResult{InputName: name}

	corps, err := c.SearchByName(ctx, name)
	if err != nil {
		return result
	}

	// アクティブな法人の中から最もよく一致するものを選ぶ
	bestScore := 0.0
	var best *Corporation
	for i := range corps {
		if !corps[i].IsActive() {
			continue
		}
		score := nameSimilarity(name, corps[i].Name)
		if score > bestScore {
			bestScore = score
			best = &corps[i]
		}
	}

	if best == nil {
		return result
	}

	result.IsVerified = true
	result.OfficialName = best.Name
	result.CorporateNumber = best.CorporateNumber
	result.IsActive = best.IsActive()
	result.Address = best.Address
	result.NameSimilarity = bestScore

	return result
}

// -- 名寄せロジック ---------------------------------------------------------

// nameSimilarity は2つの法人名の類似度を [0.0, 1.0] で返す。
// canonical化後の完全一致→1.0、包含関係→0.9、その他はbi-gram Jaccard。
// Python側の _name_similarity() と同等のロジック。
func nameSimilarity(a, b string) float64 {
	ka, kb := canonicalKey(a), canonicalKey(b)
	if ka == "" || kb == "" {
		return 0.0
	}
	if ka == kb {
		return 1.0
	}
	if strings.Contains(ka, kb) || strings.Contains(kb, ka) {
		return 0.9
	}
	return bigramJaccard(ka, kb)
}

// canonicalKey は法人名を正規化する（株式会社/㈱/（株）などを除去・小文字化）。
func canonicalKey(name string) string {
	s := name
	// よくある略称・表記を統一
	replacer := strings.NewReplacer(
		"株式会社", "",
		"㈱", "",
		"（株）", "",
		"(株)", "",
		"有限会社", "",
		"㈲", "",
		"合同会社", "",
		"合資会社", "",
		"合名会社", "",
		" ", "",
		"　", "",
	)
	s = replacer.Replace(s)
	return strings.ToLower(strings.TrimSpace(s))
}

// bigramJaccard はbi-gramのJaccard類似度を計算する。
func bigramJaccard(a, b string) float64 {
	ra, rb := []rune(a), []rune(b)
	if len(ra) < 2 || len(rb) < 2 {
		return 0.0
	}
	setA := make(map[string]bool)
	for i := 0; i < len(ra)-1; i++ {
		setA[string(ra[i:i+2])] = true
	}
	setB := make(map[string]bool)
	for i := 0; i < len(rb)-1; i++ {
		setB[string(rb[i:i+2])] = true
	}
	intersection := 0
	for k := range setA {
		if setB[k] {
			intersection++
		}
	}
	union := len(setA) + len(setB) - intersection
	if union == 0 {
		return 0.0
	}
	return float64(intersection) / float64(union)
}
