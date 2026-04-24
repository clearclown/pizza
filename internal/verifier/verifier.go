// Package verifier は国税庁法人番号CSVから生成したローカルSQLiteを使って
// 企業名を検証・正規化する。APIキー不要・オフライン動作。
//
// # セットアップ
//
//  1. 国税庁 全件CSVをダウンロード:
//     https://www.houjin-bangou.nta.go.jp/download/
//     → 「全件データ (Unicode版 zip)」を入手
//
//  2. Pythonでインポート:
//     cd services/delivery
//     uv run python -m pizza_delivery.houjin_csv import --csv /path/to/00_zenkoku_all_YYYYMMDD.zip
//     → var/houjin/registry.sqlite に取り込まれる (~577万件, 1-2分)
//
//  3. Go から参照:
//     c := verifier.New()  // or verifier.NewWithDB("var/houjin/registry.sqlite")
//     result := c.Verify(ctx, "株式会社モスストアカンパニー")
//
// Python側: services/delivery/pizza_delivery/houjin_csv.py と同一スキーマを参照。
package verifier

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	_ "modernc.org/sqlite" // CGO不要のSQLiteドライバ
)

// activeProcessCodes はアクティブ（廃業していない）と判断するprocessコード。
// 01: 新規, 11: 商号変更, 12: 本店移転, 13: 代表者変更, 21/22: 組織変更, 31: 国外本店
// 71-72: 吸収合併消滅/解散 → inactive
var activeProcessCodes = map[string]bool{
	"01": true, "11": true, "12": true, "13": true,
	"21": true, "22": true, "31": true,
}

// defaultDBPath はvar/houjin/registry.sqliteのパスを返す。
// Pythonの houjin_csv.py の _default_db_path() と同等。
func defaultDBPath() string {
	// __file__ から repo root を探す (Go ランタイムでは実行バイナリ基準)
	// 環境変数 PIZZA_ROOT を優先、なければ実行ファイルから遡る
	if root := os.Getenv("PIZZA_ROOT"); root != "" {
		return filepath.Join(root, "var", "houjin", "registry.sqlite")
	}
	// go test 実行時は package dir が cwd になる
	_, file, _, ok := runtime.Caller(0)
	if ok {
		// internal/verifier/verifier.go → ../../.. が repo root
		root := filepath.Join(filepath.Dir(file), "..", "..", "..")
		return filepath.Clean(filepath.Join(root, "var", "houjin", "registry.sqlite"))
	}
	return filepath.Join("var", "houjin", "registry.sqlite")
}

// -- ドメインモデル ----------------------------------------------------------

// Corporation はローカルSQLiteから取得した法人情報。
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
}

// IsActive は法人が廃業していないかを返す。
func (c *Corporation) IsActive() bool {
	return activeProcessCodes[c.Process]
}

// -- エラー定義 -------------------------------------------------------------

// ErrDBNotFound はSQLiteファイルが存在しない場合に返る。
var ErrDBNotFound = fmt.Errorf("verifier: var/houjin/registry.sqlite が見つかりません。\n" +
	"セットアップ手順: https://github.com/clearclown/pizza#法人番号csv\n" +
	"  cd services/delivery\n" +
	"  uv run python -m pizza_delivery.houjin_csv import --csv /path/to/00_zenkoku_all_YYYYMMDD.zip")

// ErrNotFound は検索結果が0件だった場合に返る。
var ErrNotFound = fmt.Errorf("verifier: 法人が見つかりませんでした")

// ErrDBEmpty はSQLiteが空（CSVが未インポート）の場合に返る。
var ErrDBEmpty = fmt.Errorf("verifier: registry.sqlite が空です。houjin_csv import を先に実行してください")

// -- クライアント -----------------------------------------------------------

// Client は法人番号ローカルSQLiteクライアント。
type Client struct {
	dbPath string
}

// New はデフォルトのSQLiteパス (var/houjin/registry.sqlite) でClientを生成する。
// DBが存在しない場合は ErrDBNotFound を返す。
func New() (*Client, error) {
	return NewWithDB("")
}

// NewWithDB は指定したSQLiteパスでClientを生成する。
// path が空の場合はデフォルトパスを使用。
// DBが存在しない場合は ErrDBNotFound を返す。
func NewWithDB(path string) (*Client, error) {
	if path == "" {
		path = defaultDBPath()
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, ErrDBNotFound
	}
	return &Client{dbPath: path}, nil
}

// NewWithDBUnchecked はDBファイルの存在確認なしでClientを生成する。テスト用。
func NewWithDBUnchecked(path string) *Client {
	return &Client{dbPath: path}
}

// -- 検索 ------------------------------------------------------------------

// SearchByName は法人名でSQLiteを検索し、一致する法人一覧を返す。
//
// 3段階fallback (Python houjin_csv.py の search_by_name と同等):
//  1. exact match → index が即ヒット O(log N)
//  2. prefix LIKE → index が使える
//  3. substring LIKE → O(N) だが最終手段
func (c *Client) SearchByName(ctx context.Context, name string, limit int) ([]Corporation, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("verifier: 検索名が空です")
	}
	if limit <= 0 {
		limit = 20
	}

	db, err := sql.Open("sqlite", c.dbPath)
	if err != nil {
		return nil, fmt.Errorf("verifier: DB接続エラー: %w", err)
	}
	defer db.Close()

	// テーブル存在確認
	var cnt int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='houjin_registry'",
	).Scan(&cnt); err != nil || cnt == 0 {
		return nil, ErrDBEmpty
	}

	activeCodes := sortedKeys(activeProcessCodes)
	placeholders := strings.Repeat("?,", len(activeCodes))
	placeholders = placeholders[:len(placeholders)-1]

	baseQ := fmt.Sprintf(
		"SELECT corporate_number, process, update_date, name, prefecture, city, street "+
			"FROM houjin_registry WHERE %%s AND process IN (%s) LIMIT ?",
		placeholders,
	)

	// Step 1: exact match
	args := append([]any{name}, stringsToAny(activeCodes)...)
	args = append(args, limit)
	corps, err := queryCorps(ctx, db, fmt.Sprintf(baseQ, "name = ?"), args)
	if err != nil {
		return nil, err
	}
	if len(corps) > 0 {
		return corps, nil
	}

	// Step 2: prefix LIKE
	args[0] = name + "%"
	corps, err = queryCorps(ctx, db, fmt.Sprintf(baseQ, "name LIKE ?"), args)
	if err != nil {
		return nil, err
	}
	if len(corps) > 0 {
		return corps, nil
	}

	// Step 3: substring LIKE (fallback)
	args[0] = "%" + name + "%"
	corps, err = queryCorps(ctx, db, fmt.Sprintf(baseQ, "name LIKE ?"), args)
	if err != nil {
		return nil, err
	}
	if len(corps) > 0 {
		return corps, nil
	}

	return nil, ErrNotFound
}

func queryCorps(ctx context.Context, db *sql.DB, q string, args []any) ([]Corporation, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("verifier: クエリエラー: %w", err)
	}
	defer rows.Close()

	var corps []Corporation
	for rows.Next() {
		var c Corporation
		var pref, city, street string
		if err := rows.Scan(
			&c.CorporateNumber, &c.Process, &c.UpdateDate, &c.Name,
			&pref, &city, &street,
		); err != nil {
			return nil, fmt.Errorf("verifier: scanエラー: %w", err)
		}
		c.Address = pref + city + street
		corps = append(corps, c)
	}
	return corps, rows.Err()
}

// Count はSQLiteの登録件数を返す。
func (c *Client) Count(ctx context.Context) (int, error) {
	db, err := sql.Open("sqlite", c.dbPath)
	if err != nil {
		return 0, fmt.Errorf("verifier: DB接続エラー: %w", err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM houjin_registry").Scan(&n); err != nil {
		return 0, fmt.Errorf("verifier: countエラー: %w", err)
	}
	return n, nil
}

// -- 検証ロジック -----------------------------------------------------------

// VerifyResult は企業名検証の結果。
// verifier の責務は「社名→法人番号の名寄せのみ」。
// IsCentral/IsBrandHQ 判定は L2(internal/kitchen) の責務。
type VerifyResult struct {
	// InputName は入力された企業名（正規化前）。
	InputName string
	// OfficialName は法人番号CSVで確認された正式社名。
	OfficialName string
	// CorporateNumber は13桁の法人番号。
	CorporateNumber string
	// IsVerified はCSVで実在が確認されたか。
	IsVerified bool
	// IsActive は廃業していないか。
	IsActive bool
	// Address は本社所在地。
	Address string
	// NameSimilarity は入力名と正式社名の類似度 [0.0, 1.0]。
	NameSimilarity float64
	// Source は常に "houjin_csv"。
	Source string

	// --- 以下は Phase 2 で追加したフィールド（PR#8 FU） ---

	// MatchLevel は名寄せ結果の種別。
	// MatchExact | MatchPartial | MatchAmbiguous | MatchNotFound | MatchAPIError
	MatchLevel string
	// FallbackLevel はどの検索戦略でヒットしたかの記録（デバッグ・チューニング用）。
	// "exact" | "prefix" | "substring" | "not_found" | "api_error"
	FallbackLevel string
	// Candidates は MatchAmbiguous 時の複数候補。ResolveAmbiguous() が選別する。
	Candidates []Candidate
	// HumanReviewRequired は自動解決不能な場合に true。
	// パイプラインは止めず、下流が review_queue に積む（PR#10）。
	HumanReviewRequired bool
	// HumanReviewReason は HumanReviewRequired=true の理由。
	// "score_too_close" | "no_address_match" | "multiple_furigana_match"
	HumanReviewReason string
	// ResolvedBy は ambiguous 解決に使った戦略の記録。
	// "address" | "furigana" | "score" | ""
	ResolvedBy string
}

// Verify は企業名をローカルSQLiteで検証し、正式社名・法人番号を返す。
//
// DBエラー・法人未発見の場合は IsVerified=false で返す（エラーにしない）。
// これにより呼び出し元のパイプラインが中断しない。
func (c *Client) Verify(ctx context.Context, name string) VerifyResult {
	result := VerifyResult{InputName: name, Source: "houjin_csv"}

	corps, err := c.SearchByName(ctx, name, 20)
	if err != nil {
		return result
	}

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
		return 0.9 // TODO(phase2): move to config (name_similarity_threshold)
	}
	return bigramJaccard(ka, kb)
}

// canonicalKey は法人名を正規化する（株式会社/㈱/(株)等を除去・小文字化）。
// Python側の normalize.canonical_key() と同等。
func canonicalKey(name string) string {
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
	return strings.ToLower(strings.TrimSpace(replacer.Replace(name)))
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

// -- ユーティリティ ---------------------------------------------------------

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func stringsToAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
