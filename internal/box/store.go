// Package box は M4 Box — SQLite ベースの永続化層。
//
// 使い方:
//
//	store, err := box.Open("var/pizza.sqlite")
//	defer store.Close()
//	store.UpsertStore(ctx, &pb.Store{…})
//	csv, _ := store.ExportCSV(ctx, "エニタイムフィットネス")
package box

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	_ "modernc.org/sqlite" // pure-Go SQLite driver (no CGO)
)

//go:embed migrations.sql
var migrationsSQL string

// Store は SQLite 接続 + マイグレーション完了済のハンドル。
type Store struct {
	db   *sql.DB
	path string
}

// Open は path の SQLite を開き、スキーマをマイグレーションする。
// path="" の場合はメモリ DB を使う (テスト用)。
func Open(path string) (*Store, error) {
	dsn := path
	if dsn == "" {
		dsn = ":memory:"
	} else {
		// 親ディレクトリがなければ作成
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("box: mkdir %s: %w", dir, err)
			}
		}
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("box: open %s: %w", dsn, err)
	}
	// SQLite の並列耐性を少し上げる
	db.SetMaxOpenConns(1)
	s := &Store{db: db, path: path}
	if err := s.Migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close は DB ハンドルを閉じる。
func (s *Store) Close() error {
	return s.db.Close()
}

// Migrate はスキーマを適用する (idempotent)。
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, migrationsSQL); err != nil {
		return fmt.Errorf("box: migrate: %w", err)
	}
	// Phase 5.1: 既存 DB 向けの列追加 (CREATE TABLE IF NOT EXISTS では新規列が付かないため)
	if err := s.ensureOperatorStoresVerificationColumns(ctx); err != nil {
		return fmt.Errorf("box: migrate verification columns: %w", err)
	}
	return nil
}

// ensureOperatorStoresVerificationColumns は既存 DB に Layer D の列 (verification_score,
// corporate_number, verification_source) が無ければ ALTER で追加する。
// SQLite の CREATE TABLE IF NOT EXISTS は既存テーブルに列を足さないため、
// このパッチアップが必要。
func (s *Store) ensureOperatorStoresVerificationColumns(ctx context.Context) error {
	type colDef struct {
		Name string
		Type string
	}
	required := []colDef{
		{"verification_score", "REAL DEFAULT 0.0"},
		{"corporate_number", "TEXT"},
		{"verification_source", "TEXT"},
	}
	existing := make(map[string]bool)
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(operator_stores)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		existing[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range required {
		if existing[c.Name] {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE operator_stores ADD COLUMN %s %s", c.Name, c.Type)
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("add column %s: %w", c.Name, err)
		}
	}
	return nil
}

// UpsertStore は pb.Store を stores テーブルに upsert する (place_id 一意)。
func (s *Store) UpsertStore(ctx context.Context, st *pb.Store) error {
	if st == nil || st.GetPlaceId() == "" {
		return fmt.Errorf("box: UpsertStore requires non-empty PlaceId")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO stores (place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(place_id) DO UPDATE SET
			brand = excluded.brand,
			name = excluded.name,
			address = excluded.address,
			lat = excluded.lat,
			lng = excluded.lng,
			official_url = excluded.official_url,
			phone = excluded.phone,
			grid_cell_id = excluded.grid_cell_id
	`,
		st.GetPlaceId(), st.GetBrand(), st.GetName(), st.GetAddress(),
		st.GetLocation().GetLat(), st.GetLocation().GetLng(),
		st.GetOfficialUrl(), st.GetPhone(), st.GetGridCellId(),
	)
	if err != nil {
		return fmt.Errorf("box: upsert store %s: %w", st.GetPlaceId(), err)
	}
	return nil
}

// UpsertMarkdown は pb.MarkdownDoc を markdown_docs に upsert する。
// PlaceId は Metadata["place_id"] から取る (なければ空文字)。
func (s *Store) UpsertMarkdown(ctx context.Context, doc *pb.MarkdownDoc) error {
	if doc == nil || doc.GetUrl() == "" {
		return fmt.Errorf("box: UpsertMarkdown requires non-empty Url")
	}
	placeID := ""
	if doc.GetMetadata() != nil {
		placeID = doc.GetMetadata()["place_id"]
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO markdown_docs (url, place_id, title, markdown)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(url) DO UPDATE SET
			place_id = excluded.place_id,
			title = excluded.title,
			markdown = excluded.markdown,
			fetched_at = CURRENT_TIMESTAMP
	`, doc.GetUrl(), placeID, doc.GetTitle(), doc.GetMarkdown())
	if err != nil {
		return fmt.Errorf("box: upsert markdown %s: %w", doc.GetUrl(), err)
	}
	return nil
}

// UpsertJudgement は pb.JudgeResult を judgements に upsert する。
func (s *Store) UpsertJudgement(ctx context.Context, j *pb.JudgeResult) error {
	if j == nil || j.GetPlaceId() == "" {
		return fmt.Errorf("box: UpsertJudgement requires non-empty PlaceId")
	}
	isFC := 0
	if j.GetIsFranchise() {
		isFC = 1
	}
	// Phase 4: operation_type が空なら is_franchise から推論してレガシ互換を保つ
	opType := j.GetOperationType()
	if opType == "" {
		if isFC == 1 {
			opType = "franchisee"
		} else {
			opType = "direct"
		}
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO judgements (
			place_id, is_franchise, operator_name, store_count_estimate,
			confidence, llm_provider, llm_model,
			operation_type, franchisor_name, franchisee_name, judge_mode
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(place_id) DO UPDATE SET
			is_franchise = excluded.is_franchise,
			operator_name = excluded.operator_name,
			store_count_estimate = excluded.store_count_estimate,
			confidence = excluded.confidence,
			llm_provider = excluded.llm_provider,
			llm_model = excluded.llm_model,
			operation_type = excluded.operation_type,
			franchisor_name = excluded.franchisor_name,
			franchisee_name = excluded.franchisee_name,
			judge_mode = excluded.judge_mode,
			judged_at = CURRENT_TIMESTAMP
	`,
		j.GetPlaceId(), isFC, j.GetOperatorName(), j.GetStoreCountEstimate(),
		j.GetConfidence(), j.GetLlmProvider(), j.GetLlmModel(),
		opType, j.GetFranchisorName(), j.GetFranchiseeName(), j.GetJudgeMode(),
	)
	if err != nil {
		return fmt.Errorf("box: upsert judgement %s: %w", j.GetPlaceId(), err)
	}

	// Phase 5 bridge: operation_type=franchisee で運営会社名があれば
	// operator_stores にも自動で記録 (legacy テストとの互換)。
	// ここでの operator 名は franchisee_name を優先、なければ operator_name。
	if opType == "franchisee" || opType == "direct" {
		opName := j.GetFranchiseeName()
		if opName == "" && opType == "direct" {
			opName = j.GetFranchisorName()
		}
		if opName == "" {
			opName = j.GetOperatorName()
		}
		if opName != "" {
			_ = s.UpsertOperatorStore(ctx, &OperatorStoreInput{
				OperatorName:  opName,
				PlaceID:       j.GetPlaceId(),
				Brand:         "",
				OperatorType:  opType,
				Confidence:    j.GetConfidence(),
				DiscoveredVia: "judgement_bridge",
			})
		}
	}
	return nil
}

// QueryMegaFranchisees は judgements から minCount 以上の operator をストリームで返す。
func (s *Store) QueryMegaFranchisees(ctx context.Context, minCount int) ([]*pb.Megaji, error) {
	if minCount <= 0 {
		minCount = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT operator_name, store_count, avg_confidence
		FROM mega_franchisees
		WHERE store_count >= ?
		ORDER BY store_count DESC, avg_confidence DESC
	`, minCount)
	if err != nil {
		return nil, fmt.Errorf("box: query mega: %w", err)
	}
	defer rows.Close()

	var out []*pb.Megaji
	for rows.Next() {
		var name string
		var cnt int32
		var conf float64
		if err := rows.Scan(&name, &cnt, &conf); err != nil {
			return nil, err
		}
		out = append(out, &pb.Megaji{
			OperatorName:   name,
			StoreCount:     cnt,
			AvgConfidence:  conf,
		})
	}
	return out, rows.Err()
}

// ─── Phase 5: Research Pipeline (operator-first) ─────────────────────

// OperatorStoreInput は operator_stores への upsert 入力。
type OperatorStoreInput struct {
	OperatorName   string
	PlaceID        string
	Brand          string
	OperatorType   string  // direct | franchisee | unknown
	Confidence     float64
	DiscoveredVia  string  // per_store | chain_discovery | manual
}

// UpsertOperatorStore は確定した (operator, store) ペアを記録する。
// 同 operator で同 place_id は PRIMARY KEY 重複として更新される
// (より高い confidence で上書き)。
func (s *Store) UpsertOperatorStore(ctx context.Context, in *OperatorStoreInput) error {
	if in == nil || in.OperatorName == "" || in.PlaceID == "" {
		return fmt.Errorf("box: UpsertOperatorStore requires OperatorName and PlaceID")
	}
	via := in.DiscoveredVia
	if via == "" {
		via = "per_store"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO operator_stores
		  (operator_name, place_id, brand, operator_type, confidence, discovered_via)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(operator_name, place_id) DO UPDATE SET
		  brand          = COALESCE(excluded.brand, operator_stores.brand),
		  operator_type  = COALESCE(NULLIF(excluded.operator_type, ''), operator_stores.operator_type),
		  confidence     = MAX(excluded.confidence, operator_stores.confidence),
		  discovered_via = COALESCE(NULLIF(excluded.discovered_via, ''), operator_stores.discovered_via),
		  confirmed_at   = CURRENT_TIMESTAMP
	`, in.OperatorName, in.PlaceID, in.Brand, in.OperatorType, in.Confidence, via)
	if err != nil {
		return fmt.Errorf("box: upsert operator_stores: %w", err)
	}
	return nil
}

// StoreEvidenceInput は store_evidence insert 入力。
type StoreEvidenceInput struct {
	PlaceID     string
	EvidenceURL string
	Snippet     string
	Reason      string
	Keyword     string
}

// InsertStoreEvidence は 1 件の evidence を記録する。
// 同一 (place_id, evidence_url, snippet[:200]) の重複は insert しない (冪等)。
func (s *Store) InsertStoreEvidence(ctx context.Context, in *StoreEvidenceInput) error {
	if in == nil || in.PlaceID == "" || in.EvidenceURL == "" {
		return fmt.Errorf("box: InsertStoreEvidence requires PlaceID and EvidenceURL")
	}
	// 重複チェック
	var n int
	sig := in.Snippet
	if len(sig) > 200 {
		sig = sig[:200]
	}
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM store_evidence
		WHERE place_id = ? AND evidence_url = ? AND SUBSTR(snippet, 1, 200) = ?
	`, in.PlaceID, in.EvidenceURL, sig).Scan(&n)
	if err != nil {
		return fmt.Errorf("box: check evidence dup: %w", err)
	}
	if n > 0 {
		return nil
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO store_evidence (place_id, evidence_url, snippet, reason, keyword)
		VALUES (?, ?, ?, ?, ?)
	`, in.PlaceID, in.EvidenceURL, in.Snippet, in.Reason, in.Keyword)
	if err != nil {
		return fmt.Errorf("box: insert evidence: %w", err)
	}
	return nil
}

// OperatorStoreRow は QueryStoresByOperator の 1 行。
type OperatorStoreRow struct {
	PlaceID       string
	Brand         string
	OperatorType  string
	Confidence    float64
	DiscoveredVia string
}

// QueryStoresByOperator は指定 operator が運営する店舗一覧を返す。
func (s *Store) QueryStoresByOperator(ctx context.Context, operator string) ([]OperatorStoreRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT place_id, brand, operator_type, confidence, discovered_via
		FROM operator_stores
		WHERE operator_name = ?
		ORDER BY confirmed_at
	`, operator)
	if err != nil {
		return nil, fmt.Errorf("box: query stores by operator: %w", err)
	}
	defer rows.Close()
	var out []OperatorStoreRow
	for rows.Next() {
		var r OperatorStoreRow
		if err := rows.Scan(&r.PlaceID, &r.Brand, &r.OperatorType, &r.Confidence, &r.DiscoveredVia); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CountOperatorStores は operator_stores 全件数を返す (debug/metrics 用)。
func (s *Store) CountOperatorStores(ctx context.Context, operator string) (int, error) {
	var n int
	var err error
	if operator == "" {
		err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_stores`).Scan(&n)
	} else {
		err = s.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM operator_stores WHERE operator_name = ?`,
			operator).Scan(&n)
	}
	return n, err
}

// CountStores は指定ブランドの店舗数を返す (brand="" で全件)。
func (s *Store) CountStores(ctx context.Context, brand string) (int, error) {
	var n int
	var err error
	if brand == "" {
		err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM stores`).Scan(&n)
	} else {
		err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM stores WHERE brand = ?`, brand).Scan(&n)
	}
	return n, err
}

// ExportCSV は stores テーブルを CSV bytes で返す。
// brand="" で全件。Header 付き。
func (s *Store) ExportCSV(ctx context.Context, brand string) ([]byte, error) {
	q := `SELECT place_id, brand, name, address, lat, lng, official_url, phone, grid_cell_id, extracted_at FROM stores`
	args := []interface{}{}
	if brand != "" {
		q += ` WHERE brand = ?`
		args = append(args, brand)
	}
	q += ` ORDER BY extracted_at, place_id`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("box: export csv query: %w", err)
	}
	defer rows.Close()

	buf := new(strings.Builder)
	w := csv.NewWriter(buf)
	_ = w.Write([]string{"place_id", "brand", "name", "address", "lat", "lng", "official_url", "phone", "grid_cell_id", "extracted_at"})
	for rows.Next() {
		var placeID, brandCol, name, addr, url, phone, cell, ts string
		var lat, lng float64
		if err := rows.Scan(&placeID, &brandCol, &name, &addr, &lat, &lng, &url, &phone, &cell, &ts); err != nil {
			return nil, err
		}
		_ = w.Write([]string{
			placeID, brandCol, name, addr,
			strconv.FormatFloat(lat, 'f', -1, 64),
			strconv.FormatFloat(lng, 'f', -1, 64),
			url, phone, cell, ts,
		})
	}
	w.Flush()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := w.Error(); err != nil {
		return nil, err
	}
	return []byte(buf.String()), nil
}

// Path は DB ファイルパス (メモリ DB なら "")。
func (s *Store) Path() string { return s.path }
