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
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO judgements (place_id, is_franchise, operator_name, store_count_estimate, confidence, llm_provider, llm_model)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(place_id) DO UPDATE SET
			is_franchise = excluded.is_franchise,
			operator_name = excluded.operator_name,
			store_count_estimate = excluded.store_count_estimate,
			confidence = excluded.confidence,
			llm_provider = excluded.llm_provider,
			llm_model = excluded.llm_model,
			judged_at = CURRENT_TIMESTAMP
	`, j.GetPlaceId(), isFC, j.GetOperatorName(), j.GetStoreCountEstimate(),
		j.GetConfidence(), j.GetLlmProvider(), j.GetLlmModel(),
	)
	if err != nil {
		return fmt.Errorf("box: upsert judgement %s: %w", j.GetPlaceId(), err)
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
