package verifier_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/clearclown/pizza/internal/verifier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

// setupTestDB はテスト用SQLiteを作成し、houjin_registryテーブルとサンプルデータを投入する。
func setupTestDB(t *testing.T, records []map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "registry.sqlite")

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE houjin_registry (
			corporate_number  TEXT PRIMARY KEY,
			process           TEXT,
			update_date       TEXT,
			name              TEXT NOT NULL,
			normalized_name   TEXT,
			prefecture        TEXT,
			city              TEXT,
			street            TEXT,
			imported_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX idx_houjin_name ON houjin_registry(name);
	`)
	require.NoError(t, err)

	for _, rec := range records {
		_, err = db.Exec(
			`INSERT INTO houjin_registry
			(corporate_number, process, update_date, name, normalized_name, prefecture, city, street)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			rec["number"], rec["process"], rec["date"], rec["name"],
			rec["normalized"], rec["pref"], rec["city"], rec["street"],
		)
		require.NoError(t, err)
	}

	return dbPath
}

// ─── NewWithDB ─────────────────────────────────────────────────────────────

func TestNewWithDB_DBNotFound(t *testing.T) {
	_, err := verifier.NewWithDB("/nonexistent/registry.sqlite")
	assert.ErrorIs(t, err, verifier.ErrDBNotFound)
}

func TestNewWithDB_OK(t *testing.T) {
	dbPath := setupTestDB(t, nil)
	c, err := verifier.NewWithDB(dbPath)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

// ─── SearchByName ──────────────────────────────────────────────────────────

func TestSearchByName_ExactMatch(t *testing.T) {
	dbPath := setupTestDB(t, []map[string]string{
		{
			"number": "1234567890123", "process": "01", "date": "2026-04-01",
			"name": "株式会社テスト運営", "normalized": "テスト運営",
			"pref": "東京都", "city": "渋谷区", "street": "道玄坂1-1-1",
		},
	})
	c := verifier.NewWithDBUnchecked(dbPath)

	corps, err := c.SearchByName(context.Background(), "株式会社テスト運営", 10)
	require.NoError(t, err)
	require.Len(t, corps, 1)
	assert.Equal(t, "1234567890123", corps[0].CorporateNumber)
	assert.Equal(t, "株式会社テスト運営", corps[0].Name)
	assert.Equal(t, "東京都渋谷区道玄坂1-1-1", corps[0].Address)
	assert.True(t, corps[0].IsActive())
}

func TestSearchByName_PrefixFallback(t *testing.T) {
	// exact matchではなく prefix LIKE でヒットするケース
	dbPath := setupTestDB(t, []map[string]string{
		{
			"number": "1234567890124", "process": "01", "date": "2026-04-01",
			"name": "株式会社モスストアカンパニー", "normalized": "モスストアカンパニー",
			"pref": "東京都", "city": "品川区", "street": "1-1",
		},
	})
	c := verifier.NewWithDBUnchecked(dbPath)

	corps, err := c.SearchByName(context.Background(), "株式会社モスストア", 10)
	require.NoError(t, err)
	require.NotEmpty(t, corps)
}

func TestSearchByName_NotFound(t *testing.T) {
	dbPath := setupTestDB(t, nil)
	c := verifier.NewWithDBUnchecked(dbPath)

	_, err := c.SearchByName(context.Background(), "存在しない会社XYZ12345", 10)
	assert.ErrorIs(t, err, verifier.ErrNotFound)
}

func TestSearchByName_EmptyName(t *testing.T) {
	dbPath := setupTestDB(t, nil)
	c := verifier.NewWithDBUnchecked(dbPath)

	_, err := c.SearchByName(context.Background(), "", 10)
	assert.Error(t, err)
}

func TestSearchByName_InactiveFiltered(t *testing.T) {
	// process=71 (廃業) はアクティブフィルタで除外される
	dbPath := setupTestDB(t, []map[string]string{
		{
			"number": "1234567890125", "process": "71", "date": "2024-01-01",
			"name": "株式会社廃業テスト", "normalized": "廃業テスト",
			"pref": "東京都", "city": "千代田区", "street": "1-1",
		},
	})
	c := verifier.NewWithDBUnchecked(dbPath)

	_, err := c.SearchByName(context.Background(), "株式会社廃業テスト", 10)
	assert.ErrorIs(t, err, verifier.ErrNotFound)
}

// ─── Verify ────────────────────────────────────────────────────────────────

func TestVerify_Found(t *testing.T) {
	dbPath := setupTestDB(t, []map[string]string{
		{
			"number": "1234567890123", "process": "01", "date": "2026-04-01",
			"name": "株式会社テスト運営", "normalized": "テスト運営",
			"pref": "東京都", "city": "渋谷区", "street": "道玄坂1-1-1",
		},
	})
	c := verifier.NewWithDBUnchecked(dbPath)

	result := c.Verify(context.Background(), "テスト運営")
	assert.True(t, result.IsVerified)
	assert.Equal(t, "株式会社テスト運営", result.OfficialName)
	assert.Equal(t, "1234567890123", result.CorporateNumber)
	assert.True(t, result.IsActive)
	assert.Equal(t, "houjin_csv", result.Source)
	assert.Equal(t, "テスト運営", result.InputName)
}

func TestVerify_NotFound_ReturnsFalse(t *testing.T) {
	dbPath := setupTestDB(t, nil)
	c := verifier.NewWithDBUnchecked(dbPath)

	result := c.Verify(context.Background(), "存在しない会社XYZ")
	assert.False(t, result.IsVerified)
	assert.Equal(t, "存在しない会社XYZ", result.InputName)
	assert.Equal(t, "houjin_csv", result.Source)
}

// ─── Count ─────────────────────────────────────────────────────────────────

func TestCount(t *testing.T) {
	dbPath := setupTestDB(t, []map[string]string{
		{
			"number": "1234567890123", "process": "01", "date": "2026-04-01",
			"name": "株式会社A", "normalized": "a",
			"pref": "東京都", "city": "渋谷区", "street": "1-1",
		},
		{
			"number": "1234567890124", "process": "01", "date": "2026-04-01",
			"name": "株式会社B", "normalized": "b",
			"pref": "大阪府", "city": "大阪市", "street": "2-2",
		},
	})
	c := verifier.NewWithDBUnchecked(dbPath)

	n, err := c.Count(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

// ─── Live test ─────────────────────────────────────────────────────────────

func TestVerify_Live(t *testing.T) {
	dbPath := os.Getenv("HOUJIN_CSV_DB")
	if dbPath == "" {
		t.Skip("live test: set HOUJIN_CSV_DB=/path/to/registry.sqlite")
	}
	c, err := verifier.NewWithDB(dbPath)
	require.NoError(t, err)

	result := c.Verify(context.Background(), "株式会社モスフードサービス")
	t.Logf("result: %+v", result)
	assert.True(t, result.IsVerified, "モスフードサービスが見つからない")
}
