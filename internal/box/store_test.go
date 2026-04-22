package box_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/box"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleStore(id, brand string) *pb.Store {
	return &pb.Store{
		PlaceId:     id,
		Brand:       brand,
		Name:        "店舗 " + id,
		Address:     "東京都新宿区",
		Location:    &pb.LatLng{Lat: 35.69, Lng: 139.70},
		OfficialUrl: "https://example.com/" + id,
		Phone:       "03-0000-0000",
		GridCellId:  "cell-00001",
	}
}

func TestOpen_createsSchemaAndClose(t *testing.T) {
	t.Parallel()
	s, err := box.Open("")
	require.NoError(t, err)
	defer s.Close()

	// CountStores 0 が返るので初期スキーマが適用されていることを確認
	n, err := s.CountStores(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestOpen_createsDirectoryIfMissing(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "nested", "subdir", "pizza.sqlite")
	s, err := box.Open(path)
	require.NoError(t, err)
	defer s.Close()
	assert.Equal(t, path, s.Path())
}

func TestStore_UpsertStore_insertAndUpdate(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()

	// insert
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p1", "エニタイム")))
	n, _ := s.CountStores(ctx, "")
	assert.Equal(t, 1, n)

	// update (same place_id, different name)
	updated := sampleStore("p1", "エニタイム")
	updated.Name = "更新された店舗"
	require.NoError(t, s.UpsertStore(ctx, updated))

	n, _ = s.CountStores(ctx, "")
	assert.Equal(t, 1, n, "upsert should not create duplicates")
}

func TestStore_UpsertStore_rejectsEmptyPlaceID(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	err := s.UpsertStore(context.Background(), &pb.Store{})
	assert.ErrorContains(t, err, "PlaceId")
}

func TestStore_UpsertMarkdown_withPlaceIdMetadata(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p1", "b")))
	doc := &pb.MarkdownDoc{
		Url:      "https://example.com/p1",
		Title:    "会社概要",
		Markdown: "# 会社概要\n\n株式会社テスト",
		Metadata: map[string]string{"place_id": "p1", "lang": "ja"},
	}
	require.NoError(t, s.UpsertMarkdown(ctx, doc))

	// 再度 upsert (title 変更)
	doc.Title = "新しいタイトル"
	require.NoError(t, s.UpsertMarkdown(ctx, doc))
}

func TestStore_UpsertJudgement_andQueryMegaFranchisees(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()

	// operator X 運営の店舗 3 件、operator Y 運営の店舗 2 件を作る
	for i, op := range []string{"X", "X", "X", "Y", "Y"} {
		placeID := string('a'+rune(i)) + "-id"
		require.NoError(t, s.UpsertStore(ctx, sampleStore(placeID, "test")))
		require.NoError(t, s.UpsertJudgement(ctx, &pb.JudgeResult{
			PlaceId:      placeID,
			IsFranchise:  true,
			OperatorName: op,
			Confidence:   0.8,
		}))
	}

	// minCount=3 → X だけ
	mega, err := s.QueryMegaFranchisees(ctx, 3)
	require.NoError(t, err)
	require.Len(t, mega, 1)
	assert.Equal(t, "X", mega[0].GetOperatorName())
	assert.Equal(t, int32(3), mega[0].GetStoreCount())

	// minCount=2 → X, Y
	mega, err = s.QueryMegaFranchisees(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, mega, 2)
}

func TestStore_ExportCSV_hasHeaderAndRowsInOrder(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p1", "A")))
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p2", "A")))
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p3", "B")))

	// 全件
	csvBytes, err := s.ExportCSV(ctx, "")
	require.NoError(t, err)
	csvStr := string(csvBytes)
	assert.True(t, strings.HasPrefix(csvStr, "place_id,brand,name,address,lat,lng,official_url,phone,grid_cell_id,extracted_at"))
	lines := strings.Split(strings.TrimRight(csvStr, "\n"), "\n")
	assert.Equal(t, 4, len(lines), "header + 3 rows")

	// ブランド絞り込み
	csvBytes, _ = s.ExportCSV(ctx, "A")
	lines = strings.Split(strings.TrimRight(string(csvBytes), "\n"), "\n")
	assert.Equal(t, 3, len(lines), "header + 2 rows for brand A")
}

func TestStore_roundTripThroughFile(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "pizza.sqlite")
	s, err := box.Open(tmp)
	require.NoError(t, err)
	require.NoError(t, s.UpsertStore(context.Background(), sampleStore("k1", "X")))
	require.NoError(t, s.Close())

	// 同じファイルを別ハンドルで開いて読む
	s2, err := box.Open(tmp)
	require.NoError(t, err)
	defer s2.Close()
	n, _ := s2.CountStores(context.Background(), "")
	assert.Equal(t, 1, n)
}

// ─── Phase 5: operator_stores + store_evidence tests ────────────────

func TestStore_UpsertOperatorStore_insertAndUpdate(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p1", "B")))

	// insert
	require.NoError(t, s.UpsertOperatorStore(ctx, &box.OperatorStoreInput{
		OperatorName:  "株式会社AFJ",
		PlaceID:       "p1",
		Brand:         "B",
		OperatorType:  "franchisee",
		Confidence:    0.85,
		DiscoveredVia: "per_store",
	}))
	n, err := s.CountOperatorStores(ctx, "株式会社AFJ")
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// update (higher confidence wins)
	require.NoError(t, s.UpsertOperatorStore(ctx, &box.OperatorStoreInput{
		OperatorName:  "株式会社AFJ",
		PlaceID:       "p1",
		Brand:         "B",
		OperatorType:  "franchisee",
		Confidence:    0.95,
		DiscoveredVia: "chain_discovery",
	}))
	// still 1 row
	n2, _ := s.CountOperatorStores(ctx, "株式会社AFJ")
	assert.Equal(t, 1, n2)
}

func TestStore_UpsertOperatorStore_rejectsEmpty(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	err := s.UpsertOperatorStore(context.Background(), &box.OperatorStoreInput{})
	assert.ErrorContains(t, err, "requires OperatorName and PlaceID")
}

func TestStore_QueryStoresByOperator_returnsAllForOperator(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()

	// 2 店舗を AFJ に紐付け
	for _, pid := range []string{"p1", "p2"} {
		require.NoError(t, s.UpsertStore(ctx, sampleStore(pid, "エニタイム")))
		require.NoError(t, s.UpsertOperatorStore(ctx, &box.OperatorStoreInput{
			OperatorName:  "株式会社AFJ Project",
			PlaceID:       pid,
			Brand:         "エニタイム",
			OperatorType:  "franchisee",
			Confidence:    0.9,
			DiscoveredVia: "per_store",
		}))
	}
	// 1 店舗を別 operator に紐付け
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p3", "エニタイム")))
	require.NoError(t, s.UpsertOperatorStore(ctx, &box.OperatorStoreInput{
		OperatorName:  "株式会社Other",
		PlaceID:       "p3",
		Brand:         "エニタイム",
		OperatorType:  "franchisee",
		Confidence:    0.8,
		DiscoveredVia: "per_store",
	}))

	rows, err := s.QueryStoresByOperator(ctx, "株式会社AFJ Project")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	ids := []string{rows[0].PlaceID, rows[1].PlaceID}
	assert.ElementsMatch(t, []string{"p1", "p2"}, ids)
}

func TestStore_InsertStoreEvidence_dedupe(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()
	require.NoError(t, s.UpsertStore(ctx, sampleStore("p1", "B")))

	in := &box.StoreEvidenceInput{
		PlaceID:     "p1",
		EvidenceURL: "https://example.com/about",
		Snippet:     "運営会社: 株式会社テスト",
		Reason:      "operator_keyword",
		Keyword:     "運営会社",
	}
	require.NoError(t, s.InsertStoreEvidence(ctx, in))
	// 同一 snippet prefix → 重複として insert されない
	require.NoError(t, s.InsertStoreEvidence(ctx, in))

	var n int
	err := s.InsertStoreEvidence(ctx, &box.StoreEvidenceInput{
		PlaceID: "", EvidenceURL: "x",
	})
	assert.ErrorContains(t, err, "requires PlaceID")
	_ = n
}

func TestStore_MegaFranchiseesView_usesOperatorStores(t *testing.T) {
	t.Parallel()
	s, _ := box.Open("")
	defer s.Close()
	ctx := context.Background()

	// X が 3 店舗、Y が 1 店舗 を運営
	stores := []struct {
		operator string
		placeID  string
	}{
		{"株式会社X", "p1"},
		{"株式会社X", "p2"},
		{"株式会社X", "p3"},
		{"株式会社Y", "p4"},
	}
	for _, st := range stores {
		require.NoError(t, s.UpsertStore(ctx, sampleStore(st.placeID, "B")))
		require.NoError(t, s.UpsertOperatorStore(ctx, &box.OperatorStoreInput{
			OperatorName:  st.operator,
			PlaceID:       st.placeID,
			Brand:         "B",
			OperatorType:  "franchisee",
			Confidence:    0.9,
			DiscoveredVia: "per_store",
		}))
	}

	mega, err := s.QueryMegaFranchisees(ctx, 3)
	require.NoError(t, err)
	require.Len(t, mega, 1)
	assert.Equal(t, "株式会社X", mega[0].GetOperatorName())
	assert.Equal(t, int32(3), mega[0].GetStoreCount())
}
