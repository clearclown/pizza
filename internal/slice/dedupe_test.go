// 🔴 Red phase test — 開発工程.md §3.1 Sanitization Test 相当。
package slice_test

import (
	"testing"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
	"github.com/clearclown/pizza/internal/slice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedupe_removesDuplicatePlaceIDs(t *testing.T) {
	t.Parallel()
	in := []*pb.Store{
		{PlaceId: "a", Name: "Store A"},
		{PlaceId: "b", Name: "Store B"},
		{PlaceId: "a", Name: "Store A duplicate"},
		{PlaceId: "c", Name: "Store C"},
		{PlaceId: "b", Name: "Store B duplicate"},
	}
	out, err := slice.Dedupe(in)
	require.NoError(t, err)
	assert.Len(t, out, 3, "5 stores should dedupe to 3 unique place_ids")

	ids := map[string]bool{}
	for _, s := range out {
		ids[s.GetPlaceId()] = true
	}
	assert.True(t, ids["a"] && ids["b"] && ids["c"])
}

func TestDedupe_keepsFirstOccurrence(t *testing.T) {
	t.Parallel()
	in := []*pb.Store{
		{PlaceId: "x", Name: "First"},
		{PlaceId: "x", Name: "Second"},
	}
	out, err := slice.Dedupe(in)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "First", out[0].GetName(), "dedupe must keep the first occurrence")
}

func TestDedupe_nilSafe(t *testing.T) {
	t.Parallel()
	out, err := slice.Dedupe(nil)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestSanitize_stripsControlCharsAndTrimsSpaces(t *testing.T) {
	t.Parallel()
	in := []*pb.Store{
		{PlaceId: "s1", Name: "  新宿店\n", Address: "東京都\t新宿区\x00"},
	}
	out, err := slice.Sanitize(in)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "新宿店", out[0].GetName())
	assert.NotContains(t, out[0].GetAddress(), "\x00")
	assert.NotContains(t, out[0].GetAddress(), "\t")
}
