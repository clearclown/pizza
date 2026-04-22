package menu_test

import (
	"testing"

	"github.com/clearclown/pizza/internal/menu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolvePolygon_shinjukuJP(t *testing.T) {
	t.Parallel()
	p, err := menu.ResolvePolygon("新宿")
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Len(t, p.GetVertices(), 4)
	// 新宿駅は 35.69, 139.70 くらい → 範囲内に入る
	inside := false
	v := p.GetVertices()
	minLat, maxLat := v[0].GetLat(), v[2].GetLat()
	minLng, maxLng := v[0].GetLng(), v[2].GetLng()
	if 35.69 >= minLat && 35.69 <= maxLat && 139.70 >= minLng && 139.70 <= maxLng {
		inside = true
	}
	assert.True(t, inside, "新宿駅の座標が polygon 内側にあるべき")
}

func TestResolvePolygon_englishAlias(t *testing.T) {
	t.Parallel()
	p, err := menu.ResolvePolygon("shinjuku")
	require.NoError(t, err)
	p2, err := menu.ResolvePolygon("新宿")
	require.NoError(t, err)
	// 同じ bbox
	assert.Equal(t, p.GetVertices()[0].GetLat(), p2.GetVertices()[0].GetLat())

	// caps も OK
	_, err = menu.ResolvePolygon("Tokyo")
	require.NoError(t, err)
}

func TestResolvePolygon_tokyoPrefecture(t *testing.T) {
	t.Parallel()
	p, err := menu.ResolvePolygon("東京都")
	require.NoError(t, err)
	require.Len(t, p.GetVertices(), 4)
}

func TestResolvePolygon_allPrefecturesCoverable(t *testing.T) {
	t.Parallel()
	names := []string{"北海道", "沖縄県", "大阪府", "京都府", "福岡県", "愛知県"}
	for _, n := range names {
		p, err := menu.ResolvePolygon(n)
		require.NoError(t, err, "%s", n)
		assert.Len(t, p.GetVertices(), 4)
	}
}

func TestResolvePolygon_unknownReturnsHelpfulError(t *testing.T) {
	t.Parallel()
	_, err := menu.ResolvePolygon("nonexistentplace")
	require.Error(t, err)
	assert.ErrorContains(t, err, "unknown area")
	assert.ErrorContains(t, err, "use one of:")
}

func TestResolvePolygon_emptyIsError(t *testing.T) {
	t.Parallel()
	_, err := menu.ResolvePolygon("")
	assert.ErrorContains(t, err, "empty")
}

func TestKnownAreas_containsMajors(t *testing.T) {
	t.Parallel()
	list := menu.KnownAreas()
	assert.Contains(t, list, "東京都")
	assert.Contains(t, list, "新宿")
	assert.Contains(t, list, "大阪府")
	assert.Contains(t, list, "北海道")
}
