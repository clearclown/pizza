// 🔴 Red phase test — 開発工程.md §3.1 Parser Test 相当。
//
// 「主要な FC サイトのフッターから会社概要 URL を抽出できるか」を
// 代表的な Markdown パターンで検証する。
package toppings_test

import (
	"testing"

	"github.com/clearclown/pizza/internal/toppings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const tokyoFCFooter = `# TOP

... (中略) ...

---

## サイトマップ

- [ホーム](/)
- [店舗検索](/stores/)
- [会社概要](/company/about/)
- [採用情報](/recruit/)
- [お問い合わせ](/contact/)
`

const alternateFooter = `### フッター

| 会社情報 | サービス |
|---|---|
| [運営会社](/corp/) | [店舗一覧](/shops/) |
| [ニュース](/news) | [FAQ](/faq) |
`

const englishFooter = `## Footer

- [About Us](/about)
- [Our Stores](/stores)
- [Corporate](/corporate/)
`

func TestExtractCompanyURL_findsAboutCompany(t *testing.T) {
	t.Parallel()
	url, err := toppings.ExtractCompanyURL(tokyoFCFooter)
	require.NoError(t, err)
	assert.Equal(t, "/company/about/", url)
}

func TestExtractCompanyURL_prefersOperatorWhenAboutMissing(t *testing.T) {
	t.Parallel()
	url, err := toppings.ExtractCompanyURL(alternateFooter)
	require.NoError(t, err)
	assert.Equal(t, "/corp/", url, "should prefer 運営会社 when 会社概要 is absent")
}

func TestExtractCompanyURL_fallsBackToEnglish(t *testing.T) {
	t.Parallel()
	url, err := toppings.ExtractCompanyURL(englishFooter)
	require.NoError(t, err)
	assert.Contains(t, []string{"/about", "/corporate/"}, url)
}

func TestExtractCompanyURL_returnsEmptyWhenNoMatch(t *testing.T) {
	t.Parallel()
	url, err := toppings.ExtractCompanyURL("# Just a title\n\nno links here")
	require.NoError(t, err)
	assert.Equal(t, "", url)
}

func TestExtractStoreListURL_findsShopLocator(t *testing.T) {
	t.Parallel()
	url, err := toppings.ExtractStoreListURL(alternateFooter)
	require.NoError(t, err)
	assert.Equal(t, "/shops/", url)
}
