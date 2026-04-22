// Package toppings は M2 Kitchen Client — Firecrawl REST の Go クライアント
// と、Markdown から重要セクション URL を抽出する parser。
package toppings

import (
	"regexp"
	"strings"
)

// Markdown link パターン: [text](url)
var markdownLinkRe = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`)

// 優先度順の会社概要キーワード (上から順に試す)。
var companyKeywordsPriority = [][]string{
	{"会社概要", "企業情報", "会社情報"},
	{"運営会社", "運営", "運営元"},
	{"about us", "about"},
	{"corporate", "company"},
}

// 店舗一覧キーワード。
var storeListKeywordsPriority = [][]string{
	{"店舗一覧", "店舗検索", "店舗情報", "ショップ一覧"},
	{"store locator", "stores", "shop list", "locations"},
}

// ExtractCompanyURL は Markdown から「会社概要」「運営会社」などのリンクを抽出する。
// 優先度: 会社概要 > 運営会社 > about > corporate。見つからなければ ("", nil)。
//
// 開発工程.md §3.1 Parser Test に対応。
func ExtractCompanyURL(markdown string) (string, error) {
	return extractByKeywords(markdown, companyKeywordsPriority), nil
}

// ExtractStoreListURL は「店舗一覧」「Store Locator」などのリンクを抽出する。
func ExtractStoreListURL(markdown string) (string, error) {
	return extractByKeywords(markdown, storeListKeywordsPriority), nil
}

// extractByKeywords は Markdown 中の全 [text](url) を走査し、
// priority の先頭タイヤーから優先的に一致するリンクを探す。
func extractByKeywords(markdown string, priority [][]string) string {
	matches := markdownLinkRe.FindAllStringSubmatch(markdown, -1)
	if len(matches) == 0 {
		return ""
	}
	for _, tier := range priority {
		for _, m := range matches {
			text := strings.ToLower(strings.TrimSpace(m[1]))
			for _, kw := range tier {
				if strings.Contains(text, strings.ToLower(kw)) {
					return m[2]
				}
			}
		}
	}
	return ""
}
