// Package toppings は M2 Kitchen Client — Firecrawl REST の Go クライアント
// と、Markdown から重要セクション URL を抽出する parser。
//
// Phase 0: スタブ。Phase 2 で実装。
package toppings

import "errors"

// ErrNotImplemented は Phase 0 スタブが呼ばれたときに返る。
var ErrNotImplemented = errors.New("toppings: not implemented (Phase 2 target)")

// ExtractCompanyURL は Markdown から「会社概要」「運営会社」「About」などの
// リンクを抽出する。最初の 1 件を返す (優先度: 会社概要 > 運営会社 > about > corporate)。
//
// 開発工程.md §3.1 Parser Test に対応。
func ExtractCompanyURL(markdown string) (string, error) {
	_ = markdown
	return "", ErrNotImplemented
}

// ExtractStoreListURL は「店舗一覧」「店舗検索」「Store Locator」などを抽出する。
func ExtractStoreListURL(markdown string) (string, error) {
	_ = markdown
	return "", ErrNotImplemented
}
