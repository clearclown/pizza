// Package slice は Store 列の dedupe / sanitization を提供する。
//
// 開発工程.md §3.1 Sanitization Test 対応。
// Phase 0: スタブ。Phase 1-2 で実装。
package slice

import (
	"errors"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

var ErrNotImplemented = errors.New("slice: not implemented (Phase 1 target)")

// Dedupe は place_id ベースで Store 配列を重複排除する。
// 同一 place_id が複数ある場合は最初に出現したものを残す。
func Dedupe(stores []*pb.Store) ([]*pb.Store, error) {
	_ = stores
	return nil, ErrNotImplemented
}

// Sanitize は取得 Store の不正な文字列 (改行コード / 制御文字 / 前後空白) を正規化する。
func Sanitize(stores []*pb.Store) ([]*pb.Store, error) {
	_ = stores
	return nil, ErrNotImplemented
}
