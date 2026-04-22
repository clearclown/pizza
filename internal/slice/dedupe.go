// Package slice は Store 列の dedupe / sanitization を提供する。
// 開発工程.md §3.1 Sanitization Test 対応。
package slice

import (
	"strings"
	"unicode"

	pb "github.com/clearclown/pizza/gen/go/pizza/v1"
)

// Dedupe は place_id ベースで Store 配列を重複排除する。
// 同一 place_id が複数ある場合は最初に出現したものを残す。
// nil 入力は nil + nil エラーを返す (呼び出し元で length=0 扱い)。
func Dedupe(stores []*pb.Store) ([]*pb.Store, error) {
	if stores == nil {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(stores))
	out := make([]*pb.Store, 0, len(stores))
	for _, s := range stores {
		if s == nil {
			continue
		}
		id := s.GetPlaceId()
		if id == "" {
			// place_id 無しの Store はそのまま通す (重複判定不能)
			out = append(out, s)
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, s)
	}
	return out, nil
}

// Sanitize は取得 Store の不正な文字列 (改行・制御文字・前後空白) を正規化する。
// 元の Store はコピーして変更し、入力は変更しない (immutable-ish)。
func Sanitize(stores []*pb.Store) ([]*pb.Store, error) {
	out := make([]*pb.Store, 0, len(stores))
	for _, s := range stores {
		if s == nil {
			continue
		}
		cp := *s // value copy
		cp.Name = cleanString(cp.GetName())
		cp.Address = cleanString(cp.GetAddress())
		cp.Brand = cleanString(cp.GetBrand())
		cp.OfficialUrl = strings.TrimSpace(cp.GetOfficialUrl())
		cp.Phone = cleanString(cp.GetPhone())
		out = append(out, &cp)
	}
	return out, nil
}

// cleanString は制御文字 (除 \n が意図的に残る場合もある) とタブを除去し、
// 前後空白を trim する。
func cleanString(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.IsControl(r) {
			continue
		}
		if r == '\t' {
			continue
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}
