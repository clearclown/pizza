// Package megafranchisee - Phase 19/20 クロスブランド集計ゴールデンテスト。
//
// `pizza megafranchisee` のスナップショット CSV (operators-YYYY-MM-DD.csv)
// を固定化し、次の不変条件を検証する:
//   - CSV は非空で header + 1 行以上 data
//   - total_stores >= 1 (厳密正)
//   - brand_count >= 1 (厳密正)
//   - brands_breakdown に掲載の件数 sum が total_stores 以上
//   - 法人番号が埋まっている行は 13 桁 + 全桁数字
//   - total_stores 降順でソート済
package megafranchisee

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var corporateNumberRe = regexp.MustCompile(`^\d{13}$`)

func latestFixture(t *testing.T) string {
	t.Helper()
	dir := "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var latest string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "operators-") && strings.HasSuffix(name, ".csv") {
			if name > latest {
				latest = name
			}
		}
	}
	if latest == "" {
		t.Fatal("no operators-YYYY-MM-DD.csv fixture found")
	}
	return filepath.Join(dir, latest)
}

func TestMegafranchiseeFixture_structure(t *testing.T) {
	path := latestFixture(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv read: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("fixture must have header + >= 1 row, got %d", len(rows))
	}

	header := rows[0]
	want := []string{
		"operator_name",
		"total_stores",
		"brand_count",
		"brands_breakdown",
		"corporate_number",
		"operator_types",
		"discovered_vias",
	}
	for i, h := range want {
		if i >= len(header) || header[i] != h {
			t.Fatalf("col %d: expected %q, got %q (full=%v)", i, h, header[i], header)
		}
	}

	var prevTotal = 1 << 30
	for idx, row := range rows[1:] {
		lineNo := idx + 2 // 1-based + header
		if len(row) != len(want) {
			t.Errorf("line %d: column count %d, want %d", lineNo, len(row), len(want))
			continue
		}
		name := row[0]
		if name == "" {
			t.Errorf("line %d: operator_name empty", lineNo)
		}
		total, err := strconv.Atoi(row[1])
		if err != nil || total < 1 {
			t.Errorf("line %d: invalid total_stores %q", lineNo, row[1])
			continue
		}
		bc, err := strconv.Atoi(row[2])
		if err != nil || bc < 1 {
			t.Errorf("line %d: invalid brand_count %q", lineNo, row[2])
		}
		breakdown := row[3]
		sum := sumBreakdown(breakdown)
		if sum > total {
			t.Errorf("line %d (%s): breakdown sum %d > total %d", lineNo, name, sum, total)
		}
		corp := row[4]
		if corp != "" && !corporateNumberRe.MatchString(corp) {
			t.Errorf("line %d (%s): corporate_number %q must be 13 digits",
				lineNo, name, corp)
		}
		if total > prevTotal {
			t.Errorf("line %d: total_stores %d > previous %d (sort-by total 降順が崩れている)",
				lineNo, total, prevTotal)
		}
		prevTotal = total
	}

	fmt.Printf("✅ megafranchisee fixture %s validated: %d operators\n",
		filepath.Base(path), len(rows)-1)
}

// sumBreakdown は "brand1:N; brand2:M" 形式から N+M を抽出。
func sumBreakdown(s string) int {
	total := 0
	for _, part := range strings.Split(s, ";") {
		p := strings.TrimSpace(part)
		colon := strings.LastIndex(p, ":")
		if colon < 0 {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(p[colon+1:]))
		if err != nil {
			continue
		}
		total += n
	}
	return total
}

func TestMegafranchiseeFixture_noDuplicateCorporateNumbers(t *testing.T) {
	path := latestFixture(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, _ := r.ReadAll()
	seen := map[string]string{}
	for _, row := range rows[1:] {
		corp := row[4]
		if corp == "" {
			continue
		}
		if prev, ok := seen[corp]; ok {
			t.Errorf("duplicate corporate_number %s: %q and %q", corp, prev, row[0])
		}
		seen[corp] = row[0]
	}
}
