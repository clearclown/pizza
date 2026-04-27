// Package fixtures — classification golden dataset の内部整合性バリデータ。
//
// docs/operator-definition.md の契約に基づき、以下を自動検証する:
//  - 必須列の存在
//  - true_operation_type の 4 値制約
//  - direct 行に franchisee_name が入っていないこと
//  - franchisee/mixed 行に franchisor_name が入っていること
//  - place_id 重複なし
//  - true_is_franchise / true_operator が true_operation_type と整合
//
// これを CI で走らせることで、golden 品質を常に担保する。
package fixtures

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// ValidOperationTypes は docs/operator-definition.md の 4 値 (+ unknown)。
var ValidOperationTypes = map[string]bool{
	"direct":     true,
	"franchisee": true,
	"mixed":      true,
	"unknown":    true,
}

// GoldenRow は classification-golden.csv の 1 行を表す。
type GoldenRow struct {
	PlaceID           string
	Brand             string
	Name              string
	OfficialURL       string
	TrueOperationType string
	TrueFranchisorName string
	TrueFranchiseeName string
	TrueIsFranchise   bool
	TrueOperator      string
	Notes             string
	LineNo            int
}

// ValidationIssue は見つかった問題 1 件。
type ValidationIssue struct {
	LineNo  int
	PlaceID string
	Field   string
	Message string
}

func (i ValidationIssue) String() string {
	return fmt.Sprintf("line %d [%s] %s: %s", i.LineNo, i.PlaceID, i.Field, i.Message)
}

// LoadGolden は CSV ファイルから GoldenRow を読み取る。
func LoadGolden(path string) ([]GoldenRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // 可変 OK、バリデーション側でチェック
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	// 必須列のインデックス
	required := []string{
		"place_id", "brand", "name", "official_url",
		"true_operation_type", "true_franchisor_name", "true_franchisee_name",
		"true_is_franchise", "true_operator", "notes",
	}
	idx := make(map[string]int)
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	for _, col := range required {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("missing required column: %s", col)
		}
	}

	var rows []GoldenRow
	lineNo := 1
	for {
		lineNo++
		record, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		if len(record) < len(header) {
			// 可変長 OK: 末尾の notes を空扱い
			for len(record) < len(header) {
				record = append(record, "")
			}
		}
		isFC, _ := strconv.ParseBool(strings.TrimSpace(record[idx["true_is_franchise"]]))
		rows = append(rows, GoldenRow{
			PlaceID:            strings.TrimSpace(record[idx["place_id"]]),
			Brand:              strings.TrimSpace(record[idx["brand"]]),
			Name:               strings.TrimSpace(record[idx["name"]]),
			OfficialURL:        strings.TrimSpace(record[idx["official_url"]]),
			TrueOperationType:  strings.TrimSpace(record[idx["true_operation_type"]]),
			TrueFranchisorName: strings.TrimSpace(record[idx["true_franchisor_name"]]),
			TrueFranchiseeName: strings.TrimSpace(record[idx["true_franchisee_name"]]),
			TrueIsFranchise:    isFC,
			TrueOperator:       strings.TrimSpace(record[idx["true_operator"]]),
			Notes:              record[idx["notes"]],
			LineNo:             lineNo,
		})
	}
	return rows, nil
}

// Validate は rows に対して整合性チェックをかけ、見つかった Issue を返す。
// 致命的エラー (duplicate place_id) は error として返し、警告レベルは []Issue。
func Validate(rows []GoldenRow) (issues []ValidationIssue, err error) {
	seen := make(map[string]int)

	for _, r := range rows {
		// Fatal: place_id 重複
		if prev, dup := seen[r.PlaceID]; dup {
			return issues, fmt.Errorf("duplicate place_id %q at line %d (first seen line %d)",
				r.PlaceID, r.LineNo, prev)
		}
		seen[r.PlaceID] = r.LineNo

		// place_id 空
		if r.PlaceID == "" {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "place_id", "must not be empty"})
		}

		// operation_type 値制約
		ot := r.TrueOperationType
		if ot == "" {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "true_operation_type",
				"empty (must be one of direct|franchisee|mixed|unknown)"})
		} else if !ValidOperationTypes[ot] {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "true_operation_type",
				fmt.Sprintf("invalid value %q (want direct|franchisee|mixed|unknown)", ot)})
		}

		// direct 行は franchisee_name が空であるべき
		if ot == "direct" && r.TrueFranchiseeName != "" {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "true_franchisee_name",
				fmt.Sprintf("unexpected for operation_type=direct: %q", r.TrueFranchiseeName)})
		}

		// franchisee or mixed は franchisor_name が入っているはず (本部は常に存在する)
		if (ot == "franchisee" || ot == "mixed") && r.TrueFranchisorName == "" {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "true_franchisor_name",
				"empty for operation_type=" + ot + " (franchisor should be known)"})
		}

		// 後方互換整合: true_is_franchise と operation_type
		expectedIsFC := ot != "direct" && ot != ""
		if r.TrueIsFranchise != expectedIsFC && ot != "" {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "true_is_franchise",
				fmt.Sprintf("mismatches operation_type=%s (want is_franchise=%v, got %v)",
					ot, expectedIsFC, r.TrueIsFranchise)})
		}

		// 表記揺れ軽いチェック: 会社名に「(株)」と「株式会社」混在
		for _, name := range []string{r.TrueFranchisorName, r.TrueFranchiseeName} {
			if strings.Contains(name, "(株)") || strings.Contains(name, "㈱") {
				issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "company_name_style",
					fmt.Sprintf("use 株式会社 (not (株)/㈱): %q", name)})
			}
		}

		// Brand / Name / URL 基本チェック
		if r.Brand == "" {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "brand", "empty"})
		}
		if r.Name == "" {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "name", "empty"})
		}
		if r.OfficialURL != "" && !strings.HasPrefix(r.OfficialURL, "http") {
			issues = append(issues, ValidationIssue{r.LineNo, r.PlaceID, "official_url",
				fmt.Sprintf("must start with http(s)://: %q", r.OfficialURL)})
		}
	}

	return issues, nil
}
