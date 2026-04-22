package fixtures_test

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/clearclown/pizza/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func goldenPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "judgement-golden.csv")
}

func TestLoadGolden_parsesAllRows(t *testing.T) {
	t.Parallel()
	rows, err := fixtures.LoadGolden(goldenPath(t))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(rows), 30, "expected at least 30 golden rows")
}

func TestValidate_goldenFileIsClean(t *testing.T) {
	t.Parallel()
	rows, err := fixtures.LoadGolden(goldenPath(t))
	require.NoError(t, err)

	issues, err := fixtures.Validate(rows)
	require.NoError(t, err, "validation must not fail with fatal error")

	if len(issues) > 0 {
		var lines []string
		for _, i := range issues {
			lines = append(lines, i.String())
		}
		t.Fatalf("golden validation found %d issues:\n  %s",
			len(issues), strings.Join(lines, "\n  "))
	}
}

func TestValidate_detectsDuplicatePlaceID(t *testing.T) {
	t.Parallel()
	rows := []fixtures.GoldenRow{
		{PlaceID: "p1", TrueOperationType: "direct", TrueFranchisorName: "株式会社A", Brand: "B", Name: "N", LineNo: 2},
		{PlaceID: "p1", TrueOperationType: "direct", TrueFranchisorName: "株式会社A", Brand: "B", Name: "N", LineNo: 3},
	}
	_, err := fixtures.Validate(rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate place_id")
}

func TestValidate_detectsInvalidOperationType(t *testing.T) {
	t.Parallel()
	rows := []fixtures.GoldenRow{
		{PlaceID: "p1", TrueOperationType: "xxx", Brand: "B", Name: "N", LineNo: 2},
	}
	issues, err := fixtures.Validate(rows)
	require.NoError(t, err)
	require.NotEmpty(t, issues)
	found := false
	for _, i := range issues {
		if i.Field == "true_operation_type" && strings.Contains(i.Message, "invalid value") {
			found = true
		}
	}
	assert.True(t, found, "should flag invalid operation_type, got: %v", issues)
}

func TestValidate_directShouldNotHaveFranchisee(t *testing.T) {
	t.Parallel()
	rows := []fixtures.GoldenRow{
		{
			PlaceID:            "p-direct-with-franchisee",
			Brand:              "Starbucks",
			Name:               "S",
			TrueOperationType:  "direct",
			TrueFranchisorName: "スタバ JP",
			TrueFranchiseeName: "株式会社Illegal", // これが問題
			LineNo:             2,
		},
	}
	issues, _ := fixtures.Validate(rows)
	found := false
	for _, i := range issues {
		if i.Field == "true_franchisee_name" && strings.Contains(i.Message, "unexpected for operation_type=direct") {
			found = true
		}
	}
	assert.True(t, found, "direct 行に franchisee があったら flag するべき")
}

func TestValidate_franchiseeShouldHaveFranchisor(t *testing.T) {
	t.Parallel()
	rows := []fixtures.GoldenRow{
		{PlaceID: "p-fc-no-franchisor", Brand: "B", Name: "N", TrueOperationType: "franchisee", LineNo: 2, TrueIsFranchise: true},
	}
	issues, _ := fixtures.Validate(rows)
	found := false
	for _, i := range issues {
		if i.Field == "true_franchisor_name" && strings.Contains(i.Message, "empty for operation_type=franchisee") {
			found = true
		}
	}
	assert.True(t, found, "franchisee 行に franchisor が空なら flag するべき")
}

func TestValidate_isFranchiseMustMatchOperationType(t *testing.T) {
	t.Parallel()
	rows := []fixtures.GoldenRow{
		{
			PlaceID:            "p-mismatch",
			Brand:              "B",
			Name:               "N",
			TrueOperationType:  "direct",
			TrueFranchisorName: "株式会社X",
			TrueIsFranchise:    true, // direct なのに true
			LineNo:             2,
		},
	}
	issues, _ := fixtures.Validate(rows)
	found := false
	for _, i := range issues {
		if i.Field == "true_is_franchise" && strings.Contains(i.Message, "mismatches") {
			found = true
		}
	}
	assert.True(t, found, "operation_type と is_franchise が矛盾したら flag するべき")
}

func TestValidate_flagsLegacyCompanySuffix(t *testing.T) {
	t.Parallel()
	rows := []fixtures.GoldenRow{
		{
			PlaceID:            "p",
			Brand:              "B",
			Name:               "N",
			TrueOperationType:  "direct",
			TrueFranchisorName: "(株)Legacy", // "株式会社" に統一すべき
			LineNo:             2,
		},
	}
	issues, _ := fixtures.Validate(rows)
	found := false
	for _, i := range issues {
		if i.Field == "company_name_style" {
			found = true
		}
	}
	assert.True(t, found, "(株) などレガシー表記を flag するべき")
}
