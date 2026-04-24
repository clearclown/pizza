// cmd/validate-judge は judge.yaml の制約に対して LLM 出力を検証する CI ツール。
//
// 使い方:
//
//	go run ./cmd/validate-judge --config prompts/judge.yaml --output output.json
//	go run ./cmd/validate-judge --config prompts/judge.yaml --golden test/fixtures/judgement-golden.csv
//	go run ./cmd/validate-judge --config prompts/judge.yaml --golden test/fixtures/judgement-golden.csv --skip-url-resolve
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// JudgeConfig は prompts/judge.yaml の構造体。
type JudgeConfig struct {
	Rules struct {
		Evidence struct {
			URLMustResolve bool     `yaml:"url_must_resolve"`
			SkipDomains    []string `yaml:"skip_domains"`
		} `yaml:"evidence"`
		HallucinationGuards struct {
			RefusePhrases []string `yaml:"refuse_phrases"`
		} `yaml:"hallucination_guards"`
		SourcePriority []any `yaml:"source_priority"`
	} `yaml:"rules"`
}

// SourcePriorityNames は source_priority から許可ソース名一覧を返す。
// エントリは string または map[string]any (gmaps_cluster 等) の混在を処理する。
func (c JudgeConfig) SourcePriorityNames() map[string]bool {
	out := make(map[string]bool)
	for _, entry := range c.Rules.SourcePriority {
		switch v := entry.(type) {
		case string:
			out[v] = true
		case map[string]any:
			for name := range v {
				out[name] = true
			}
		}
	}
	return out
}

// Output は LLM の judge 出力構造体。
type Output struct {
	SourceURL                string  `json:"source_url"`
	CorporateNameFromSource  string  `json:"corporate_name_from_source"`
	StoreCountClaim          int     `json:"store_count_claim"`
	StoreCountSourceText     string  `json:"store_count_source_text"`
	CountSource              string  `json:"count_source"`
	CountUnit                string  `json:"count_unit"`
	Confidence               float64 `json:"confidence"`
	IsMegaFranchisee         bool    `json:"is_mega_franchisee"`
	Reject                   bool    `json:"-"` // 内部フラグ
	RejectReason             string  `json:"-"`
	RequiresL3Verification   bool    `json:"-"`
}

// ValidationRule は単一の検証ルール。
type ValidationRule struct {
	Name   string
	Check  func(Output) bool
	Action func(*Output)
}

// buildRules は judge.yaml の constraints に対応するルール一覧を返す。
func buildRules(cfg JudgeConfig, skipURLResolve bool) []ValidationRule {
	return []ValidationRule{
		{
			Name: "source_url_empty",
			Check: func(o Output) bool {
				return o.SourceURL == "" || o.SourceURL == "null"
			},
			Action: func(o *Output) {
				o.Confidence = 0.0
				o.Reject = true
				o.RejectReason = "source_url_empty"
			},
		},
		{
			Name: "source_text_invalid",
			Check: func(o Output) bool {
				l := len([]rune(o.StoreCountSourceText))
				return l == 0 || l > 50
			},
			Action: func(o *Output) {
				o.Confidence = 0.0
				o.Reject = true
				o.RejectReason = "source_text_invalid"
			},
		},
		{
			Name: "corporate_name_missing",
			Check: func(o Output) bool {
				return o.CorporateNameFromSource == ""
			},
			Action: func(o *Output) {
				o.Reject = true
				o.RejectReason = "corporate_name_missing"
			},
		},
		{
			Name: "source_url_resolves",
			Check: func(o Output) bool {
				if skipURLResolve || o.SourceURL == "" {
					return false // チェックしない
				}
				// skip_domains: API エンドポイントは HEAD チェック不要
				for _, d := range cfg.Rules.Evidence.SkipDomains {
					if strings.Contains(o.SourceURL, d) {
						return false
					}
				}
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Head(o.SourceURL)
				if err != nil || resp.StatusCode >= 400 {
					return true // 失敗 → flag
				}
				return false
			},
			Action: func(o *Output) {
				// on_failure: "flag" → HumanReview に回す（reject はしない）
				o.RequiresL3Verification = true
				o.RejectReason = "url_unresolvable"
			},
		},
		{
			Name: "refuse_phrases_present",
			Check: func(o Output) bool {
				text := o.SourceURL + o.CorporateNameFromSource + o.StoreCountSourceText
				for _, phrase := range cfg.Rules.HallucinationGuards.RefusePhrases {
					if strings.Contains(text, phrase) {
						return true
					}
				}
				return false
			},
			Action: func(o *Output) {
				o.Confidence *= 0.5
				o.RejectReason = "refuse_phrase_found"
			},
		},
		{
			Name: "gmaps_requires_l3",
			Check: func(o Output) bool {
				return o.CountSource == "gmaps_cluster"
			},
			Action: func(o *Output) {
				o.Confidence *= 0.8
				o.RequiresL3Verification = true
			},
		},
		{
			// unknown_source: "error" — source_priority 外のソースはサイレント通過禁止
			// 許可リストは judge.yaml の source_priority から動的に取得（ハードコードなし）。
			Name: "unknown_source_rejected",
			Check: func(o Output) bool {
				if o.CountSource == "" {
					return false // 未指定は別ルールで処理
				}
				return !cfg.SourcePriorityNames()[o.CountSource]
			},
			Action: func(o *Output) {
				o.Reject = true
				o.RejectReason = fmt.Sprintf("unknown_source: %q not in source_priority", o.CountSource)
			},
		},
	}
}

// applyRules は全ルールを順番に適用する。
func applyRules(output *Output, rules []ValidationRule) []string {
	var violations []string
	for _, rule := range rules {
		if rule.Check(*output) {
			rule.Action(output)
			if output.RejectReason != "" {
				violations = append(violations, fmt.Sprintf("%s: %s", rule.Name, output.RejectReason))
			}
		}
	}
	return violations
}

// GoldenRow は judgement-golden.csv の1行。
type GoldenRow struct {
	ID                        string
	CaseType                  string
	Description               string
	SourceURL                 string
	SourceHasClaim            string // "true" | "false" | "partial"
	StoreCountClaim           string
	StoreCountSourceText      string
	ExpectedConfidenceMin     float64
	ExpectedConfidenceMax     float64
	ExpectedIsMega            bool
	ExpectedReject            bool
	ExpectedSourceTextContains string
}

func parseGolden(path string) ([]GoldenRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	var rows []GoldenRow
	for i, rec := range records {
		if i == 0 {
			continue // ヘッダースキップ
		}
		if len(rec) < 12 {
			continue
		}
		// CSV列順: id,case_type,description,source_url,corporate_name_from_source,
		//   source_has_claim,store_count_claim,store_count_source_text,
		//   expected_confidence_min,expected_confidence_max,expected_is_mega,expected_reject,expected_source_text_contains
		if len(rec) < 13 {
			continue
		}
		cMin, _ := strconv.ParseFloat(rec[8], 64)
		cMax, _ := strconv.ParseFloat(rec[9], 64)
		isMega := rec[10] == "true"
		reject := rec[11] == "true"
		rows = append(rows, GoldenRow{
			ID: rec[0], CaseType: rec[1], Description: rec[2],
			SourceURL: rec[3], SourceHasClaim: rec[5],
			StoreCountClaim: rec[6], StoreCountSourceText: rec[7],
			ExpectedConfidenceMin: cMin, ExpectedConfidenceMax: cMax,
			ExpectedIsMega: isMega, ExpectedReject: reject,
			ExpectedSourceTextContains: rec[12],
		})
	}
	return rows, nil
}

func main() {
	configPath := flag.String("config", "prompts/judge.yaml", "Path to judge.yaml")
	outputPath := flag.String("output", "", "Path to judge output JSON (single run)")
	goldenPath := flag.String("golden", "", "Path to golden CSV (batch test)")
	skipURLResolve := flag.Bool("skip-url-resolve", false, "Skip url_must_resolve checks (for CI with fixture URLs)")
	flag.Parse()

	// judge.yaml 読み込み
	cfgData, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot read %s: %v\n", *configPath, err)
		os.Exit(1)
	}
	var cfg JudgeConfig
	if err := yaml.Unmarshal(cfgData, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot parse judge.yaml: %v\n", err)
		os.Exit(1)
	}

	rules := buildRules(cfg, *skipURLResolve)

	// シングル出力モード
	if *outputPath != "" {
		data, err := os.ReadFile(*outputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		var output Output
		if err := json.Unmarshal(data, &output); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: cannot parse output JSON: %v\n", err)
			os.Exit(1)
		}
		violations := applyRules(&output, rules)
		if len(violations) > 0 {
			fmt.Println("FAIL:")
			for _, v := range violations {
				fmt.Println(" -", v)
			}
			os.Exit(1)
		}
		fmt.Println("PASS")
		return
	}

	// golden.csv バッチモード
	if *goldenPath != "" {
		rows, err := parseGolden(*goldenPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		failed := 0
		for _, row := range rows {
			if strings.HasPrefix(row.SourceURL, "TODO") {
				fmt.Printf("SKIP #%s (%s): TODO_REAL_URL\n", row.ID, row.Description)
				continue
			}
			fmt.Printf("CHECK #%s (%s)... ", row.ID, row.Description)
			// golden.csv の各行を Output として構築してルール適用
			cnt, _ := strconv.Atoi(row.StoreCountClaim)
			output := Output{
				SourceURL:               row.SourceURL,
				CorporateNameFromSource: "テスト社名",
				StoreCountClaim:         cnt,
				StoreCountSourceText:    row.StoreCountSourceText,
				Confidence:              1.0,
			}
			applyRules(&output, rules)

			ok := true
			if row.ExpectedReject && !output.Reject {
				fmt.Printf("FAIL (expected reject but not rejected)\n")
				ok = false
			}
			if !row.ExpectedReject && output.Reject {
				fmt.Printf("FAIL (unexpected reject: %s)\n", output.RejectReason)
				ok = false
			}
			// source_has_claim 別追加検証
			switch row.SourceHasClaim {
			case "true":
				// 主張有り → source_text が決して空にならないはず
				if output.StoreCountSourceText == "" && !row.ExpectedReject {
					fmt.Printf("FAIL (source_has_claim=true but store_count_source_text is empty)\n")
					ok = false
				}
			case "false":
				// 主張なし → reject 指定行は confidence > 0.0 なら FAIL
				if row.ExpectedReject && output.Confidence > 0.0 && !output.Reject {
					fmt.Printf("FAIL (source_has_claim=false + expected_reject but confidence=%.2f)\n", output.Confidence)
					ok = false
				}
			case "partial":
				// 曖昧表現 → confidence < 0.5 を期待
				if output.Confidence >= 0.5 && !output.Reject {
					fmt.Printf("FAIL (source_has_claim=partial but confidence=%.2f >= 0.5)\n", output.Confidence)
					ok = false
				}
			}
			if ok {
				fmt.Println("PASS")
			} else {
				failed++
			}
		}
		if failed > 0 {
			fmt.Printf("\n%d test(s) failed\n", failed)
			os.Exit(1)
		}
		fmt.Printf("\nAll tests passed\n")
		return
	}

	flag.Usage()
	os.Exit(1)
}
