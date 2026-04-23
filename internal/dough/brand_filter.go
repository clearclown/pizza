package dough

import (
	_ "embed"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// 本ファイルは「Places Text Search の fuzzy match でブランド混入を起こす問題」
// に対する 多層フィルタ を実装する。
//
//   Layer A1: 明示的 conflict blocklist — 既知の誤合致ブランドを決定論で除外
//   Layer A2: 正規化 substring / prefix
//   Layer A3: bi-gram Jaccard / 編集距離 (低信頼フォールバック用の数値スコア)
//
// LLM に問い合わせる前にここで弾くことで、後段 (Research Pipeline) に
// 別ブランドの店舗が混入してメガフランチャイジー集計を汚すのを防ぐ。

// ─── ナレッジベース (knowledge/brand_conflicts.yaml) ──────────────────

//go:embed knowledge/brand_conflicts.yaml
var brandConflictsYAML []byte

// BrandConflictEntry はナレッジベース 1 エントリのメタ情報を保持する。
// 運用者が「誰がいつどのエリアで発見したか」を追跡できるようにする。
type BrandConflictEntry struct {
	Pattern      string `yaml:"pattern"`
	FirstSighted string `yaml:"first_sighted"`
	Example      string `yaml:"example"`
	Area         string `yaml:"area"`
	Note         string `yaml:"note"`
}

type brandConflictsFile struct {
	Version   int    `yaml:"version"`
	UpdatedAt string `yaml:"updated_at"`
	Brands    map[string]struct {
		Allow     []string             `yaml:"allow"`
		Conflicts []BrandConflictEntry `yaml:"conflicts"`
	} `yaml:"brands"`
}

// KnownBrandConflicts は「ブランド → 正規化済み conflict pattern 配列」。
// 互換のため従来どおり map[string][]string で公開する。
//
// メタ情報 (first_sighted 等) が必要なケースでは BrandConflictEntriesOf() を使う。
var KnownBrandConflicts map[string][]string

// brandConflictEntries は blocklist 本体 (メタ付き)。
var brandConflictEntries map[string][]BrandConflictEntry

func init() {
	m, entries, err := loadBrandConflicts(brandConflictsYAML)
	if err != nil {
		// embed の parse 失敗はビルド品質の問題なので panic で落とす。
		panic(fmt.Errorf("dough: failed to parse embedded brand_conflicts.yaml: %w", err))
	}
	KnownBrandConflicts = m
	brandConflictEntries = entries
}

func loadBrandConflicts(raw []byte) (map[string][]string, map[string][]BrandConflictEntry, error) {
	var file brandConflictsFile
	if err := yaml.Unmarshal(raw, &file); err != nil {
		return nil, nil, err
	}
	patterns := make(map[string][]string, len(file.Brands))
	entries := make(map[string][]BrandConflictEntry, len(file.Brands))
	for brand, spec := range file.Brands {
		if len(spec.Conflicts) == 0 {
			continue
		}
		ps := make([]string, 0, len(spec.Conflicts))
		for _, c := range spec.Conflicts {
			if strings.TrimSpace(c.Pattern) == "" {
				continue
			}
			ps = append(ps, c.Pattern)
		}
		patterns[brand] = ps
		entries[brand] = spec.Conflicts
	}
	return patterns, entries, nil
}

// BrandConflictEntriesOf は brand の conflict エントリ (メタ含む) を返す。
// ナレッジベースの監査 / レポート用。
func BrandConflictEntriesOf(brand string) []BrandConflictEntry {
	return brandConflictEntries[brand]
}

// ─── Phase 18: URL ドメイン二次 filter ───────────────────────────────

// BrandForeignDomains はブランドごとの「混入が起きやすい外部ドメイン」。
// 店舗の official_url がこのドメインで終わる場合、displayName の brand が
// 正しくても reject する (Places 側の登録誤り対策)。
//
// 例: Mos バーガー店として Places 検出された店舗が official_url =
//     burgerking.co.jp になっている 6 件の誤登録データ発見を契機に導入。
var BrandForeignDomains = map[string][]string{
	"モスバーガー": {
		"burgerking.co.jp",
		"mcdonalds.co.jp",
		"kfc.co.jp",
		"lotteria.jp",
	},
	"マクドナルド": {
		"mos.co.jp",
		"mos.jp",
		"burgerking.co.jp",
		"kfc.co.jp",
		"lotteria.jp",
	},
	"エニタイムフィットネス": {
		"fitplace.jp",
		"chocozap.jp",
		"joyfit.jp",
	},
	"スターバックス コーヒー": {
		"doutor.co.jp",
		"tullys.co.jp",
	},
	"セブン-イレブン": {
		"family.co.jp",
		"lawson.co.jp",
	},
	"ファミリーマート": {
		"sej.co.jp",
		"lawson.co.jp",
	},
	"ローソン": {
		"sej.co.jp",
		"family.co.jp",
	},
}

// hasForeignDomain は brand の official_url が別ブランドのドメインかを判定。
// URL が空 or 判定不能なら false (判定保留)。
func hasForeignDomain(brand, officialURL string) bool {
	if officialURL == "" {
		return false
	}
	domains := BrandForeignDomains[brand]
	if len(domains) == 0 {
		return false
	}
	url := strings.ToLower(officialURL)
	for _, d := range domains {
		if strings.Contains(url, strings.ToLower(d)) {
			return true
		}
	}
	return false
}

// isKnownConflict は brand に対して name が blocklist に該当するか判定。
// 正規化後の name に conflict キーワードの正規化形が含まれていれば true。
// この判定は substring ベースであり、正則文字列一致より広めに弾く。
func isKnownConflict(brand, name string) bool {
	list, ok := KnownBrandConflicts[brand]
	if !ok {
		return false
	}
	nname := normalizeForBrandMatch(name)
	if nname == "" {
		return false
	}
	for _, c := range list {
		nc := normalizeForBrandMatch(c)
		if nc == "" {
			continue
		}
		if strings.Contains(nname, nc) {
			return true
		}
	}
	return false
}

// editDistance は Unicode rune ベースの Levenshtein 距離。
// 主に brand との "近いけど違う" 店舗名を機械的にスコアリングするため。
func editDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	n, m := len(ra), len(rb)
	if n == 0 {
		return m
	}
	if m == 0 {
		return n
	}
	prev := make([]int, m+1)
	curr := make([]int, m+1)
	for j := 0; j <= m; j++ {
		prev[j] = j
	}
	for i := 1; i <= n; i++ {
		curr[0] = i
		for j := 1; j <= m; j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			curr[j] = min3(curr[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[m]
}

func min3(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}

// bigrams は文字列から rune 単位の bi-gram 配列を生成。
func bigrams(s string) []string {
	r := []rune(s)
	if len(r) < 2 {
		return nil
	}
	out := make([]string, 0, len(r)-1)
	for i := 0; i < len(r)-1; i++ {
		out = append(out, string(r[i:i+2]))
	}
	return out
}

// bigramJaccard は 2 文字 n-gram の Jaccard 類似度 [0, 1]。
// 完全一致は 1.0、無交差は 0.0。
func bigramJaccard(a, b string) float64 {
	ga, gb := bigrams(a), bigrams(b)
	if len(ga) == 0 || len(gb) == 0 {
		return 0
	}
	setA := make(map[string]bool, len(ga))
	for _, g := range ga {
		setA[g] = true
	}
	inter := 0
	setB := make(map[string]bool, len(gb))
	for _, g := range gb {
		setB[g] = true
	}
	for g := range setA {
		if setB[g] {
			inter++
		}
	}
	union := len(setA) + len(setB) - inter
	if union <= 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

// brandSimilarityScore は brand と name の「類似度指標」を [0, 1] で返す。
// 値が大きいほど同じブランドである可能性が高い。
//   - 正規化後の Jaccard と 編集距離から 1 - distance/maxLen を取って max する
//   - LLM 推論は一切挟まない
func brandSimilarityScore(brand, name string) float64 {
	nb := normalizeForBrandMatch(brand)
	nn := normalizeForBrandMatch(name)
	if nb == "" || nn == "" {
		return 0
	}
	// 短いほうが brand だと仮定して、name に brand が含まれているなら 1.0
	if strings.Contains(nn, nb) {
		return 1.0
	}
	jac := bigramJaccard(nb, nn)
	// edit distance ベースの ratio
	maxLen := len([]rune(nb))
	if len([]rune(nn)) > maxLen {
		maxLen = len([]rune(nn))
	}
	ed := editDistance(nb, nn)
	edRatio := 0.0
	if maxLen > 0 {
		edRatio = 1.0 - float64(ed)/float64(maxLen)
		if edRatio < 0 {
			edRatio = 0
		}
	}
	if jac > edRatio {
		return jac
	}
	return edRatio
}
