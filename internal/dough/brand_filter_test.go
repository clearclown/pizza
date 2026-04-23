package dough

import "testing"

func TestMatchesBrand_exactSubstring(t *testing.T) {
	p := &PlaceRaw{DisplayName: DisplayName{Text: "エニタイムフィットネス 新宿6丁目店"}}
	if !matchesBrand(p, "エニタイムフィットネス") {
		t.Error("should match exact substring")
	}
}

func TestMatchesBrand_normalizedMatch(t *testing.T) {
	// スペース違い、中黒違い
	p := &PlaceRaw{DisplayName: DisplayName{Text: "ファミリー マート 渋谷店"}}
	if !matchesBrand(p, "ファミリーマート") {
		t.Error("should match after normalization")
	}
}

func TestMatchesBrand_rejectsDifferentBrand(t *testing.T) {
	cases := []struct {
		name  string
		brand string
	}{
		{"FIT PLACE24 新宿西口店", "エニタイムフィットネス"},
		{"24GYM 西新宿五丁目店", "エニタイムフィットネス"},
		{"WHITEGYM新宿1号店/レンタルジム", "エニタイムフィットネス"},
		{"VITAL GYM24中野新橋店", "エニタイムフィットネス"},
		{"ファストジム24東中野店", "エニタイムフィットネス"},
		{"addict gym 千駄ヶ谷店", "エニタイムフィットネス"},
		{"Amazing fitness", "エニタイムフィットネス"},
		{"WILL BE fitness studio 高田馬場店", "エニタイムフィットネス"},
	}
	for _, tc := range cases {
		p := &PlaceRaw{DisplayName: DisplayName{Text: tc.name}}
		if matchesBrand(p, tc.brand) {
			t.Errorf("brand %q should NOT match place %q", tc.brand, tc.name)
		}
	}
}

func TestMatchesBrand_emptyBrandAllowsAll(t *testing.T) {
	p := &PlaceRaw{DisplayName: DisplayName{Text: "何でも"}}
	if !matchesBrand(p, "") {
		t.Error("empty brand should match anything")
	}
}

func TestMatchesBrand_prefixOfLongName(t *testing.T) {
	// "セブン-イレブン" は長いブランド名。一部の店で短縮形 "セブン" と
	// 表記されても許容する (5 文字以上の prefix)
	p := &PlaceRaw{DisplayName: DisplayName{Text: "セブンイレブン 新宿東口店"}}
	if !matchesBrand(p, "セブン-イレブン") {
		t.Error("normalization should handle hyphen in brand name")
	}
}

func TestMatchesBrand_starbucks(t *testing.T) {
	// 実ケース: スターバックス コーヒー (中黒や空白ゆれ)
	tests := []struct {
		name  string
		brand string
		want  bool
	}{
		{"スターバックス コーヒー 新宿東口店", "スターバックス コーヒー", true},
		{"スターバックスコーヒー 新宿東口店", "スターバックス コーヒー", true},
		{"スターバックス", "スターバックス コーヒー", true}, // prefix 部分一致
		{"ドトールコーヒー 新宿南口店", "スターバックス コーヒー", false},
	}
	for _, tc := range tests {
		p := &PlaceRaw{DisplayName: DisplayName{Text: tc.name}}
		got := matchesBrand(p, tc.brand)
		if got != tc.want {
			t.Errorf("brand=%q name=%q: got=%v want=%v", tc.brand, tc.name, got, tc.want)
		}
	}
}

// ─── Layer A KB: メタデータの整合性 ───────────────────────────────────

func TestBrandConflictsKB_loadsWithMetadata(t *testing.T) {
	// ナレッジベースに最低限期待されるブランドが入っているか
	for _, brand := range []string{
		"エニタイムフィットネス",
		"スターバックス コーヒー",
		"マクドナルド",
	} {
		if _, ok := KnownBrandConflicts[brand]; !ok {
			t.Errorf("KnownBrandConflicts missing brand %q", brand)
		}
		if len(BrandConflictEntriesOf(brand)) == 0 {
			t.Errorf("BrandConflictEntriesOf(%q) returned empty", brand)
		}
	}
	// 実発見ケースはメタ情報必須 (first_sighted + example)
	entries := BrandConflictEntriesOf("エニタイムフィットネス")
	var foundFitPlace bool
	for _, e := range entries {
		if e.Pattern == "fitplace" {
			foundFitPlace = true
			if e.FirstSighted == "" {
				t.Error("fitplace entry must carry first_sighted for traceability")
			}
			if e.Example == "" {
				t.Error("fitplace entry must carry example (生 displayName)")
			}
		}
	}
	if !foundFitPlace {
		t.Error("ナレッジベースに fitplace conflict が存在するはず")
	}
}

// ─── Layer A1: conflict blocklist ────────────────────────────────────

func TestIsKnownConflict_anytime(t *testing.T) {
	conflicts := []string{
		"FIT PLACE24 新宿西口店",
		"24GYM 西新宿五丁目店",
		"WHITEGYM新宿1号店",
		"VITAL GYM24中野新橋店",
		"ファストジム24東中野店",
		"addict gym 千駄ヶ谷店",
		"Amazing fitness",
		"WILL BE fitness studio 高田馬場店",
		"chocoZAP 新宿三丁目店",
	}
	for _, n := range conflicts {
		if !isKnownConflict("エニタイムフィットネス", n) {
			t.Errorf("expected conflict for brand=エニタイムフィットネス name=%q", n)
		}
	}
	// 正しい Anytime 名は conflict ではない
	if isKnownConflict("エニタイムフィットネス", "エニタイムフィットネス 新宿6丁目店") {
		t.Error("Anytime name should not be flagged as conflict")
	}
}

func TestIsKnownConflict_coffee(t *testing.T) {
	cases := []struct {
		brand string
		name  string
		want  bool
	}{
		{"スターバックス コーヒー", "ドトールコーヒー 新宿南口店", true},
		{"スターバックス コーヒー", "タリーズコーヒー 新宿大ガード店", true},
		{"スターバックス コーヒー", "コメダ珈琲店 新宿店", true},
		{"スターバックス コーヒー", "スターバックス コーヒー 新宿東口店", false},
	}
	for _, c := range cases {
		got := isKnownConflict(c.brand, c.name)
		if got != c.want {
			t.Errorf("isKnownConflict(%q, %q) = %v, want %v", c.brand, c.name, got, c.want)
		}
	}
}

func TestIsKnownConflict_unregisteredBrand(t *testing.T) {
	// blocklist に無いブランドは常に false
	if isKnownConflict("未登録ブランド", "全然違うお店の名前") {
		t.Error("unregistered brand should return false")
	}
}

// ─── Layer A3: 編集距離 / bi-gram Jaccard ───────────────────────────

func TestEditDistance(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "abc", 0},
		{"abc", "abd", 1},
		{"abc", "", 3},
		{"", "abc", 3},
		{"kitten", "sitting", 3},
		{"エニタイム", "エニタイ", 1},
		{"エニタイム", "エニタイン", 1},
		{"スターバックス", "スタバックス", 1},
	}
	for _, c := range cases {
		got := editDistance(c.a, c.b)
		if got != c.want {
			t.Errorf("editDistance(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestBigramJaccard(t *testing.T) {
	// 完全一致
	if got := bigramJaccard("abcd", "abcd"); got != 1.0 {
		t.Errorf("identical strings should score 1.0, got %v", got)
	}
	// 無交差
	if got := bigramJaccard("ab", "cd"); got != 0 {
		t.Errorf("no overlap should score 0, got %v", got)
	}
	// 短文字列エッジ
	if got := bigramJaccard("a", "a"); got != 0 {
		t.Errorf("single char has no bigrams, expected 0, got %v", got)
	}
	// 部分重複: "abc" と "bcd" は共通 bi-gram "bc" のみ → 1/3
	got := bigramJaccard("abc", "bcd")
	if got < 0.33 || got > 0.34 {
		t.Errorf("partial overlap expected ~0.333, got %v", got)
	}
}

func TestBrandSimilarityScore(t *testing.T) {
	cases := []struct {
		brand  string
		name   string
		minMax [2]float64 // [min, max] 許容範囲
	}{
		// name に brand 完全包含 → 1.0
		{"エニタイムフィットネス", "エニタイムフィットネス 新宿6丁目店", [2]float64{1.0, 1.0}},
		// 別ブランド → 0.3 未満
		{"エニタイムフィットネス", "FIT PLACE24 新宿西口店", [2]float64{0.0, 0.3}},
		// 同系 (ほぼ同名) 高スコア
		{"スターバックス コーヒー", "スターバックスコーヒー 新宿南口店", [2]float64{0.9, 1.0}},
	}
	for _, c := range cases {
		got := brandSimilarityScore(c.brand, c.name)
		if got < c.minMax[0] || got > c.minMax[1] {
			t.Errorf("brandSimilarityScore(%q, %q) = %v, want in [%v, %v]",
				c.brand, c.name, got, c.minMax[0], c.minMax[1])
		}
	}
}

// ─── KB 非依存: 類似度だけで判定できる層 ────────────────────────────
//
// ユーザー要求「ナレッジベースに影響されすぎないで欲しい」に応えるため、
// KB (blocklist) を空にしても判定が正しく働くことを明示する。

func TestMatchesBrand_similarityOnly_acceptsHighScore(t *testing.T) {
	cases := []struct {
		brand, name string
	}{
		{"エニタイムフィットネス", "エニタイムフィットネス 新宿6丁目店"},
		{"スターバックス コーヒー", "スターバックスコーヒー 新宿南口店"},
		{"スターバックス コーヒー", "スターバックス コーヒー 新宿東口店"},
		{"マクドナルド", "マクドナルド 新宿東口店"},
	}
	for _, tc := range cases {
		p := &PlaceRaw{DisplayName: DisplayName{Text: tc.name}}
		if !matchesBrand(p, tc.brand) {
			t.Errorf("高類似度 (substring/含有) は KB なしでも accept されるはず: brand=%q name=%q",
				tc.brand, tc.name)
		}
	}
}

func TestMatchesBrand_similarityOnly_rejectsLowScore(t *testing.T) {
	// KB blocklist に登録されていない完全無関係な店舗でも、低類似度で reject される
	cases := []struct {
		brand, name string
	}{
		{"エニタイムフィットネス", "セブン-イレブン 渋谷道玄坂店"}, // KB 未登録
		{"エニタイムフィットネス", "吉野家 新宿東口店"},         // KB 未登録
		{"スターバックス コーヒー", "すき家 新宿店"},            // KB 未登録
	}
	for _, tc := range cases {
		p := &PlaceRaw{DisplayName: DisplayName{Text: tc.name}}
		if matchesBrand(p, tc.brand) {
			t.Errorf("低類似度の無関係店舗は KB 未登録でも reject されるはず: brand=%q name=%q",
				tc.brand, tc.name)
		}
	}
}

func TestMatchesBrand_midRangeRequiresKB(t *testing.T) {
	// 中間帯 (類似度 0.2–0.85) だけ KB を参照する設計の確認。
	// FIT PLACE24 は normalize 後 "fitplace24 新宿西口店" で、エニタイムとの
	// bi-gram 類似度は低いが、文字列長が近いため中間帯になる可能性がある。
	// ここでは KB にあるのだから確実に reject される。
	p := &PlaceRaw{DisplayName: DisplayName{Text: "FIT PLACE24 新宿西口店"}}
	if matchesBrand(p, "エニタイムフィットネス") {
		t.Error("KB にある FIT PLACE24 は中間帯で KB によって reject されるはず")
	}
}

// ─── 統合: matchesBrand に blocklist が効くか ─────────────────────────

func TestMatchesBrand_blocklistPriority(t *testing.T) {
	// blocklist と substring が両方該当しそうな名前 →
	// blocklist が優先されて reject
	// "セブンイレブン" を含むローソン店舗名は通常ないが、
	// "エニタイムフィットネス" ← "amazing fitness" の例で確認
	// blocklist なければ substring 正規化で通過しないが、
	// ここでは blocklist によって確定 reject を保証するケース:
	p := &PlaceRaw{DisplayName: DisplayName{Text: "Amazing fitness エニタイム風"}}
	if matchesBrand(p, "エニタイムフィットネス") {
		t.Error("blocklist should take priority and reject Amazing fitness")
	}
}

func TestNormalizeForBrandMatch(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{"エニタイム フィットネス", "エニタイムフィットネス"},
		{"スターバックス・コーヒー", "スターバックスコーヒー"},
		{"セブン-イレブン", "セブンイレブン"},
		{"ABC DEF", "abcdef"},
		{"", ""},
	}
	for _, c := range cases {
		got := normalizeForBrandMatch(c.in)
		if got != c.out {
			t.Errorf("normalize(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}
