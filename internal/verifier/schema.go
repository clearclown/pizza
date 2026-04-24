// Package verifier の型定義・定数。
// 設計責務: verifier は「社名→法人番号の名寄せのみ」。集約・メガジー判定は下流に委譲。
//
// Out of scope (handled by other layers):
//   - Store count / clustering → internal/kitchen
//   - StoreCountByBrand aggregation → Box(SQLite) VIEW
//   - IsCentral / IsBrandHQ judgment → internal/kitchen
//   - Mega-franchisee judgment → post-Aggregate() business logic
//   - browser-use crawling → services/delivery
package verifier

// MatchLevel 定数（VerifyResult.MatchLevel に使用）
const (
	MatchExact     = "exact"      // 完全一致（bi-gram + name完全一致）
	MatchPartial   = "partial"    // 部分一致（prefix または ambiguous 解決済み）
	MatchAmbiguous = "ambiguous"  // 複数候補あり、未解決
	MatchNotFound  = "not_found"  // 見つからなかった（API/DBは正常）
	MatchAPIError  = "api_error"  // DB/APIエラー（NTAVerified=false で通過）
)

// Candidate は ambiguous 時の候補法人情報。
// ResolveAmbiguous() が住所・フリガナ・スコアで選別する。
type Candidate struct {
	// CorporateNumber は13桁の法人番号。
	CorporateNumber string
	// Name は正式社名。
	Name string
	// Furigana は商号フリガナ。
	// TODO(phase2): 国税庁CSVのCOL_FURIGANA=7を houjin_csv.py で取り込んだら有効化。
	// TODO(phase2): check if houjin_csv.py imports furigana column (COL_FURIGANA=7).
	// TODO(phase2): if available, add furigana to SearchByName query and Candidate struct.
	// 現在は空文字（CSVには存在するが未インポート）。
	Furigana string
	// Prefecture は都道府県名。
	Prefecture string
	// City は市区町村名。
	City string
	// Score は bi-gram Jaccard 類似度 [0.0, 1.0]。
	Score float64
	// FuriganaMatch はフリガナ一致フラグ。
	// TODO(phase2): Furigana 取り込み後に AdjustedScore() で使用。
	FuriganaMatch bool
}
