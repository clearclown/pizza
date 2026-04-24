package verifier

import (
	"math"
	"sort"
)

// AddressHint は住所によるスコアリングのヒント情報。
// L2クラスタリングで取得した店舗の住所を渡す（本社住所ではない）。
// 住所は「加点のみ」に使用し、不一致でも候補を落とさない（本社別県問題の対処）。
type AddressHint struct {
	Prefecture string
	City       string
}

// ComputeScore は Candidate の総合スコアを計算する。
// bi-gram Jaccard スコア（c.Score）に住所・フリガナのボーナスを加算する。
//
// スコア設計:
//   - 完全一致: +3.0
//   - フリガナ一致: +2.0（TODO(phase2): Furigana 取り込み後に有効化）
//   - 同県: +1.0（加点のみ、city は加点なし＝本社別県問題の対処）
func (c *Candidate) ComputeScore(query string, hint AddressHint) float64 {
	score := 0.0
	if c.Name == query {
		score += 3.0
	}
	// TODO(phase2): FuriganaMatch が有効になったら以下を有効化
	// if c.FuriganaMatch { score += 2.0 }
	if c.Prefecture == hint.Prefecture {
		score += 1.0
	}
	// city は加点なし（本社所在地 != 店舗所在地のケースが多い）
	return score
}

// AdjustedScore は bi-gram Jaccard + ボーナスの合算スコア。
// ボーナスは 0.1 スケールで加算し、上限 1.0。
func (c *Candidate) AdjustedScore(query string, hint AddressHint) float64 {
	base := c.Score // bi-gram Jaccard（0.0-1.0）
	bonus := c.ComputeScore(query, hint)
	return math.Min(base+bonus*0.1, 1.0)
}

// scoreToMatchLevel は ComputeScore の結果から MatchLevel を判定する。
//
//	score >= 3.0 → exact（完全一致）
//	score >= 1.0 → partial（部分一致）
//	score < 1.0  → ambiguous
func scoreToMatchLevel(score float64) string {
	switch {
	case score >= 3.0:
		return MatchExact
	case score >= 1.0:
		return MatchPartial
	default:
		return MatchAmbiguous
	}
}

const scoreCloseThreshold = 0.1 // この差以下なら "score_too_close"

// ResolveAmbiguous は ambiguous 状態の VerifyResult から最良の候補を選ぶ。
//
// 優先順位:
//  1. Prefecture + City 完全一致
//  2. FuriganaMatch=true（TODO(phase2)）
//  3. AdjustedScore 最大
//  4. スコア差 < 0.1 → 解決不能（HumanReviewRequired=true を呼び元がセット）
//
// 戻り値: (解決した候補, 解決できたか)
func ResolveAmbiguous(result *VerifyResult, query string, hint AddressHint) (*Candidate, bool) {
	if len(result.Candidates) == 0 {
		return nil, false
	}

	// 優先順位 1: Prefecture + City 完全一致
	for i, c := range result.Candidates {
		if c.Prefecture == hint.Prefecture && c.City == hint.City {
			return &result.Candidates[i], true
		}
	}

	// 優先順位 2: FuriganaMatch=true（Phase 2）
	// TODO(phase2): Furigana 取り込み後に有効化
	// for i, c := range result.Candidates {
	// 	if c.FuriganaMatch { return &result.Candidates[i], true }
	// }

	// 優先順位 3: AdjustedScore 降順でソートして最大を選ぶ
	sort.Slice(result.Candidates, func(i, j int) bool {
		return result.Candidates[i].AdjustedScore(query, hint) >
			result.Candidates[j].AdjustedScore(query, hint)
	})

	best := result.Candidates[0]

	// スコア差チェック
	if len(result.Candidates) > 1 {
		diff := best.AdjustedScore(query, hint) -
			result.Candidates[1].AdjustedScore(query, hint)
		if diff < scoreCloseThreshold {
			return nil, false // 解決不能 → HumanReviewRequired=true
		}
	}

	return &result.Candidates[0], true
}
