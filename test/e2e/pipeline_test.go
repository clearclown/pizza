// 🔴 E2E skeleton — 開発工程.md §3.2 E2E Flow 相当。
//
// testcontainers-go で docker compose を起動し、
//   pizza bake --query ブランド名 --area 東京都
// を実行し、CSV が出力されるところまでを検証する。
//
// Phase 0: スケルトンのみ (build tag "integration" で通常 test から除外)。
// Phase 1-4 で段階的に Green 化。
//
//go:build integration

package e2e_test

import (
	"testing"
)

func TestE2E_pipelineProducesCSV_skeleton(t *testing.T) {
	t.Skip("Phase 0: skeleton. Phase 1+ で testcontainers-go で compose を起動し、" +
		"エニタイムフィットネス × 東京都 の実データを使ったパイプライン完走を検証する。")

	// TODO Phase 1+:
	// 1. testcontainers-go compose.yaml を起動
	// 2. pizza bake --query 'エニタイムフィットネス' --area '東京都' を exec
	// 3. var/output/*.csv が生成されていることを assert
	// 4. CSV 中に少なくとも 1 件のメガジー候補が含まれることを assert
}
