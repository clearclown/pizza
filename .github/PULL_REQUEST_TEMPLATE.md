<!--
  🍕 PI-ZZA PR Template
  TDD 規約 (Red → Green → Refactor) と Conventional Commits を遵守してください。
-->

## 概要 / Summary

<!-- 何を・なぜ変更するか、1-3 文で。関連 issue: Fixes #123 -->

## TDD コミット履歴

| Phase | Commit SHA | 説明 |
|-------|-----------|------|
| 🔴 Red (failing test) | `xxxxxxx` | |
| 🟢 Green (minimum impl) | `xxxxxxx` | |
| 🔵 Refactor | `xxxxxxx` | |

> **TDD 非該当の場合**: 以下にチェックして理由を記載
> - [ ] 本 PR は TDD 非該当（タイポ / ドキュメント / 依存更新など）
> - 理由:

## チェックリスト

- [ ] `make test` がローカルで pass する
- [ ] `make lint` がローカルで pass する
- [ ] `buf lint api && buf breaking api --against '.git#branch=main'` が pass する（proto 変更時）
- [ ] 関連ドキュメント (`README.md` / `docs/*.md` / `ARCHITECTURE.md`) を更新した
- [ ] `CHANGELOG.md` の `[Unreleased]` にエントリを追加した（user-visible な変更時）
- [ ] 新規依存を追加した場合、ライセンスを確認した（AGPL コードを Go バイナリにリンクしていない）
- [ ] Commit メッセージが Conventional Commits に従っている

## 破壊的変更 / Breaking Changes

<!-- API / CLI / proto の後方互換を壊す変更がある場合、ここに明記 -->

## テスト

<!-- どのようにテストしたか。再現手順 / 期待結果 -->

## スクリーンショット / ログ（該当する場合）

<!-- UI 変更や CLI 出力の差分 -->

---

🍕 _Thank you for the slice of code!_
