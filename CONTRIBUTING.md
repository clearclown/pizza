# Contributing to PI-ZZA 🍕

PI-ZZA への貢献、歓迎します。このプロジェクトは **厳格 TDD** と **Conventional Commits** によって運営されています。

Thank you for contributing to PI-ZZA. This project is governed by strict **TDD** and **Conventional Commits**.

---

## 📜 行動規範

参加前に [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) を必ずお読みください。

---

## 🚀 クイックスタート

```bash
git clone git@github.com:clearclown/pizza.git
cd pizza
make bootstrap    # 依存一式インストール
make proto        # gRPC コード生成
make test         # 全言語テスト実行
```

詳細な開発環境構築は [docs/tdd-workflow.md](./docs/tdd-workflow.md) を参照。

---

## 🧪 TDD の誓い — Red → Green → Refactor

新機能 / バグ修正は、以下の **3 コミット** を **この順序で** 作成してください。

### 1️⃣ Red — 失敗するテストだけをコミット

```bash
git commit -m "test(grid): add failing test for polygon boundary handling"
```

- 期待する振る舞いを表現したテストのみ
- 実装コードは一切含めない
- CI で「テスト失敗」になることを確認

### 2️⃣ Green — テストを通す最小実装

```bash
git commit -m "feat(grid): handle polygon boundary to pass the coverage test"
```

- 美しさより動作優先
- テストが緑になる最小限のコード
- リファクタはまだしない

### 3️⃣ Refactor — 緑を保ったまま整形

```bash
git commit -m "refactor(grid): extract boundary computation helper"
```

- 動作は変えない
- テストは依然として緑
- 命名改善・関数抽出・重複除去など

> **例外**: タイポ修正、ドキュメントのみの変更、依存バージョン更新などは単一コミットで可。ただし PR テンプレに **「TDD 非該当」** と明記すること。

---

## 📝 Conventional Commits

全コミットメッセージは [Conventional Commits v1.0.0](https://www.conventionalcommits.org/ja/v1.0.0/) に従ってください。

### タイプ

| Type | 用途 |
|---|---|
| `feat` | 新機能 |
| `fix` | バグ修正 |
| `test` | テスト追加/修正（Red コミットはほぼこれ） |
| `refactor` | 振る舞いを変えないコード改善 |
| `docs` | ドキュメントのみ |
| `build` | ビルドシステム・依存（proto, Dockerfile, go.mod） |
| `ci` | CI 設定（.github/workflows/） |
| `chore` | その他（スキャフォールド、設定ファイル） |
| `perf` | 性能改善 |

### スコープ例

`grid`, `oven`, `dough`, `toppings`, `courier`, `box`, `scoring`, `proto`, `delivery`, `ci`, `readme`

### 例

```
feat(scoring): add 20-store threshold for mega franchisee detection
test(delivery): add failing pytest for anthropic provider fallback
fix(grid): correct latitude wraparound at poles
docs(architecture): add sequence diagram for end-to-end pipeline
build(proto): bump protobuf bindings for python 3.12
ci(upstream-sync): schedule weekly subtree pull for google-maps-scraper
```

---

## 🔀 プルリクエスト

1. issue を作成または既存 issue に紐付ける（trivial 変更除く）
2. feature ブランチを切る: `git switch -c type/scope-short-description`
3. Red → Green → Refactor の 3 コミット（または単一コミット + TDD 非該当明記）
4. `make test && make lint` がローカルで pass することを確認
5. `.github/PULL_REQUEST_TEMPLATE.md` のチェックリストを全て埋める
6. レビューを受け、指摘を別コミットで反映（squash は最終段階でメンテナが実施）

### ブランチ保護ルール (main)

- CI 全ジョブの pass 必須 (`go test`, `pytest`, `buf lint`, `buf breaking`, `codeql`)
- 1 名以上の approve 必須
- force push 禁止
- linear history (merge commit 禁止、rebase or squash のみ)

---

## 🌐 proto 変更（Breaking Change 禁止）

`api/pizza/v1/` 配下は **v1 API として backward compatible を維持** してください。

```bash
buf lint api              # スタイルチェック
buf breaking api --against '.git#branch=main'  # 互換性チェック
```

破壊的変更が必要な場合は `api/pizza/v2/` を新設してください（v1 を削除しない）。詳細は [docs/proto-versioning.md](./docs/proto-versioning.md)。

---

## 🍴 フォーク元 OSS の改造

`third_party/` 配下は git subtree でフォーク先をミラーしています。改造は:

1. 先に upstream へ PR を送る（可能なら）
2. PI-ZZA 独自が必要ならフォークで maintain し、subtree pull で取り込む
3. 改造点は `third_party/README.md` に記録

詳細は [docs/fork-strategy.md](./docs/fork-strategy.md)。

---

## 📚 言語別ローカル開発

### Go

```bash
go test -race -cover ./...
golangci-lint run
gofmt -w .
```

### Python (uv)

```bash
uv sync
uv run pytest
uv run ruff check
uv run ruff format
```

### Proto

```bash
buf lint api
buf generate api
buf breaking api --against '.git#branch=main'
```

---

## 🎯 Good First Issue

新規コントリビュータは [`good first issue`](https://github.com/clearclown/pizza/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22) ラベルから探してください。

---

ありがとうございます 🍕 Happy baking!
