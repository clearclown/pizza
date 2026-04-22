# Fork Strategy — git subtree による upstream 同期

## 原則

1. **通常依存は `go get` / `pip install` / `npm i` の公式ルート** で取得する。フォーク不要。
2. **改造が必要** になった時点で初めてフォークし、`third_party/` 以下に git subtree として取り込む。
3. **submodule は使わない**（merge の複雑化・浅い clone 問題を避ける）。
4. upstream の更新は**週次**で自動的に取り込み PR を作る。

## 想定フォーク先

| フォーク元 | ローカル prefix | トリガ |
|---|---|---|
| [gosom/google-maps-scraper](https://github.com/gosom/google-maps-scraper) | `third_party/google-maps-scraper/` | Go API 拡張・独自フィールド追加時 |
| [browser-use/browser-use](https://github.com/browser-use/browser-use) | `third_party/browser-use/` | プロンプト/挙動カスタマイズが必要な時 |
| [mendableai/firecrawl](https://github.com/mendableai/firecrawl) | `third_party/firecrawl/` | **原則フォークしない**（AGPL 隔離のため独立コンテナ運用） |

## セットアップ手順

フォーク作成:
```bash
gh repo fork gosom/google-maps-scraper --clone=false
gh repo fork browser-use/browser-use --clone=false
# firecrawl は独立コンテナのまま使うため fork しない
```

subtree として取り込み:
```bash
# Seed (Go)
git subtree add --prefix=third_party/google-maps-scraper \
  https://github.com/clearclown/google-maps-scraper.git main --squash

# Delivery (Python)
git subtree add --prefix=third_party/browser-use \
  https://github.com/clearclown/browser-use.git main --squash
```

## upstream 同期

手動:
```bash
git subtree pull --prefix=third_party/google-maps-scraper \
  https://github.com/gosom/google-maps-scraper.git main --squash

git subtree pull --prefix=third_party/browser-use \
  https://github.com/browser-use/browser-use.git main --squash
```

自動: `.github/workflows/upstream-sync.yml` が週次 cron で回り、差分があれば PR を作成します。

## ローカル改造を upstream に還元

```bash
git subtree push --prefix=third_party/google-maps-scraper \
  https://github.com/clearclown/google-maps-scraper.git feature/my-change
# その後 GitHub で upstream に PR
```

## Go モジュールとの連携

フォークを `third_party/` に置いた場合、`go.mod` に replace を追加:

```go
replace github.com/gosom/google-maps-scraper => ./third_party/google-maps-scraper
```

これで Go 側から `import "github.com/gosom/google-maps-scraper/..."` と書きつつ、ローカルフォークが使われます。

## 改造ログ

各 subtree の改造点は `third_party/README.md` に日付付きで記録してください（diff ではなく **趣旨** を記録する）。

例:
```markdown
## third_party/google-maps-scraper
- 2026-05-01: `scraper.NearbySearch` に `radius` パラメータ追加（PR 未送付）
- 2026-05-10: upstream main を subtree pull（コミット 7a3b9c1）
```
