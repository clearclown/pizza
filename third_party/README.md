# third_party/

外部 OSS のフォーク先を git subtree として取り込むディレクトリ。

現時点では **取り込みなし**（Phase 0 では公式パッケージ依存で十分）。

改造が必要になった時点で、[docs/fork-strategy.md](../docs/fork-strategy.md) の手順に従い、以下のプレフィクス名で追加してください:

- `third_party/google-maps-scraper/`
- `third_party/browser-use/`
- ~~`third_party/firecrawl/`~~ — AGPL のため独立コンテナ運用。フォーク・取り込みしない。

## 改造ログ

各 subtree への独自変更は、取り込み後にこのファイルへ日付付きで記録してください。

例:

```markdown
## third_party/google-maps-scraper
- 2026-05-01: `scraper.NearbySearch` に `radius` パラメータ追加（upstream PR #123）
- 2026-05-10: upstream main を subtree pull（コミット 7a3b9c1）
```

(まだ記録なし)
