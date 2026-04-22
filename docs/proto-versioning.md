# Proto Versioning — buf breaking ポリシー

## 原則

1. **v1 は永久に backward compatible に保つ**
2. Breaking change が必要な場合、`api/pizza/v2/` を新設する（v1 を残したまま）
3. CI で `buf breaking` を強制し、main 直 push を防ぐ

## ルール

```yaml
# api/buf.yaml
version: v2
modules:
  - path: api
lint:
  use: [STANDARD]
breaking:
  use: [FILE]   # ファイル単位で破壊的変更を検出
```

## 許可される変更

- 新しい RPC 追加
- 新しい message 追加
- 既存 message への **reserved ではない新フィールド** 追加
- コメント追加・改善
- option 追加

## 禁止される変更

- フィールド削除（代わりに `reserved` へ移動）
- フィールド型変更
- フィールド番号変更
- RPC 削除
- service 名変更
- enum 値削除

## ローカル検証

```bash
buf lint api
buf breaking api --against '.git#branch=main'
```

## CI 強制

`.github/workflows/buf.yml` が PR ごとに lint + breaking を走らせます。

## v2 への移行

将来、破壊的変更が避けられない場合:

1. `api/pizza/v2/` を作成し、新しい契約を定義
2. Go/Python コードは v1 と v2 を並行サポート（移行期間中）
3. v1 → v2 のクライアント移行が完了したら、v1 を段階的に deprecated → 削除

v1 を削除するのは **複数リリースサイクルを跨いだ deprecation 通知後** に限ります。
