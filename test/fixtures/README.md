# test/fixtures/

E2E・精度ベンチマークで使う既知データのスナップショット。

## 構成

```
fixtures/
├── direct-only/              # 全店直営のサンプル
├── single-franchisee/        # 単一加盟 FC サンプル
├── mega-franchisee/          # 20 店舗以上のメガジーサンプル
├── classification-golden.csv # §3.3 Classification Accuracy の人工テストデータ
└── judgement-golden.csv      # judge prompt / メガジー証跡ルールの検証データ
```

## 現状 (Phase 0)

ディレクトリ骨格のみ。Phase 1-3 で実 HTML / Markdown / 判定正解を追加する。

## Golden set の基準 (Phase 3 で作成)

`classification-golden.csv` のスキーマ:

```csv
place_id,brand,name,url,true_is_franchise,true_operator,notes
ChIJ...,エニタイムフィットネス,新宿店,https://...,true,株式会社メガスポーツ,
...
```

100 行を目標 (§3.3)。
