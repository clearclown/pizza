# License Compliance — ライセンス境界の設計

## 要旨

PI-ZZA 本体は **MIT License** で公開します。フォーク元 OSS のうち Firecrawl だけが **AGPL-3.0** であり、これを本体にリンクすると MIT との同時配布が難しくなります。本ドキュメントは、**プロセス境界を REST にすることで AGPL の伝播を回避する設計根拠** を示します。

**免責事項**: 本ドキュメントは法的助言ではありません。重大な商用利用前には弁護士にご相談ください。

## ライセンス一覧

| モジュール | フォーク元 | ライセンス | PI-ZZA 本体との関係 |
|---|---|---|---|
| M1 Seed (Go) | gosom/google-maps-scraper | MIT | 直接 import OK |
| M1 補助 (Go) | googlemaps/google-maps-services-go | Apache-2.0 | 直接 import OK |
| M2 Kitchen (TS) | mendableai/firecrawl | **AGPL-3.0** | **REST 越境のみ**、リンク禁止 |
| M3 Delivery (Python) | browser-use/browser-use | MIT | `pip install` OK |
| LLM SDKs | Anthropic / OpenAI / Gemini | 各種（MIT / Apache-2.0） | OK |

## AGPL-3.0 の何が問題か

AGPL は「派生物をネットワーク経由で提供する場合、ソースコード開示義務が伝播する」ライセンスです。**Firecrawl のコードを import またはリンクして 1 バイナリにした場合、PI-ZZA 全体が AGPL になる可能性** があります。

一方、**独立プロセスとして起動し、標準的な REST API を叩くだけ** の利用は、AGPL の「派生物」には該当しない（判例・FSF 見解の通説）と解釈できます。

## 実装上の分離

```
┌───────────────────┐            ┌─────────────────────┐
│ PI-ZZA (MIT)      │            │ Firecrawl (AGPL)    │
│ Go binary         │            │ Separate container   │
│                   │            │ mendableai/firecrawl │
│  HTTP client ─────┼── REST ───▶│  :3002              │
│                   │            │                     │
└───────────────────┘            └─────────────────────┘
        ↑                                  ↑
   リンクしない                 独立 docker コンテナ
   コピーしない                 公式 image を pull
```

### 禁止事項

- ❌ `import "github.com/mendableai/firecrawl/..."` で Go コードに取り込む
- ❌ Firecrawl のコードを fork してコピペで改造
- ❌ 同一コンテナ内に Firecrawl と PI-ZZA バイナリを同梱

### 許可事項

- ✅ Firecrawl の公式 Docker image を `docker compose` で起動
- ✅ HTTP/REST 経由での API 呼び出し（`FIRECRAWL_API_URL=http://firecrawl:3002`）
- ✅ Firecrawl Cloud (SaaS) の API キーで呼び出し（`FIRECRAWL_MODE=saas`）

## ユーザーがセルフホストで AGPL 条項を満たすには

`FIRECRAWL_MODE=docker` を使用して自社ネットワーク内で Firecrawl を動かし、**かつエンドユーザーに Firecrawl のソースコードを提供できる状態** にしておく必要があります。PI-ZZA はこれを自動化しません — ユーザーは Firecrawl の AGPL 条項を各自遵守する責任があります。

`FIRECRAWL_MODE=saas` の場合、ソース提供義務は Mendable 社が負うため、ユーザー側の手間は発生しません。

## 代替 Markdown 変換器

Firecrawl の AGPL を避けたいユーザー向けの代替選択肢（将来の `KITCHEN_PROVIDER` 環境変数で切替予定）:

- [Crawl4AI](https://github.com/unclecode/crawl4ai) — Apache-2.0 ✅
- [Jina Reader](https://r.jina.ai) — SaaS
- 自作 minimal Markdown converter

## 参考文献

- [GNU AGPL-3.0 FAQ](https://www.gnu.org/licenses/agpl-3.0-faq.html)
- [FSF: Aggregation vs. modification](https://www.gnu.org/licenses/gpl-faq.html#MereAggregation)
- [REST API and AGPL (Stack Exchange)](https://softwareengineering.stackexchange.com/questions/387067)
