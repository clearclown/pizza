# Security Policy

## 対応バージョン / Supported Versions

PI-ZZA は現在 Phase 0 (pre-release)。正式リリース後に本表を更新します。

| Version | Supported |
|---------|-----------|
| main (HEAD) | :white_check_mark: |

## 脆弱性の報告 / Reporting a Vulnerability

**公開 issue として報告しないでください。** 代わりに **GitHub Security Advisories** を使用してプライベートに報告してください:

👉 https://github.com/clearclown/pizza/security/advisories/new

報告には以下を含めてください:
1. 脆弱性の種類と影響範囲
2. 再現手順 (PoC 最小化が望ましい)
3. 影響を受けるバージョン / コミット SHA
4. 緩和策または修正案 (あれば)

**Please do not file public issues.** Use GitHub Security Advisories (link above) to report privately. Include vulnerability type, reproduction steps, affected versions, and suggested mitigation.

## 対応 SLA / Response Timeline

- **24 時間以内**: 受領確認
- **7 日以内**: 影響評価と重大度判定
- **30 日以内**: 修正コミットまたは緩和策の公開

深刻な脆弱性は coordinated disclosure の対象とし、公表前に修正リリースを行います。

## スコープ外 / Out of Scope

- フォーク元の upstream OSS の脆弱性 (該当プロジェクトに報告してください)
  - [gosom/google-maps-scraper](https://github.com/gosom/google-maps-scraper/security)
  - [mendableai/firecrawl](https://github.com/mendableai/firecrawl/security)
  - [browser-use/browser-use](https://github.com/browser-use/browser-use/security)
- ユーザーが設定した API キー漏洩（管理はユーザー責任）
- サードパーティ LLM プロバイダの脆弱性
