"""Brand profile 取得用の source-specific scraper 群 (Phase 25)。

各モジュールは **Scrapling で HTML を取得 + 決定論 regex で抽出** する純関数
を提供する。LLM 推論は使わない (ハルシネーション禁止)。

- jfa_detail: JFA 会員個別 detail page から代表者/資本金/設立 等
- official_site: 公式 HP の 会社概要 / IR ページから代表者/住所/売上
- fc_recruitment: 公式 HP の /franchise/ /fc/ /recruit/owner 等を発見
- revenue_extractor: 売上表 (百万円 / 億円) を円単位に正規化して 2 期分抽出
"""
