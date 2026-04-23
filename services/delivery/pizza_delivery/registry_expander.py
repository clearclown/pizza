"""Phase 17.4 + Phase 19: Registry 自動拡充 loop / 横断メガジー集計。

unknown_stores (registry 未突合の bottom-up 店舗) に per_store で見つかった
operator_name を集計し、頻度 >= min_stores の社を「registry 追加候補」として
YAML-ready 形式で書き出す (1 ブランド内集計)。

さらに Phase 19 で追加: **brand を跨いだ operator 集計**。1 つの事業会社が
複数ブランドを運営しているケース (例: 大和フーヅ=ミスド48+モス18) を正しく
1 行に集約する。この事業会社主語の view がメガジー (多業態メガフランチャイジー)
発見の正しい粒度。

運用:
  1. pizza scan / audit で unknown_stores.csv が生成される
  2. pizza registry-expand で 1 ブランド候補を抽出
  3. pizza megafranchisee (cross-brand) で事業会社横断リストを抽出
  4. 人間 or agent がレビューして franchisee_registry.yaml に追記
"""

from __future__ import annotations

import csv as csv_mod
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


# ─── Data types ────────────────────────────────────────────────────────


@dataclass
class CandidateFranchisee:
    """Registry 追加候補の 1 entry。"""

    name: str
    brand: str
    estimated_store_count: int
    source: str = "per_store extraction"
    corporate_number: str = ""       # 要手動確認 or gBizINFO lookup
    head_office: str = ""
    first_seen_via: str = ""         # operator_stores.discovered_via


# ─── 集計 ──────────────────────────────────────────────────────────────


def aggregate_unknown_operators(
    *,
    db_path: str,
    brand: str,
    min_stores: int = 2,
) -> list[CandidateFranchisee]:
    """operator_stores から registry 未登録の operator を集計。

    条件:
      - brand が指定ブランドと一致
      - discovered_via != 'registry' (既登録を除外)
      - operator_type が 'franchisor' / 'direct' でない (本部・直営は除外)
      - store_count >= min_stores

    Note: 既に registry 登録済の operator (discovered_via='registry' エントリが
    1 件でもあれば) はリストから除外する。
    """
    conn = sqlite3.connect(db_path)
    try:
        # Registry 既登録の operator name 一覧
        registered_names = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT operator_name FROM operator_stores "
                "WHERE discovered_via='registry' AND brand=?",
                (brand,),
            ).fetchall()
        }

        rows = conn.execute(
            "SELECT operator_name, COUNT(DISTINCT place_id) AS n, "
            "       MIN(discovered_via) AS via "
            "FROM operator_stores "
            "WHERE brand=? "
            "  AND operator_name != '' "
            "  AND COALESCE(operator_type,'') NOT IN ('franchisor','direct') "
            "  AND COALESCE(discovered_via,'') != 'registry' "
            "GROUP BY operator_name "
            "HAVING n >= ? "
            "ORDER BY n DESC",
            (brand, min_stores),
        ).fetchall()
    finally:
        conn.close()

    out: list[CandidateFranchisee] = []
    for name, n, via in rows:
        if name in registered_names:
            continue
        out.append(
            CandidateFranchisee(
                name=name, brand=brand,
                estimated_store_count=int(n),
                first_seen_via=str(via or ""),
            )
        )
    return out


# ─── YAML 出力 ────────────────────────────────────────────────────────


def export_candidates_to_yaml(
    candidates: list[CandidateFranchisee],
    *,
    out_path: str,
) -> None:
    """franchisee_registry.yaml に追記できる形式で候補を書き出す。

    人間が中身を確認して既存 YAML にコピペする運用。または別途
    agent で gBizINFO 検証 → corporate_number を埋める処理に繋げる。
    """
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Phase 17.4: Registry 拡充候補 (per_store extraction 由来)")
    lines.append(f"# 候補 {len(candidates)} 社")
    lines.append("")
    for c in candidates:
        lines.append(f"# brand: {c.brand}")
        lines.append(f"- name: {c.name}")
        lines.append(f'  corporate_number: ""  # TODO: gBizINFO で確認')
        lines.append(f"  estimated_store_count: {c.estimated_store_count}")
        lines.append(f'  source: "{c.source}"')
        lines.append(f'  first_seen_via: "{c.first_seen_via}"')
        lines.append(f"  verified_at: \"\"  # TODO: ファクトチェック後に埋める")
        lines.append("")
    p.write_text("\n".join(lines), encoding="utf-8")


# ─── CSV loader ────────────────────────────────────────────────────


def load_unknown_stores_csv(path: str) -> list[dict]:
    """audit の `*-unknown-stores.csv` を読み込む。"""
    out: list[dict] = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            out.append(row)
    return out


# ─── Phase 19: 横断 (cross-brand) メガジー集計 ─────────────────────────


@dataclass
class CrossBrandOperator:
    """複数ブランドを跨いだ 1 事業会社の集計行。

    例: 大和フーヅ = {ミスタードーナツ:48, モスバーガー:18} → total 66 店。
    1 ブランドごとの件数 `brand_counts` を保持し、total_stores は合計。
    """

    name: str
    total_stores: int
    brand_counts: dict[str, int] = field(default_factory=dict)
    corporate_number: str = ""
    operator_types: set[str] = field(default_factory=set)
    discovered_vias: set[str] = field(default_factory=set)

    @property
    def brand_count(self) -> int:
        """運営しているブランド数 (多業態度)。"""
        return len(self.brand_counts)


def _load_known_franchisor_names() -> set[str]:
    """registry YAML の master_franchisor 名を全 brand 横断で収集する。

    megafranchisee 集計で本部 (= 加盟店でなく franchisor) を混入させないために
    使う。master_franchisor が per_store 抽出で operator_type='unknown' のまま
    登録されてしまうケース (例: モスバーガー per_store で『株式会社モス
    フードサービス』が 363 件) を除外する。

    また、明らかに別ブランドの本部 (例: ドムドムフードサービス = ドムドム
    ハンバーガーの本部) もブロックリストとして追加する。
    """
    names: set[str] = set()
    # master_franchisor from registry
    try:
        from .franchisee_registry import load_registry

        reg = load_registry()
        for brand_name, br in reg.brands.items():
            mf = (br.master_franchisor or {}).get("name", "")
            if mf:
                names.add(mf)
    except Exception:
        pass
    # 明示ブロックリスト (別ブランドの本部が per_store で誤抽出されたとき用)
    names.update({
        "株式会社ドムドムフードサービス",
        "ドムドムフードサービス",
        "株式会社日本マクドナルドホールディングス",
        "日本マクドナルドホールディングス",
    })
    return names


def aggregate_cross_brand_operators(
    *,
    db_path: str,
    min_total_stores: int = 1,
    min_brands: int = 1,
    exclude_franchisor: bool = True,
    normalize_names: bool = True,
    extra_franchisor_blocklist: set[str] | None = None,
) -> list[CrossBrandOperator]:
    """operator_stores を brand 指定なしで集計 (1 operator = 1 行)。

    - 同一 operator が複数ブランドに跨って保有していれば全ブランド合算
    - operator_type in ('franchisor', 'direct') は本部・直営なので除外
      (exclude_franchisor=True のとき)
    - total_stores >= min_total_stores AND brand_count >= min_brands
    - 合計店舗数降順でソート
    - normalize_names=True で "㈱X" / "(株)X" / "株式会社X" 等の表記ゆれを
      同一 operator として集約 (正規化後の文字列を key、元表記のうち最長を表示)

    対象: メガジー (20+店) から中堅 (2店) まで、1 行で全事業実態を見たい
    ときのエントリポイント。
    """
    if normalize_names:
        try:
            from .normalize import normalize_operator_name as _norm
        except ImportError:
            _norm = lambda x: x  # noqa: E731
    else:
        _norm = lambda x: x  # noqa: E731
    conn = sqlite3.connect(db_path)
    try:
        # ブランド別内訳まで一度に拾う
        filt = ""
        if exclude_franchisor:
            filt = " AND COALESCE(operator_type,'') NOT IN ('franchisor','direct')"
        rows = conn.execute(
            "SELECT operator_name, brand, "
            "       COUNT(DISTINCT place_id) AS n, "
            "       COALESCE(MAX(corporate_number),'') AS cn, "
            "       COALESCE(MAX(operator_type),'') AS ot, "
            "       COALESCE(MAX(discovered_via),'') AS dv "
            "FROM operator_stores "
            "WHERE operator_name != ''"
            + filt
            + " GROUP BY operator_name, brand "
            + "ORDER BY operator_name, n DESC"
        ).fetchall()
    finally:
        conn.close()

    # 本部 / 別ブランド本部の除外セット (正規化済で比較)
    blocklist_raw = _load_known_franchisor_names()
    if extra_franchisor_blocklist:
        blocklist_raw = blocklist_raw | set(extra_franchisor_blocklist)
    blocklist = {_norm(n) or n for n in blocklist_raw}

    by_operator: dict[str, CrossBrandOperator] = {}
    # 元表記の候補を key ごとに保持して最長を表示名にする
    displays: dict[str, list[str]] = {}
    for operator_name, brand, n, cn, ot, dv in rows:
        if not operator_name:
            continue
        name_key = _norm(operator_name) or operator_name
        # 本部・フランチャイザー名と一致すれば除外 (名前ベース)
        if exclude_franchisor and name_key in blocklist:
            continue
        # identity key: corporate_number があれば最優先 (表記揺れ横断)、
        # 無ければ正規化 name で fallback
        cn_clean = str(cn or "").strip()
        key = f"corp:{cn_clean}" if cn_clean else f"name:{name_key}"
        displays.setdefault(key, []).append(operator_name)
        op = by_operator.get(key)
        if op is None:
            op = CrossBrandOperator(
                name=operator_name,  # 後で最長に差し替え
                total_stores=0,
                corporate_number=str(cn or ""),
            )
            by_operator[key] = op
        # 同一 (operator, brand) は集計済の可能性があるので加算でなく max
        b = str(brand or "")
        op.brand_counts[b] = op.brand_counts.get(b, 0) + int(n)
        op.total_stores += int(n)
        if ot:
            op.operator_types.add(str(ot))
        if dv:
            op.discovered_vias.add(str(dv))
        # 最初に来た非空 corporate_number を固定
        if not op.corporate_number and cn:
            op.corporate_number = str(cn)

    # 元表記のうち最も長い (情報量の多い) ものを表示名に採用
    for key, op in by_operator.items():
        candidates = displays.get(key, [op.name])
        op.name = max(candidates, key=lambda s: (len(s), s))

    out: list[CrossBrandOperator] = []
    for op in by_operator.values():
        if op.total_stores < min_total_stores:
            continue
        if op.brand_count < min_brands:
            continue
        out.append(op)
    # 合計降順、同数なら名前昇順
    out.sort(key=lambda o: (-o.total_stores, o.name))
    return out


def export_cross_brand_to_csv(
    operators: list[CrossBrandOperator],
    *,
    out_path: str,
) -> None:
    """メガジー横断 CSV を書き出す。

    列: operator_name, total_stores, brand_count, brands_breakdown,
        corporate_number, operator_types, discovered_vias
    brands_breakdown は "ブランドA:N; ブランドB:M; ..." の 1 カラム文字列。
    """
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv_mod.writer(f)
        w.writerow([
            "operator_name",
            "total_stores",
            "brand_count",
            "brands_breakdown",
            "corporate_number",
            "operator_types",
            "discovered_vias",
        ])
        for op in operators:
            breakdown = "; ".join(
                f"{b}:{n}" for b, n in sorted(
                    op.brand_counts.items(), key=lambda kv: (-kv[1], kv[0])
                )
            )
            w.writerow([
                op.name,
                op.total_stores,
                op.brand_count,
                breakdown,
                op.corporate_number,
                ",".join(sorted(op.operator_types)),
                ",".join(sorted(op.discovered_vias)),
            ])


def export_cross_brand_to_yaml(
    operators: list[CrossBrandOperator],
    *,
    out_path: str,
) -> None:
    """operator-first YAML で書き出す。

    既存 `franchisee_registry.yaml` は brand-first だが、メガジー分析では
    operator を主語にした逆方向 index が欲しい。これはその候補 dump。
    """
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Phase 19: 横断メガジー候補 (operator → brands 逆方向 index)")
    lines.append(f"# {len(operators)} 社、合計店舗数降順")
    lines.append("")
    lines.append("operators:")
    for op in operators:
        # key のコロン安全化
        safe_key = op.name.replace(":", "：")
        lines.append(f"  {safe_key}:")
        lines.append(f"    total_stores: {op.total_stores}")
        lines.append(f"    brand_count: {op.brand_count}")
        if op.corporate_number:
            lines.append(f'    corporate_number: "{op.corporate_number}"')
        else:
            lines.append(
                '    corporate_number: ""  # TODO: gBizINFO で確認'
            )
        lines.append("    brands:")
        for b, n in sorted(
            op.brand_counts.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            safe_b = (b or "unknown").replace(":", "：")
            lines.append(f"      {safe_b}: {n}")
        if op.operator_types:
            lines.append(
                "    operator_types: ["
                + ", ".join(f'"{t}"' for t in sorted(op.operator_types))
                + "]"
            )
        if op.discovered_vias:
            lines.append(
                "    discovered_vias: ["
                + ", ".join(f'"{d}"' for d in sorted(op.discovered_vias))
                + "]"
            )
        lines.append("")
    p.write_text("\n".join(lines), encoding="utf-8")
