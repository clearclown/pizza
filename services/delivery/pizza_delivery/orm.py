"""PI-ZZA ORM 中央集約 (SQLAlchemy 2.x)。

手動 YAML (franchisee_registry.yaml) を最終的に廃止し、DB に集約する。
外部ソース (JFA 協会 / BC 誌 / gBizINFO / 国税庁 CSV / Places API) から
pipeline で自動的に取込む。

Table 設計:
  - franchise_brand        FC ブランド (マクドナルド / モス / エニタイム …)
  - operator_company       事業会社 (株式会社モスストアカンパニー 等)
                           corporate_number でユニーク
  - brand_operator_link    ブランド × 事業会社 (多対多、店舗数推定を伴う)
  - data_source            どこから取ったか (jfa, bc2024, houjin_csv, places, manual)

設計方針:
  - ORM model のみで scheme 定義、raw SQL は極力避ける
  - 既存 sqlite3 直叩きモジュール (franchisee_registry.py / houjin_csv.py) は
    当面共存させる。段階的に ORM 化する
  - 外部ソースからの自動取込で、`manual` source は人間が registry に手で書いた
    時だけ使う (→ いずれ撤廃)
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    text,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)


def _default_db_path() -> Path:
    """repo root / var/pizza-registry.sqlite を返す (operator_stores DB と別管理)。"""
    here = Path(__file__).resolve()
    root = here.parents[3]
    return root / "var" / "pizza-registry.sqlite"


def default_engine():
    """pizza-registry DB 用のデフォルトエンジン。"""
    p = _default_db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)


class Base(DeclarativeBase):
    pass


class FranchiseBrand(Base):
    """FC ブランド本部。"""

    __tablename__ = "franchise_brand"
    __table_args__ = (UniqueConstraint("name", name="uq_franchise_brand_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    industry: Mapped[str] = mapped_column(String(80), default="", index=True)
    master_franchisor_name: Mapped[str] = mapped_column(String(200), default="")
    master_franchisor_corp: Mapped[str] = mapped_column(String(13), default="")
    jfa_member: Mapped[bool] = mapped_column(default=False)
    source: Mapped[str] = mapped_column(String(40), default="manual", index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    links: Mapped[list["BrandOperatorLink"]] = relationship(
        back_populates="brand", cascade="all, delete-orphan"
    )


class OperatorCompany(Base):
    """事業会社 (FC 加盟企業 / 本部自体 も含む)。

    corporate_number は「空でなければ一意」という条件付き unique。
    法人番号 未特定な operator を複数レコード持てるようにしている。
    """

    __tablename__ = "operator_company"
    __table_args__ = (
        # partial unique index: corporate_number が空文字列なら衝突しない
        Index(
            "uq_op_corp_nonempty",
            "corporate_number",
            unique=True,
            sqlite_where=text("corporate_number != ''"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    corporate_number: Mapped[str] = mapped_column(String(13), default="", index=True)
    head_office: Mapped[str] = mapped_column(String(200), default="")
    prefecture: Mapped[str] = mapped_column(String(20), default="", index=True)
    kind: Mapped[str] = mapped_column(String(40), default="")  # franchisee / franchisor / direct
    source: Mapped[str] = mapped_column(String(40), default="manual", index=True)
    note: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    links: Mapped[list["BrandOperatorLink"]] = relationship(
        back_populates="operator", cascade="all, delete-orphan"
    )


class BrandOperatorLink(Base):
    """ブランド × 事業会社 の多対多リンク。

    `estimated_store_count` は出典時点での推定店舗数。時系列で複数レコードを
    持たせるため (brand_id, operator_id, source) で uniq。
    """

    __tablename__ = "brand_operator_link"
    __table_args__ = (
        UniqueConstraint(
            "brand_id", "operator_id", "source", name="uq_bol"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    brand_id: Mapped[int] = mapped_column(ForeignKey("franchise_brand.id"), index=True)
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator_company.id"), index=True)
    estimated_store_count: Mapped[int] = mapped_column(Integer, default=0)
    observed_at: Mapped[str] = mapped_column(String(20), default="")  # "2024-08" 等
    operator_type: Mapped[str] = mapped_column(String(40), default="franchisee")
    source: Mapped[str] = mapped_column(String(40), default="manual", index=True)
    source_url: Mapped[str] = mapped_column(Text, default="")
    note: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    brand: Mapped[FranchiseBrand] = relationship(back_populates="links")
    operator: Mapped[OperatorCompany] = relationship(back_populates="links")


class DataSource(Base):
    """取込みソースの管理 (URL + timestamp)。JFA scrape の再実行判定に使う。"""

    __tablename__ = "data_source"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    url: Mapped[str] = mapped_column(Text, default="")
    last_fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    record_count: Mapped[int] = mapped_column(Integer, default=0)
    note: Mapped[str] = mapped_column(Text, default="")


# ─── helper ────────────────────────────────────────────────


def make_session(engine=None) -> Session:
    """ORM session factory。テストでは in-memory engine を渡す。"""
    engine = engine or default_engine()
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal()


def create_all(engine=None) -> None:
    """スキーマを作成 (テストや初期化用)。"""
    Base.metadata.create_all(engine or default_engine())


def upsert_brand(sess: Session, name: str, *, source: str = "manual",
                 industry: str = "", master_franchisor_name: str = "",
                 master_franchisor_corp: str = "", jfa_member: bool = False) -> FranchiseBrand:
    """name でブランドを upsert して ORM インスタンスを返す。"""
    brand = sess.query(FranchiseBrand).filter_by(name=name).one_or_none()
    if brand is None:
        brand = FranchiseBrand(name=name, source=source)
        sess.add(brand)
    if industry:
        brand.industry = industry
    if master_franchisor_name:
        brand.master_franchisor_name = master_franchisor_name
    if master_franchisor_corp:
        brand.master_franchisor_corp = master_franchisor_corp
    if jfa_member:
        brand.jfa_member = True
    return brand


def upsert_operator(
    sess: Session,
    *,
    name: str,
    corporate_number: str = "",
    head_office: str = "",
    prefecture: str = "",
    kind: str = "",
    source: str = "manual",
    note: str = "",
) -> OperatorCompany:
    """corporate_number (あれば) 優先で upsert。無ければ name 一致で探す。"""
    op: OperatorCompany | None = None
    if corporate_number:
        op = sess.query(OperatorCompany).filter_by(corporate_number=corporate_number).one_or_none()
    if op is None:
        op = sess.query(OperatorCompany).filter_by(name=name, corporate_number="").one_or_none()
    if op is None:
        op = OperatorCompany(name=name, corporate_number=corporate_number, source=source)
        sess.add(op)
    if name:
        op.name = name
    if corporate_number:
        op.corporate_number = corporate_number
    if head_office:
        op.head_office = head_office
    if prefecture:
        op.prefecture = prefecture
    if kind:
        op.kind = kind
    if note:
        op.note = note
    return op


def link_brand_operator(
    sess: Session,
    *,
    brand: FranchiseBrand,
    operator: OperatorCompany,
    estimated_store_count: int = 0,
    observed_at: str = "",
    operator_type: str = "franchisee",
    source: str = "manual",
    source_url: str = "",
    note: str = "",
) -> BrandOperatorLink:
    """ブランド × 事業会社 の link を upsert (source 単位で 1 本)。"""
    link = (
        sess.query(BrandOperatorLink)
        .filter_by(brand_id=brand.id, operator_id=operator.id, source=source)
        .one_or_none()
    )
    if link is None:
        link = BrandOperatorLink(
            brand_id=brand.id, operator_id=operator.id, source=source,
        )
        sess.add(link)
    if estimated_store_count:
        link.estimated_store_count = estimated_store_count
    if observed_at:
        link.observed_at = observed_at
    if operator_type:
        link.operator_type = operator_type
    if source_url:
        link.source_url = source_url
    if note:
        link.note = note
    return link
