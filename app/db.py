from __future__ import annotations

from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import Json

TABLE = "master_data"
AUDIT_TABLE = "audit_log"


def connect(dsn: str) -> psycopg2.extensions.connection:
    return psycopg2.connect(dsn)


def init_db(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                business_key TEXT PRIMARY KEY,
                date_request TEXT,
                request_no TEXT,
                item_name TEXT,
                service TEXT,
                author TEXT,
                client TEXT,
                qty_mp DOUBLE PRECISION,
                qty_bars DOUBLE PRECISION,
                moved_mp DOUBLE PRECISION,
                reserved_mp DOUBLE PRECISION,
                processed_mp DOUBLE PRECISION,
                processed_bars DOUBLE PRECISION,
                exported DOUBLE PRECISION,
                correction DOUBLE PRECISION,
                note TEXT,
                remaining DOUBLE PRECISION,
                is_active INTEGER NOT NULL DEFAULT 1,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                actor_user_id TEXT,
                actor_email TEXT,
                action TEXT NOT NULL,
                business_key TEXT,
                field_name TEXT,
                old_value TEXT,
                new_value TEXT,
                metadata JSONB
            );
            """
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON {AUDIT_TABLE} (created_at DESC);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_audit_log_business_key ON {AUDIT_TABLE} (business_key);")
    conn.commit()


def _append_audit(
    conn: psycopg2.extensions.connection,
    *,
    actor_user_id: str | None,
    actor_email: str | None,
    action: str,
    business_key: str | None,
    field_name: str | None,
    old_value: Any,
    new_value: Any,
    metadata: dict | None = None,
) -> None:
    ov = None if old_value is None else str(old_value)
    nv = None if new_value is None else str(new_value)
    meta_adapt = Json(metadata) if metadata is not None else None
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {AUDIT_TABLE}
                (actor_user_id, actor_email, action, business_key, field_name, old_value, new_value, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (actor_user_id, actor_email, action, business_key, field_name, ov, nv, meta_adapt),
        )


def upsert_master(conn: psycopg2.extensions.connection, df: pd.DataFrame, archive_missing: bool = True) -> dict[str, int]:
    if df.empty:
        return {"added": 0, "updated": 0, "archived": 0}

    existing = pd.read_sql_query(f"SELECT business_key, exported, correction, note FROM {TABLE}", conn)
    existing_map = {
        r["business_key"]: (r["exported"], r["correction"], r["note"]) for _, r in existing.iterrows()
    }
    incoming_keys = set(df["business_key"].tolist())

    added = 0
    updated = 0
    with conn.cursor() as cur:
        for _, r in df.iterrows():
            ex_row = existing_map.get(r["business_key"], (None, None, None))
            exported, correction, note = ex_row
            if r["business_key"] in existing_map:
                updated += 1
            else:
                added += 1

            remaining = r["Кол-во хлыстов обработанных"] if pd.notna(r["Кол-во хлыстов обработанных"]) else None
            if remaining is not None and exported is not None:
                remaining = float(remaining) - float(exported)

            cur.execute(
                f"""
                INSERT INTO {TABLE} (
                    business_key, date_request, request_no, item_name, service, author, client, qty_mp, qty_bars,
                    moved_mp, reserved_mp, processed_mp, processed_bars, exported, correction, note, remaining, is_active, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, NOW())
                ON CONFLICT (business_key) DO UPDATE SET
                    date_request = EXCLUDED.date_request,
                    request_no = EXCLUDED.request_no,
                    item_name = EXCLUDED.item_name,
                    service = EXCLUDED.service,
                    author = EXCLUDED.author,
                    client = EXCLUDED.client,
                    qty_mp = EXCLUDED.qty_mp,
                    qty_bars = EXCLUDED.qty_bars,
                    moved_mp = EXCLUDED.moved_mp,
                    reserved_mp = EXCLUDED.reserved_mp,
                    processed_mp = EXCLUDED.processed_mp,
                    processed_bars = EXCLUDED.processed_bars,
                    exported = COALESCE({TABLE}.exported, EXCLUDED.exported),
                    correction = COALESCE({TABLE}.correction, EXCLUDED.correction),
                    note = COALESCE({TABLE}.note, EXCLUDED.note),
                    remaining = CASE
                        WHEN COALESCE({TABLE}.exported, EXCLUDED.exported) IS NULL THEN EXCLUDED.processed_bars
                        ELSE EXCLUDED.processed_bars - COALESCE({TABLE}.exported, EXCLUDED.exported)
                    END,
                    is_active = 1,
                    updated_at = NOW()
                """,
                (
                    r["business_key"],
                    str(pd.to_datetime(r["Дата заявки"], errors="coerce").date()) if pd.notna(r["Дата заявки"]) else None,
                    r["№ заявки"],
                    r["Наименование"],
                    None if pd.isna(r.get("Услуга", pd.NA)) else str(r.get("Услуга")),
                    None if pd.isna(r.get("Автор", pd.NA)) else str(r.get("Автор")),
                    None if pd.isna(r.get("Клиент", pd.NA)) else str(r.get("Клиент")),
                    float(r["Сумма по полю Кол-во м.п."]) if pd.notna(r["Сумма по полю Кол-во м.п."]) else None,
                    float(r["Сумма по полю Хлысты"]) if pd.notna(r["Сумма по полю Хлысты"]) else None,
                    float(r["Перемещено м.п."]) if pd.notna(r["Перемещено м.п."]) else None,
                    float(r["Бронь под обработку м.п."]) if pd.notna(r["Бронь под обработку м.п."]) else None,
                    float(r["Обработано м.п."]) if pd.notna(r["Обработано м.п."]) else None,
                    float(r["Кол-во хлыстов обработанных"]) if pd.notna(r["Кол-во хлыстов обработанных"]) else None,
                    exported,
                    correction,
                    note,
                    float(remaining) if remaining is not None else None,
                ),
            )

        archived = 0
        if archive_missing:
            cur.execute(f"SELECT business_key FROM {TABLE} WHERE is_active = 1")
            active_keys = {row[0] for row in cur.fetchall()}
            to_archive = active_keys - incoming_keys
            for key in to_archive:
                cur.execute(
                    f"UPDATE {TABLE} SET is_active = 0, updated_at = NOW() WHERE business_key = %s",
                    (key,),
                )
            archived = len(to_archive)

    conn.commit()
    return {"added": added, "updated": updated, "archived": archived}


def _float_differs(a: float | None, b: float | None) -> bool:
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    return abs(float(a) - float(b)) > 1e-9


def set_export_fields(
    conn: psycopg2.extensions.connection,
    business_key: str,
    exported: float | None,
    correction: float | None,
    note: str | None,
    *,
    actor_user_id: str | None = None,
    actor_email: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT exported, correction, note FROM {TABLE} WHERE business_key = %s",
            (business_key,),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return
        old_exp, old_corr, old_note = row[0], row[1], row[2]

        cur.execute(
            f"""
            UPDATE {TABLE}
            SET exported = %s,
                correction = %s,
                note = %s,
                remaining = CASE
                    WHEN processed_bars IS NULL OR %s IS NULL THEN processed_bars
                    ELSE processed_bars - %s
                END,
                updated_at = NOW()
            WHERE business_key = %s
            """,
            (exported, correction, note, exported, exported, business_key),
        )

        if _float_differs(
            float(old_exp) if old_exp is not None else None,
            exported,
        ):
            _append_audit(
                conn,
                actor_user_id=actor_user_id,
                actor_email=actor_email,
                action="manual_edit",
                business_key=business_key,
                field_name="exported",
                old_value=old_exp,
                new_value=exported,
            )
        if _float_differs(
            float(old_corr) if old_corr is not None else None,
            correction,
        ):
            _append_audit(
                conn,
                actor_user_id=actor_user_id,
                actor_email=actor_email,
                action="manual_edit",
                business_key=business_key,
                field_name="correction",
                old_value=old_corr,
                new_value=correction,
            )
        on = (old_note or "") if old_note is not None else ""
        nn = (note or "") if note is not None else ""
        if on != nn:
            _append_audit(
                conn,
                actor_user_id=actor_user_id,
                actor_email=actor_email,
                action="manual_edit",
                business_key=business_key,
                field_name="note",
                old_value=old_note,
                new_value=note,
            )

    conn.commit()


def fetch_all(conn: psycopg2.extensions.connection, include_inactive: bool = False) -> pd.DataFrame:
    q = f"SELECT * FROM {TABLE}"
    if not include_inactive:
        q += " WHERE is_active = 1"
    q += " ORDER BY date_request DESC NULLS LAST, request_no ASC"
    return pd.read_sql_query(q, conn)


def log_etl_import(
    conn: psycopg2.extensions.connection,
    *,
    actor_user_id: str | None,
    actor_email: str | None,
    stats: dict[str, int | list],
) -> None:
    """Одна запись в аудит по завершении импорта из файлов."""
    meta = {
        "znom_files_read": stats.get("znom_files_read"),
        "znom_files_failed": stats.get("znom_files_failed"),
        "reestr_files_read": stats.get("reestr_files_read"),
        "reestr_files_failed": stats.get("reestr_files_failed"),
        "rows_added": stats.get("rows_added"),
        "rows_updated": stats.get("rows_updated"),
        "rows_archived": stats.get("rows_archived"),
    }
    _append_audit(
        conn,
        actor_user_id=actor_user_id,
        actor_email=actor_email,
        action="etl_import",
        business_key=None,
        field_name=None,
        old_value=None,
        new_value=None,
        metadata=meta,
    )
    conn.commit()
