from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

TABLE = "master_data"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            business_key TEXT PRIMARY KEY,
            date_request TEXT,
            request_no TEXT,
            item_name TEXT,
            service TEXT,
            author TEXT,
            client TEXT,
            qty_mp REAL,
            qty_bars REAL,
            moved_mp REAL,
            reserved_mp REAL,
            processed_mp REAL,
            processed_bars REAL,
            exported REAL,
            correction REAL,
            note TEXT,
            remaining REAL,
            is_active INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()

    # Migration for existing DBs: add `author` column if missing.
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({TABLE})").fetchall()}
    if "author" not in cols:
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN author TEXT;")
        conn.commit()
    if "client" not in cols:
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN client TEXT;")
        conn.commit()
    if "service" not in cols:
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN service TEXT;")
        conn.commit()
    if "correction" not in cols:
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN correction REAL;")
        conn.commit()
    if "note" not in cols:
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN note TEXT;")
        conn.commit()


def upsert_master(conn: sqlite3.Connection, df: pd.DataFrame, archive_missing: bool = True) -> dict[str, int]:
    if df.empty:
        return {"added": 0, "updated": 0, "archived": 0}

    existing = pd.read_sql_query(f"SELECT business_key, exported, correction, note FROM {TABLE}", conn)
    existing_map = {
        r["business_key"]: (r["exported"], r["correction"], r["note"]) for _, r in existing.iterrows()
    }
    incoming_keys = set(df["business_key"].tolist())

    added = 0
    updated = 0
    for _, r in df.iterrows():
        ex_row = existing_map.get(r["business_key"], (None, None, None))
        exported, correction, note = ex_row
        if r["business_key"] in existing_map:
            updated += 1
        else:
            added += 1

        remaining = (r["Кол-во хлыстов обработанных"] if pd.notna(r["Кол-во хлыстов обработанных"]) else None)
        if remaining is not None and exported is not None:
            remaining = float(remaining) - float(exported)

        conn.execute(
            f"""
            INSERT INTO {TABLE} (
                business_key, date_request, request_no, item_name, service, author, client, qty_mp, qty_bars,
                moved_mp, reserved_mp, processed_mp, processed_bars, exported, correction, note, remaining, is_active, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(business_key) DO UPDATE SET
                date_request=excluded.date_request,
                request_no=excluded.request_no,
                item_name=excluded.item_name,
                service=excluded.service,
                author=excluded.author,
                client=excluded.client,
                qty_mp=excluded.qty_mp,
                qty_bars=excluded.qty_bars,
                moved_mp=excluded.moved_mp,
                reserved_mp=excluded.reserved_mp,
                processed_mp=excluded.processed_mp,
                processed_bars=excluded.processed_bars,
                exported=COALESCE({TABLE}.exported, excluded.exported),
                correction=COALESCE({TABLE}.correction, excluded.correction),
                note=COALESCE({TABLE}.note, excluded.note),
                remaining=CASE
                    WHEN COALESCE({TABLE}.exported, excluded.exported) IS NULL THEN excluded.processed_bars
                    ELSE excluded.processed_bars - COALESCE({TABLE}.exported, excluded.exported)
                END,
                is_active=1,
                updated_at=CURRENT_TIMESTAMP
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
                remaining,
            ),
        )

    archived = 0
    if archive_missing:
        cur = conn.execute(f"SELECT business_key FROM {TABLE} WHERE is_active = 1")
        active_keys = {row["business_key"] for row in cur.fetchall()}
        to_archive = active_keys - incoming_keys
        for key in to_archive:
            conn.execute(f"UPDATE {TABLE} SET is_active = 0, updated_at=CURRENT_TIMESTAMP WHERE business_key = ?", (key,))
        archived = len(to_archive)

    conn.commit()
    return {"added": added, "updated": updated, "archived": archived}


def set_export_fields(
    conn: sqlite3.Connection, business_key: str, exported: float | None, correction: float | None, note: str | None
) -> None:
    conn.execute(
        f"""
        UPDATE {TABLE}
        SET exported = ?,
            correction = ?,
            note = ?,
            remaining = CASE
                WHEN processed_bars IS NULL OR ? IS NULL THEN processed_bars
                ELSE processed_bars - ?
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE business_key = ?
        """,
        (exported, correction, note, exported, exported, business_key),
    )
    conn.commit()


def fetch_all(conn: sqlite3.Connection, include_inactive: bool = False) -> pd.DataFrame:
    q = f"SELECT * FROM {TABLE}"
    if not include_inactive:
        q += " WHERE is_active = 1"
    q += " ORDER BY date_request DESC, request_no ASC"
    return pd.read_sql_query(q, conn)
