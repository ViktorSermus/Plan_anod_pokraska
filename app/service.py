from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.db import connect, fetch_all, init_db, log_etl_import, upsert_master
from app.etl import EtlResult, load_reestr_upload, load_znom_uploads, transform_master


@dataclass
class RefreshStats:
    znom_files_read: int
    znom_files_failed: int
    reestr_files_read: int
    reestr_files_failed: int
    rows_added: int
    rows_updated: int
    rows_archived: int
    errors: list[str]


def _db_rows_to_znom_input(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Дата заявки",
                "№ заявки",
                "Наименование",
                "Кол-во м.п.",
                "Хлысты",
                "Услуга",
                "Автор",
                "Клиент",
            ]
        )
    out = pd.DataFrame(
        {
            "Дата заявки": df.get("date_request"),
            "№ заявки": df.get("request_no"),
            "Наименование": df.get("item_name"),
            "Кол-во м.п.": df.get("qty_mp"),
            "Хлысты": df.get("qty_bars"),
            "Услуга": df.get("service"),
            "Автор": df.get("author"),
            "Клиент": df.get("client"),
        }
    )
    return out


def refresh_from_uploads(
    database_url: str,
    znom_parts: list[tuple[bytes, str]],
    reestr: tuple[bytes, str] | None,
    *,
    archive_missing: bool = True,
    actor_user_id: str | None = None,
    actor_email: str | None = None,
    supabase_pooler_region: str | None = None,
) -> RefreshStats:
    znom = load_znom_uploads(znom_parts)
    if reestr:
        reestr_res = load_reestr_upload(reestr[0], reestr[1])
    else:
        reestr_res = EtlResult(pd.DataFrame(), 0, 0, [])

    conn = connect(database_url, supabase_pooler_region=supabase_pooler_region)
    try:
        init_db(conn)
        znom_source = znom.dataframe
        if znom_source.empty and not reestr_res.dataframe.empty:
            existing_active = fetch_all(conn, include_inactive=False)
            znom_source = _db_rows_to_znom_input(existing_active)

        master = transform_master(znom_source, reestr_res.dataframe)
        up = upsert_master(conn, master, archive_missing=archive_missing and not znom.dataframe.empty)
        stats = RefreshStats(
            znom_files_read=znom.files_read,
            znom_files_failed=znom.files_failed,
            reestr_files_read=reestr_res.files_read,
            reestr_files_failed=reestr_res.files_failed,
            rows_added=up["added"],
            rows_updated=up["updated"],
            rows_archived=up["archived"],
            errors=[*znom.errors, *reestr_res.errors],
        )
        log_etl_import(
            conn,
            actor_user_id=actor_user_id,
            actor_email=actor_email,
            stats={
                "znom_files_read": stats.znom_files_read,
                "znom_files_failed": stats.znom_files_failed,
                "reestr_files_read": stats.reestr_files_read,
                "reestr_files_failed": stats.reestr_files_failed,
                "rows_added": stats.rows_added,
                "rows_updated": stats.rows_updated,
                "rows_archived": stats.rows_archived,
            },
        )
    finally:
        conn.close()

    return stats


def get_grid_data(
    database_url: str,
    include_inactive: bool,
    *,
    supabase_pooler_region: str | None = None,
) -> pd.DataFrame:
    conn = connect(database_url, supabase_pooler_region=supabase_pooler_region)
    try:
        init_db(conn)
        return fetch_all(conn, include_inactive=include_inactive)
    finally:
        conn.close()
