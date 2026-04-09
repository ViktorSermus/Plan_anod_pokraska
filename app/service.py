from __future__ import annotations

from dataclasses import dataclass

from app.config import AppConfig
from app.db import connect, fetch_all, init_db, upsert_master
from app.etl import load_latest_reestr, load_znom_folder, transform_master


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


def refresh_data(cfg: AppConfig) -> RefreshStats:
    znom = load_znom_folder(cfg.znom_dir, cfg.file_patterns)
    reestr = load_latest_reestr(cfg.reestr_dir, cfg.file_patterns)
    master = transform_master(znom.dataframe, reestr.dataframe)

    conn = connect(cfg.db_path)
    try:
        init_db(conn)
        up = upsert_master(conn, master, archive_missing=cfg.archive_missing_as_inactive)
    finally:
        conn.close()

    return RefreshStats(
        znom_files_read=znom.files_read,
        znom_files_failed=znom.files_failed,
        reestr_files_read=reestr.files_read,
        reestr_files_failed=reestr.files_failed,
        rows_added=up["added"],
        rows_updated=up["updated"],
        rows_archived=up["archived"],
        errors=[*znom.errors, *reestr.errors],
    )


def get_grid_data(cfg: AppConfig, include_inactive: bool):
    conn = connect(cfg.db_path)
    try:
        init_db(conn)
        return fetch_all(conn, include_inactive=include_inactive)
    finally:
        conn.close()
