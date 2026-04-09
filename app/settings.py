from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppSettings:
    database_url: str
    """Регион AWS из Supabase (eu-central-1 и т.д.) — для Session pooler при DATABASE_URL на db.*.supabase.co."""
    supabase_pooler_region: str | None
    supabase_url: str
    supabase_anon_key: str
    """Полный URL приложения (например https://xxx.streamlit.app) — для redirect OAuth."""
    app_base_url: str
    archive_missing_as_inactive: bool


def load_app_settings() -> AppSettings:
    database_url = _get_secret("DATABASE_URL")
    _pooler_reg = (_get_secret("SUPABASE_POOLER_REGION") or "").strip()
    supabase_pooler_region = _pooler_reg or None
    supabase_url = _get_secret("SUPABASE_URL")
    supabase_anon_key = _get_secret("SUPABASE_ANON_KEY")
    app_base_url = (_get_secret("APP_BASE_URL") or os.environ.get("APP_BASE_URL") or "http://localhost:8501").rstrip("/")

    missing = []
    if not database_url:
        missing.append("DATABASE_URL")
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_anon_key:
        missing.append("SUPABASE_ANON_KEY")
    if missing:
        raise RuntimeError(
            "Не заданы переменные: "
            + ", ".join(missing)
            + ". Добавьте их в .streamlit/secrets.toml (локально) или в Secrets на Streamlit Cloud."
        )

    arch = _get_secret("ARCHIVE_MISSING_AS_INACTIVE")
    if arch is None:
        archive_missing = True
    else:
        archive_missing = str(arch).strip().lower() in ("1", "true", "yes", "on")

    return AppSettings(
        database_url=database_url.strip(),
        supabase_pooler_region=supabase_pooler_region,
        supabase_url=supabase_url.strip(),
        supabase_anon_key=supabase_anon_key.strip(),
        app_base_url=app_base_url,
        archive_missing_as_inactive=archive_missing,
    )


def _get_secret(name: str) -> str | None:
    v = os.environ.get(name)
    if v and str(v).strip():
        return str(v).strip()
    try:
        import streamlit as st

        if name not in st.secrets:
            return None
        val = st.secrets[name]
        if val is None:
            return None
        s = str(val).strip()
        return s or None
    except Exception:
        return None
