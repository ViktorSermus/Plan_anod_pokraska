from __future__ import annotations

import hashlib

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from app.db import connect, init_db, set_export_fields
from app.service import get_grid_data, refresh_from_uploads
from app.settings import load_app_settings
from app.supabase_auth import (
    build_supabase_client,
    current_user,
    logout,
    process_oauth_redirect,
    render_login_page,
    restore_session,
)
from app.status import add_status_column


def _norm_author(a: object) -> str:
    if pd.isna(a) or (isinstance(a, str) and not str(a).strip()):
        return "—"
    return str(a).strip()


def _norm_service(s: object) -> str:
    if pd.isna(s) or (isinstance(s, str) and not str(s).strip()):
        return "—"
    return str(s).strip()


def _strip_item_prefix(v: object) -> str:
    """Убирает всё до '] ' включительно: '[xxx] Текст' -> 'Текст'."""
    if pd.isna(v):
        return ""
    s = str(v).strip()
    marker = "] "
    pos = s.find(marker)
    if pos == -1:
        return s
    return s[pos + len(marker) :].strip()


def _author_initials(v: object) -> str:
    """Фамилия Имя Отчество -> ФИ (без отчества)."""
    if pd.isna(v):
        return "—"
    s = str(v).strip()
    if not s:
        return "—"

    parts = [p for p in s.split() if p]
    if not parts:
        return "—"

    fam = parts[0][0]
    name = ""
    if len(parts) > 1:
        # Поддержка "Иванов И.И." и "Иванов И. И." — берём первую букву имени.
        p1 = parts[1].replace(".", "").strip()
        if p1:
            name = p1[0]
    return (fam + name).upper()


def _fcb_key(prefix: str, label: str) -> str:
    return prefix + hashlib.md5(label.encode("utf-8")).hexdigest()


def _filt_sig(sel: set) -> str:
    if not sel:
        return "0"
    return hashlib.md5("|".join(sorted(sel)).encode()).hexdigest()[:12]


st.set_page_config(page_title="Производственный план по аноду и покарске", layout="wide")
st.markdown(
    """
    <style>
    .main .block-container,
    section.main > div,
    [data-testid="stAppViewContainer"] > .main > div,
    [data-testid="stAppViewContainer"] [data-testid="stMainBlockContainer"] {
        max-width: 97%;
        width: 97%;
        margin-left: auto;
        margin-right: auto;
        padding-left: 0.05rem;
        padding-right: 0.05rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    "<h3 style='margin-top:-8px; margin-bottom:4px;'>Производственный план по аноду и покарске</h3>",
    unsafe_allow_html=True,
)

try:
    settings = load_app_settings()
except Exception as e:
    st.error(str(e))
    st.stop()

supabase = build_supabase_client(settings.supabase_url, settings.supabase_anon_key)
if process_oauth_redirect(supabase):
    st.stop()
restore_session(supabase)
user = current_user()
if not user:
    render_login_page(supabase, settings.app_base_url)
    st.stop()

with st.sidebar:
    st.caption(user.get("email") or user.get("id") or "—")
    if st.button("Выйти", use_container_width=True):
        logout(supabase)

h1, h2, h3 = st.columns([1.4, 1.4, 0.78])
with h1:
    znom_uploads = st.file_uploader(
        "Заявки (ZNOM), несколько файлов",
        type=["xlsx", "xlsm", "xls"],
        accept_multiple_files=True,
        key="znom_upload",
    )
with h2:
    reestr_upload = st.file_uploader(
        "Реестр готовности (один файл)",
        type=["xlsx", "xlsm", "xls"],
        accept_multiple_files=False,
        key="reestr_upload",
    )
with h3:
    st.write("")
    st.write("")
    if st.button("Загрузить в базу", type="primary", use_container_width=True):
        if not znom_uploads and not reestr_upload:
            st.warning("Выберите хотя бы один файл: заявки или реестр.")
        else:
            parts = [(f.getvalue(), f.name) for f in znom_uploads] if znom_uploads else []
            reestr_tuple = (reestr_upload.getvalue(), reestr_upload.name) if reestr_upload else None
            try:
                stats = refresh_from_uploads(
                    settings.database_url,
                    parts,
                    reestr_tuple,
                    archive_missing=settings.archive_missing_as_inactive,
                    actor_user_id=user.get("id"),
                    actor_email=user.get("email") or None,
                    supabase_pooler_region=settings.supabase_pooler_region,
                )
                st.success(
                    f"Готово. ZNOM: {stats.znom_files_read} файлов ({stats.znom_files_failed} ошибок), "
                    f"REESTR: {stats.reestr_files_read} файл ({stats.reestr_files_failed} ошибок). "
                    f"Строк: +{stats.rows_added}, обновлено {stats.rows_updated}, архив {stats.rows_archived}."
                )
                # После импорта данные/колонки могут измениться; старое состояние грида иногда
                # ломает отрисовку или скрывает все колонки.
                st.session_state.pop("aggrid_columns_state", None)
                st.session_state.pop("aggrid_grid_state", None)
                st.session_state["grid_reset_counter"] = st.session_state.get("grid_reset_counter", 0) + 1
                for e in stats.errors:
                    st.warning(e)
            except Exception as ex:
                st.error(f"Ошибка импорта: {ex}")

row_filters = st.columns([1.75, 0.78])
with row_filters[0]:
    include_inactive = st.checkbox("Показывать архивные (is_active=0)", value=False)
    if not include_inactive:
        st.caption("По умолчанию показаны только активные заявки.")
with row_filters[1]:
    if st.button("Сбросить фильтры", type="secondary", use_container_width=True):
        st.session_state["pending_reset"] = True
        st.session_state["grid_reset_counter"] = st.session_state.get("grid_reset_counter", 0) + 1
        st.rerun()

df = get_grid_data(
    settings.database_url,
    include_inactive=include_inactive,
    supabase_pooler_region=settings.supabase_pooler_region,
)

if df.empty:
    st.info("База пуста. Загрузите файлы заявок (и при необходимости реестр), затем нажмите «Загрузить в базу».")
    st.stop()


def _parse_request_dates(series: pd.Series) -> pd.Series:
    """Поддерживает ISO и старые текстовые даты вида ДД.ММ.ГГ/ДД.ММ.ГГГГ."""
    s = series.astype("string").str.strip()
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    dotted_mask = s.str.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{2,4}", na=False)
    if dotted_mask.any():
        parsed.loc[dotted_mask] = pd.to_datetime(s.loc[dotted_mask], errors="coerce", dayfirst=True)
    needs_generic = parsed.isna() & s.notna() & (s != "")
    if needs_generic.any():
        parsed.loc[needs_generic] = pd.to_datetime(s.loc[needs_generic], errors="coerce")
    needs_dayfirst = parsed.isna() & s.notna() & (s != "")
    if needs_dayfirst.any():
        parsed.loc[needs_dayfirst] = pd.to_datetime(s.loc[needs_dayfirst], errors="coerce", dayfirst=True)
    return parsed


# ---- Диапазон дат по всему набору ----
date_series_all = _parse_request_dates(df["date_request"])
min_date_all = date_series_all.min().date() if date_series_all.notna().any() else None
max_date_all = date_series_all.max().date() if date_series_all.notna().any() else None

if min_date_all is None or max_date_all is None:
    min_date_all = pd.Timestamp.today().date()
    max_date_all = pd.Timestamp.today().date()

# Сброс фильтров (кнопка в шапке)
if st.session_state.get("pending_reset", False):
    st.session_state["date_range"] = (min_date_all, max_date_all)
    st.session_state["inp_f"] = min_date_all
    st.session_state["inp_t"] = max_date_all
    st.session_state["pending_reset"] = False
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and (
            k.startswith("fcb_st_") or k.startswith("fcb_au_") or k.startswith("fcb_sv_")
        ):
            del st.session_state[k]
    if "_fcb_date_sig" in st.session_state:
        del st.session_state["_fcb_date_sig"]
    for _k in ("aggrid_columns_state", "aggrid_grid_state"):
        st.session_state.pop(_k, None)
    for k in ("status_filter_sel", "author_filter_sel", "service_filter_sel"):
        if k in st.session_state:
            del st.session_state[k]

if "grid_reset_counter" not in st.session_state:
    st.session_state["grid_reset_counter"] = 0
if "date_range" not in st.session_state:
    st.session_state["date_range"] = (min_date_all, max_date_all)
if "inp_f" not in st.session_state:
    st.session_state["inp_f"] = st.session_state["date_range"][0]
if "inp_t" not in st.session_state:
    st.session_state["inp_t"] = st.session_state["date_range"][1]


def _clamp_session_date(value: object, fallback: object) -> object:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        ts = pd.to_datetime(fallback, errors="coerce")
    if pd.isna(ts):
        return min_date_all
    d = ts.date()
    if d < min_date_all:
        return min_date_all
    if d > max_date_all:
        return max_date_all
    return d


_date_range_raw = st.session_state.get("date_range", (min_date_all, max_date_all))
if not isinstance(_date_range_raw, (tuple, list)) or len(_date_range_raw) != 2:
    _date_range_raw = (min_date_all, max_date_all)
_dr0 = _clamp_session_date(_date_range_raw[0], min_date_all)
_dr1 = _clamp_session_date(_date_range_raw[1], max_date_all)
st.session_state["date_range"] = (min(_dr0, _dr1), max(_dr0, _dr1))
st.session_state["inp_f"] = _clamp_session_date(st.session_state.get("inp_f"), st.session_state["date_range"][0])
st.session_state["inp_t"] = _clamp_session_date(st.session_state.get("inp_t"), st.session_state["date_range"][1])


def _sync_inputs_to_slider() -> None:
    lo = st.session_state["inp_f"]
    hi = st.session_state["inp_t"]
    a, b = min(lo, hi), max(lo, hi)
    st.session_state["date_range"] = (a, b)


def _sync_slider_to_inputs() -> None:
    dr = st.session_state["date_range"]
    st.session_state["inp_f"] = dr[0]
    st.session_state["inp_t"] = dr[1]


st.caption(
    "Статусы, авторы и услуги — только по строкам в выбранном диапазоне дат "
    "(списки обновляются после смены дат)"
)

# Опции чекбоксов: строки внутри текущего date_range из session_state
_d0, _d1 = st.session_state["date_range"]
_lo = max(min(_d0, _d1), min_date_all)
_hi = min(max(_d0, _d1), max_date_all)
_ds_all = _parse_request_dates(df["date_request"])
_mask_opts = _ds_all.notna() & (_ds_all >= pd.Timestamp(_lo)) & (_ds_all <= pd.Timestamp(_hi))
df_for_opts = df.loc[_mask_opts].copy()

if df_for_opts.empty:
    status_options = []
    authors_all = []
    services_all = []
else:
    _st_ser = add_status_column(df_for_opts)
    status_options = sorted(_st_ser.unique().tolist())
    authors_all = sorted({_norm_author(x) for x in df_for_opts["author"].tolist()})
    _svc_col = df_for_opts["service"] if "service" in df_for_opts.columns else pd.Series(dtype=object)
    services_all = sorted({_norm_service(x) for x in _svc_col.tolist()})

_OPT_EMPTY = "(нет данных в периоде)"
if not status_options:
    status_options = [_OPT_EMPTY]
if not authors_all:
    authors_all = [_OPT_EMPTY]
if not services_all:
    services_all = [_OPT_EMPTY]

_real_status_opts = [x for x in status_options if x != _OPT_EMPTY]
_real_auth_opts = [x for x in authors_all if x != _OPT_EMPTY]
_real_service_opts = [x for x in services_all if x != _OPT_EMPTY]

# Сброс чекбоксов статусов/авторов только при смене периода (дат), не при обновлении данных
_dr_fcb = st.session_state["date_range"]
_fcb_date_sig = (_dr_fcb[0], _dr_fcb[1])
if st.session_state.get("_fcb_date_sig") != _fcb_date_sig:
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and (
            k.startswith("fcb_st_") or k.startswith("fcb_au_") or k.startswith("fcb_sv_")
        ):
            del st.session_state[k]
    st.session_state["_fcb_date_sig"] = _fcb_date_sig

row_d = st.columns([0.95, 0.95, 3.0])
with row_d[0]:
    st.date_input(
        "От",
        min_value=min_date_all,
        max_value=max_date_all,
        key="inp_f",
        on_change=_sync_inputs_to_slider,
    )
with row_d[1]:
    st.date_input(
        "До",
        min_value=min_date_all,
        max_value=max_date_all,
        key="inp_t",
        on_change=_sync_inputs_to_slider,
    )
with row_d[2]:
    st.slider(
        "Период",
        min_value=min_date_all,
        max_value=max_date_all,
        key="date_range",
        format="DD.MM.YYYY",
        on_change=_sync_slider_to_inputs,
    )

ex1, ex2, ex3 = st.columns(3)
with ex1:
    with st.expander("Услуги", expanded=False):
        if not _real_service_opts:
            st.caption(_OPT_EMPTY)
        else:
            for opt in _real_service_opts:
                fk = _fcb_key("fcb_sv_", opt)
                if fk not in st.session_state:
                    st.session_state[fk] = True
                c_l, c_r = st.columns([5, 1])
                with c_l:
                    st.markdown(f"<span style='font-size:0.92rem'>{opt}</span>", unsafe_allow_html=True)
                with c_r:
                    st.checkbox("вкл", key=fk, label_visibility="collapsed")
with ex2:
    with st.expander("Авторы", expanded=False):
        if not _real_auth_opts:
            st.caption(_OPT_EMPTY)
        else:
            for opt in _real_auth_opts:
                fk = _fcb_key("fcb_au_", opt)
                if fk not in st.session_state:
                    st.session_state[fk] = True
                c_l, c_r = st.columns([5, 1])
                with c_l:
                    st.markdown(f"<span style='font-size:0.92rem'>{opt}</span>", unsafe_allow_html=True)
                with c_r:
                    st.checkbox("вкл", key=fk, label_visibility="collapsed")
with ex3:
    with st.expander("Статусы", expanded=False):
        if not _real_status_opts:
            st.caption(_OPT_EMPTY)
        else:
            for opt in _real_status_opts:
                fk = _fcb_key("fcb_st_", opt)
                if fk not in st.session_state:
                    st.session_state[fk] = True
                c_l, c_r = st.columns([5, 1])
                with c_l:
                    st.markdown(f"<span style='font-size:0.92rem'>{opt}</span>", unsafe_allow_html=True)
                with c_r:
                    st.checkbox("вкл", key=fk, label_visibility="collapsed")

date_from, date_to = st.session_state["date_range"]

status_sel = {opt for opt in _real_status_opts if st.session_state.get(_fcb_key("fcb_st_", opt), False)}
author_sel = {opt for opt in _real_auth_opts if st.session_state.get(_fcb_key("fcb_au_", opt), False)}
service_sel = {opt for opt in _real_service_opts if st.session_state.get(_fcb_key("fcb_sv_", opt), False)}

# ---- Фильтрация ----
df_filtered = df.copy()
date_series = _parse_request_dates(df_filtered["date_request"])
start_dt = pd.to_datetime(date_from)
end_dt = pd.to_datetime(date_to)
mask = date_series.notna() & (date_series >= start_dt) & (date_series <= end_dt)
df_filtered = df_filtered.loc[mask].copy()

if df_filtered.empty:
    st.info("В выбранном диапазоне дат нет строк.")
    st.stop()

if not status_sel:
    st.info("Выберите хотя бы один статус.")
    st.stop()
if not author_sel:
    st.info("Выберите хотя бы одного автора.")
    st.stop()
if not service_sel:
    st.info("Выберите хотя бы одну услугу.")
    st.stop()

st_series = add_status_column(df_filtered)
df_filtered = df_filtered.loc[st_series.isin(status_sel)].copy()

if df_filtered.empty:
    st.info("Нет строк с выбранными статусами.")
    st.stop()

df_filtered["_author_norm"] = df_filtered["author"].map(_norm_author)
df_filtered = df_filtered.loc[df_filtered["_author_norm"].isin(author_sel)].drop(columns=["_author_norm"])

if df_filtered.empty:
    st.info("Нет строк с выбранными авторами.")
    st.stop()

if "service" not in df_filtered.columns:
    df_filtered["service"] = pd.NA
df_filtered["_service_norm"] = df_filtered["service"].map(_norm_service)
df_filtered = df_filtered.loc[df_filtered["_service_norm"].isin(service_sel)].drop(columns=["_service_norm"])

if df_filtered.empty:
    st.info("Нет строк с выбранными услугами.")
    st.stop()

filters_active_count = 0
if (date_from, date_to) != (min_date_all, max_date_all):
    filters_active_count += 1
if _real_status_opts and len(status_sel) < len(_real_status_opts):
    filters_active_count += 1
if _real_auth_opts and len(author_sel) < len(_real_auth_opts):
    filters_active_count += 1
if _real_service_opts and len(service_sel) < len(_real_service_opts):
    filters_active_count += 1

_FILTERS_CAPTION = f"Активных фильтров: {filters_active_count}"

show_cols = [
    "date_request",
    "request_no",
    "item_name",
    "client",
    "author",
    "qty_mp",
    "qty_bars",
    "moved_mp",
    "reserved_mp",
    "processed_mp",
    "processed_bars",
    "remaining",
    "exported",
    "correction",
    "note",
    "business_key",
]
grid = df_filtered[show_cols].copy()
grid["item_name"] = grid["item_name"].map(_strip_item_prefix)
grid["author"] = grid["author"].map(_author_initials)
grid["status"] = add_status_column(grid)
grid = grid[
    [
        "date_request",
        "request_no",
        "item_name",
        "client",
        "author",
        "status",
        "qty_mp",
        "qty_bars",
        "moved_mp",
        "reserved_mp",
        "processed_mp",
        "processed_bars",
        "remaining",
        "exported",
        "correction",
        "note",
        "business_key",
    ]
]
grid = grid.rename(
    columns={
        "date_request": "Дата",
        "request_no": "№",
        "item_name": "Наим.",
        "client": "Клиент",
        "author": "Автор",
        "status": "Статус",
        "qty_mp": "Сумм м.п.",
        "qty_bars": "Сумм хл.",
        "moved_mp": "Перем.",
        "reserved_mp": "Бронь",
        "processed_mp": "Обр. м.п.",
        "processed_bars": "Обр. хл.",
        "remaining": "Ост. выв.",
        "exported": "Вывезено",
        "correction": "Корр.",
        "note": "ПР.",
        "business_key": "_key",
    }
)

# Отображение даты как ДД.ММ.ГГГГ (например 24.11.2025)
_dt = _parse_request_dates(grid["Дата"])
grid["Дата"] = _dt.dt.strftime("%d.%m.%Y").where(_dt.notna(), "")

# ---- AgGrid table with column-header filters ----
exported_col = "Вывезено"
remaining_col = "Ост. выв."
corr_col = "Корр."
note_col = "ПР."
key_col = "_key"

df_grid = grid.copy()
df_grid[note_col] = (
    df_grid[note_col]
    .fillna("")
    .replace(["NaN", "nan", "None", "none", "NULL", "null"], "")
)
df_grid[exported_col] = pd.to_numeric(df_grid[exported_col], errors="coerce")
df_grid[corr_col] = pd.to_numeric(df_grid[corr_col], errors="coerce")

# Убираем артефакты float в ячейках (396.00000000000006 → 396)
_GRID_FLOAT_DECIMALS = 6
_NUM_ROUND_COLS = (
    "Сумм м.п.",
    "Сумм хл.",
    "Перем.",
    "Бронь",
    "Обр. м.п.",
    "Обр. хл.",
    exported_col,
    remaining_col,
    corr_col,
)
for _c in _NUM_ROUND_COLS:
    if _c in df_grid.columns:
        df_grid[_c] = pd.to_numeric(df_grid[_c], errors="coerce").round(_GRID_FLOAT_DECIMALS)

# Служебные поля для rowClassRules: в AG Grid строковые правила с data['Статус'] (кириллица) часто не срабатывают.
_ST_ROW_MAP = {"Готово": "g", "Вывезена": "v", "Забронировано": "b"}


def _st_row_code(v: object) -> str:
    if pd.isna(v):
        return ""
    return _ST_ROW_MAP.get(str(v).strip(), "")


df_grid["st_row"] = df_grid["Статус"].map(_st_row_code)
_rem = pd.to_numeric(df_grid[remaining_col], errors="coerce")
df_grid["rem_full"] = (_rem.notna() & (_rem <= 0)).astype(int)


def _row_business_key(row: pd.Series) -> str | None:
    """Сохраняем только по ключу из БД: display-значения в гриде могут быть преобразованы для UI."""
    raw = row.get(key_col)
    if raw is not None and not (isinstance(raw, float) and pd.isna(raw)):
        s = str(raw).strip()
        if s:
            return s
    return None


def _exported_vals_differ(a: float | None, b: float | None) -> bool:
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    return abs(a - b) > 1e-9


def _to_opt_float(x: object) -> float | None:
    if pd.isna(x):
        return None
    try:
        return float(x)
    except Exception:
        return None


def _to_opt_str(x: object) -> str | None:
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return None
    s = str(x).strip()
    return s or None


def _note_vals_differ(a: str | None, b: str | None) -> bool:
    return (a or "") != (b or "")


# Baseline: current editable values at the time of grid render (ключ = business_key из _key).
baseline_map: dict[str, tuple[float | None, float | None, str | None]] = {}
for _, r in df_grid.iterrows():
    k = _row_business_key(r)
    if not k:
        continue
    baseline_map[k] = (
        _to_opt_float(r.get(exported_col)),
        _to_opt_float(r.get(corr_col)),
        _to_opt_str(r.get(note_col)),
    )
st.session_state["exported_baseline_map"] = baseline_map
_pending_grid_save_toast = st.session_state.pop("grid_save_toast", None)
if _pending_grid_save_toast:
    st.toast(_pending_grid_save_toast)

st.caption(_FILTERS_CAPTION)

gb = GridOptionsBuilder.from_dataframe(df_grid)
gb.configure_selection("single")
gb.configure_default_column(
    filter=True,
    sortable=True,
    resizable=True,
    editable=False,
    minWidth=90,
)

status_col_name = "Статус"
date_col_name = "Дата"

# Сортировка по календарю, а не по строке «ДД.ММ.ГГГГ»
_date_cmp_js = JsCode(
    """
function (valueA, valueB, nodeA, nodeB, isDescending) {
    function toTime(v) {
        if (v == null || v === "") return null;
        var s = String(v).trim();
        var p = s.split(".");
        if (p.length !== 3) return null;
        var d = parseInt(p[0], 10);
        var m = parseInt(p[1], 10) - 1;
        var y = parseInt(p[2], 10);
        if (isNaN(d) || isNaN(m) || isNaN(y)) return null;
        var t = new Date(y, m, d).getTime();
        return isNaN(t) ? null : t;
    }
    var ta = toTime(valueA);
    var tb = toTime(valueB);
    if (ta == null && tb == null) return 0;
    if (ta == null) return 1;
    if (tb == null) return -1;
    return ta - tb;
}
"""
)

# Case-insensitive partial text filter for all visible columns
for col in df_grid.columns:
    if col == key_col:
        gb.configure_column(col, hide=True, editable=False)
        continue
    if col in ("st_row", "rem_full"):
        gb.configure_column(col, hide=True, editable=False)
        continue
    if col == status_col_name:
        continue
    if col == date_col_name:
        gb.configure_column(
            col,
            comparator=_date_cmp_js,
            filter="agTextColumnFilter",
            filterParams={"defaultOption": "contains", "caseSensitive": False},
            tooltipField=col,
            width=104,
            minWidth=96,
            maxWidth=120,
            flex=0,
            suppressSizeToFit=True,
            wrapText=False,
        )
        continue
    if col == "№":
        gb.configure_column(
            col,
            filter="agTextColumnFilter",
            filterParams={"defaultOption": "contains", "caseSensitive": False},
            tooltipField=col,
            width=92,
            minWidth=84,
            maxWidth=108,
            flex=0,
            suppressSizeToFit=True,
            wrapText=False,
        )
        continue
    if col == "Автор":
        gb.configure_column(
            col,
            filter="agTextColumnFilter",
            filterParams={"defaultOption": "contains", "caseSensitive": False},
            tooltipField=col,
            width=68,
            minWidth=58,
            maxWidth=84,
            flex=0,
            suppressSizeToFit=True,
            wrapText=False,
        )
        continue
    gb.configure_column(
        col,
        filter="agTextColumnFilter",
        filterParams={"defaultOption": "contains", "caseSensitive": False},
        tooltipField=col,
    )

if status_col_name in df_grid.columns:
    gb.configure_column(
        status_col_name,
        width=88,
        minWidth=72,
        maxWidth=120,
        flex=0,
        wrapText=False,
        suppressSizeToFit=True,
        filter="agTextColumnFilter",
        filterParams={"defaultOption": "contains", "caseSensitive": False},
        tooltipField=status_col_name,
    )

# Readability tuning: pin key identifiers and align numeric columns.
for _pin_col, _w in (("Дата", 104), ("№", 92), ("Клиент", 130)):
    if _pin_col in df_grid.columns:
        gb.configure_column(_pin_col, pinned="left", width=_w, minWidth=_w - 10, maxWidth=_w + 40, flex=0)

for _num_col, _w in (
    ("Сумм м.п.", 88),
    ("Сумм хл.", 82),
    ("Перем.", 82),
    ("Бронь", 82),
    ("Обр. м.п.", 88),
    ("Обр. хл.", 82),
    (remaining_col, 70),
    (exported_col, 82),
    (corr_col, 72),
):
    if _num_col in df_grid.columns:
        gb.configure_column(
            _num_col,
            width=_w,
            minWidth=_w - 12,
            maxWidth=_w + 20,
            flex=0,
            suppressSizeToFit=True,
            type=["numericColumn"],
            cellStyle={"textAlign": "right"},
        )

# Editable only "Вывезено"
gb.configure_column(exported_col, editable=True)
gb.configure_column(corr_col, editable=True, tooltipField=note_col)
gb.configure_column(
    note_col,
    editable=True,
    width=42,
    minWidth=36,
    maxWidth=52,
    flex=0,
    suppressSizeToFit=True,
    wrapText=False,
)

# Подсветка строк: rowClassRules со строковыми выражениями в этой связке часто не работают — используем getRowStyle (JsCode).
_row_style_js = JsCode(
    """
function (params) {
    var d = params.data;
    if (!d) return null;
    var st = d.st_row;
    var rem = d.rem_full;
    var remN = rem == null || rem === "" ? NaN : Number(rem);
    var bg = null;
    if (st === "g") bg = "rgba(200, 235, 205, 0.75)";
    else if (st === "v") bg = "rgba(180, 230, 255, 0.65)";
    else if (st === "b") bg = "rgba(255, 215, 170, 0.7)";
    else if (!isNaN(remN) && remN === 1) bg = "rgba(160, 220, 255, 0.45)";
    if (bg === null) return null;
    return { background: bg };
}
"""
)
gb.configure_grid_options(getRowStyle=_row_style_js)
gb.configure_grid_options(tooltipShowDelay=200)
gb.configure_grid_options(
    getRowId=JsCode(
        f"""
function(params) {{
    var row = params && params.data ? params.data : null;
    var key = row ? row["{key_col}"] : null;
    if (key == null || key === "") return undefined;
    return String(key);
}}
"""
    )
)

_grid_opts = gb.build()

grid_key = (
    f"grid_{st.session_state['grid_reset_counter']}_{date_from}_{date_to}"
    f"_{_filt_sig(status_sel)}_{_filt_sig(author_sel)}_{_filt_sig(service_sel)}"
)

grid_response = AgGrid(
    df_grid,
    gridOptions=_grid_opts,
    # Возвращаем данные только при изменении значения ячейки, без широкого MODEL_CHANGED.
    update_mode=GridUpdateMode.VALUE_CHANGED,
    editable=True,
    fit_columns_on_grid_load=True,
    height=720,
    key=grid_key,
    theme="light",
    custom_css={
        ".ag-cell": {"border-right": "1px solid #d0d7de"},
        ".ag-header-cell": {"border-right": "1px solid #b8c0c9"},
    },
    # Иначе после st.rerun() грид держит старые правки и не подставляет данные с сервера (статус, БД).
    server_sync_strategy="server_wins",
    allow_unsafe_jscode=True,
)

# Свойство .data — актуальные строки грида (в т.ч. после правок)
_edited = grid_response.data
if _edited is None:
    edited_rows = pd.DataFrame()
elif isinstance(_edited, pd.DataFrame):
    edited_rows = _edited
else:
    edited_rows = pd.DataFrame(_edited)
if not edited_rows.empty:
    baseline = st.session_state.get("exported_baseline_map", {})
    last_save_signature = st.session_state.get("last_grid_save_signature")

    changed_keys: list[str] = []
    new_values: dict[str, tuple[float | None, float | None, str | None]] = {}
    missing_key_rows = 0
    for _, row in edited_rows.iterrows():
        k = _row_business_key(row)
        if not k:
            missing_key_rows += 1
            continue
        new_exp = _to_opt_float(row.get(exported_col))
        new_corr = _to_opt_float(row.get(corr_col))
        new_note = _to_opt_str(row.get(note_col))
        old_exp, old_corr, old_note = baseline.get(k, (None, None, None))
        if (
            _exported_vals_differ(new_exp, old_exp)
            or _exported_vals_differ(new_corr, old_corr)
            or _note_vals_differ(new_note, old_note)
        ):
            changed_keys.append(k)
            new_values[k] = (new_exp, new_corr, new_note)

    if missing_key_rows:
        st.error("Не удалось определить ключ строки для части изменений. Обновите страницу и повторите ввод.")

    if changed_keys:
        save_signature = hashlib.md5(
            repr(sorted((k, new_values[k]) for k in changed_keys)).encode("utf-8")
        ).hexdigest()
        if save_signature == last_save_signature:
            changed_keys = []

    if changed_keys:
        conn = connect(
            settings.database_url,
            supabase_pooler_region=settings.supabase_pooler_region,
        )
        try:
            init_db(conn)
            for k in changed_keys:
                exp_v, corr_v, note_v = new_values[k]
                set_export_fields(
                    conn,
                    k,
                    exp_v,
                    corr_v,
                    note_v,
                    actor_user_id=user.get("id"),
                    actor_email=user.get("email") or None,
                )
            for k in changed_keys:
                baseline[k] = new_values[k]
            st.session_state["last_grid_save_signature"] = save_signature
            st.session_state["grid_save_toast"] = f"Автосохранено: {len(changed_keys)} изменений"
        finally:
            conn.close()
        st.rerun()
