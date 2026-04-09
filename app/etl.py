from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pandas as pd

KEY_COLUMNS = ["№ заявки", "Дата заявки", "Наименование"]


@dataclass
class EtlResult:
    dataframe: pd.DataFrame
    files_read: int
    files_failed: int
    errors: list[str]


def _decode_mojibake(v: object) -> object:
    if not isinstance(v, str):
        return v
    try:
        return v.encode("latin1").decode("cp1251")
    except Exception:
        return v


def _extract_client_from_note(cell: object) -> str | None:
    """
    Ячейка с текстом «Примечания: <клиент>_<...>» — клиент не содержит «_».
    Неразрывные пробелы из Excel приводятся к обычному пробелу.
    """
    if pd.isna(cell):
        return None
    s = str(cell).strip().replace("\u00a0", " ").replace("\u2007", " ")
    if not s:
        return None
    low = s.lower()
    if not (low.startswith("примечания") or "примечания:" in low):
        return None
    m = re.search(r"Примечания:\s*(.+?)_", s, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    out = m.group(1).strip()
    return out if out else None


def _find_client_note_in_sheet(txt: pd.DataFrame, max_rows: int = 45, max_cols: int = 30) -> str | None:
    """Ищет примечание в любой ячейке верхней зоны (не только в колонке A)."""
    nrows = min(len(txt), max_rows)
    ncols = min(txt.shape[1], max_cols)
    for i in range(nrows):
        for j in range(ncols):
            client = _extract_client_from_note(txt.iloc[i, j])
            if client:
                return client
    return None


def _normalize_author(author: str | None) -> str | None:
    """Сохраняем полное ФИО из источника (без урезания до фамилии)."""
    if not author:
        return None
    s = str(author).strip()
    if not s:
        return None
    return s


def _norm_text(v: object) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip().lower()


def build_business_key(df: pd.DataFrame) -> pd.Series:
    parts = (
        df["№ заявки"].map(_norm_text)
        + "|"
        + df["Дата заявки"].map(lambda x: str(pd.to_datetime(x, errors="coerce").date()) if not pd.isna(x) else "")
        + "|"
        + df["Наименование"].map(_norm_text)
    )
    return parts.map(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())


def _scan_meta_from_raw_header(raw: pd.DataFrame) -> tuple[str | None, str | None]:
    """Строки вида «Услуга: …», «Автор: …» в верхней части листа (первые ~40 строк)."""
    service = None
    author = None
    for i in range(min(len(raw), 40)):
        row_vals = [str(x) for x in raw.iloc[i].tolist() if not pd.isna(x)]
        if not row_vals:
            continue
        row_join = " ".join(row_vals)
        low = row_join.lower()
        if service is None and "услуга" in low:
            service = row_join.split(":", 1)[-1].strip()
        if author is None and "автор" in low:
            author = row_join.split(":", 1)[-1].strip() if ":" in row_join else row_join.strip()
    return service, author


def _extract_meta_lines_from_excel_top(path: Path) -> tuple[str | None, str | None]:
    """Строки вида «Услуга: …», «Автор: …» в верхней части листа (в т.ч. xlsx без legacy-парсера)."""
    try:
        raw = pd.read_excel(path, engine="openpyxl", header=None, nrows=40)
    except Exception:
        return None, None
    return _scan_meta_from_raw_header(raw)


def _extract_meta_lines_from_excel_bytes(data: bytes) -> tuple[str | None, str | None]:
    try:
        raw = pd.read_excel(BytesIO(data), engine="openpyxl", header=None, nrows=40)
    except Exception:
        return None, None
    return _scan_meta_from_raw_header(raw)


def _read_xls_raw(source: Path | BytesIO) -> pd.DataFrame:
    """
    Сырой лист .xls (header=None). Сначала calamine (python-calamine), затем xlrd —
    чтобы не зависеть от одного движка и типичных сбоев установки.
    """
    last: Exception | None = None
    for engine in ("calamine", "xlrd"):
        try:
            if isinstance(source, Path):
                return pd.read_excel(source, engine=engine, header=None)
            source.seek(0)
            return pd.read_excel(source, engine=engine, header=None)
        except Exception as e:
            last = e
            continue
    assert last is not None
    raise last


def _read_excel_safe(path: Path) -> pd.DataFrame | None:
    try:
        if path.suffix.lower() == ".xls":
            raw = _read_xls_raw(path)
            return _parse_legacy_znom_xls(raw)
        return pd.read_excel(path, engine="openpyxl")
    except Exception:
        return None


def read_excel_bytes(data: bytes, name: str) -> pd.DataFrame | None:
    """Чтение Excel из памяти (загрузка в Streamlit). `name` — имя файла с расширением."""
    suffix = Path(name).suffix.lower()
    try:
        if suffix == ".xls":
            raw = _read_xls_raw(BytesIO(data))
            return _parse_legacy_znom_xls(raw)
        return pd.read_excel(BytesIO(data), engine="openpyxl")
    except Exception:
        return None


def _parse_legacy_znom_xls(raw: pd.DataFrame) -> pd.DataFrame:
    # Decode cp1251 text which may be exposed as latin1 mojibake.
    txt = raw.copy().applymap(_decode_mojibake)

    header_idx: int | None = None
    for i in range(min(len(txt), 40)):
        row = [str(x) for x in txt.iloc[i].tolist() if not pd.isna(x)]
        row_join = " | ".join(row).lower()
        if "наименование" in row_join and ("коли" in row_join or "кол-во" in row_join):
            header_idx = i
            break
    if header_idx is None:
        return pd.DataFrame()

    request_no = None
    request_date = None
    service = None
    color_mark = None
    author = None

    for i in range(min(len(txt), 20)):
        row_vals = [str(x) for x in txt.iloc[i].tolist() if not pd.isna(x)]
        row_join = " ".join(row_vals)
        if request_no is None:
            m = re.search(r"[№N]\s*0*([0-9]{3,})", row_join)
            if m:
                request_no = m.group(1).zfill(9)
        if request_date is None:
            d = re.search(r"(\d{2}\.\d{2}\.\d{4})", row_join)
            if d:
                request_date = pd.to_datetime(d.group(1), dayfirst=True, errors="coerce")
        if service is None and "услуга" in row_join.lower():
            service = row_join.split(":", 1)[-1].strip()
        if color_mark is None and "цвет" in row_join.lower():
            color_mark = row_join.strip()
        if author is None and "автор" in row_join.lower():
            # Typical pattern: "Автор: <value>"
            author = row_join.split(":", 1)[-1].strip() if ":" in row_join else row_join.strip()

    client = _find_client_note_in_sheet(txt)

    data = txt.iloc[header_idx + 1 :].copy()
    data = data.iloc[:, :12]
    data.columns = [
        "№ п.п.",
        "Наименование",
        "Кол-во м.п.",
        "Хлысты",
        "Периметр",
        "Вес м.п.",
        "Площадь",
        "Вес кг",
        "расход краски",
        "Цена м.п.",
        "Цена м2",
        "Сумма, руб.",
    ]

    # Keep real detail rows only.
    data = data.dropna(how="all")
    if "Наименование" in data.columns:
        data = data[data["Наименование"].notna()]
        data = data[~data["Наименование"].astype(str).str.contains("ИТОГО", case=False, na=False)]
        data = data[~data["Наименование"].astype(str).str.contains("вес упаковочного", case=False, na=False)]

    # Leave only detail rows with numeric item index.
    data["№ п.п."] = pd.to_numeric(data["№ п.п."], errors="coerce")
    data = data[data["№ п.п."].notna()]

    # In source forms "Кол-во м.п." is often stored as x1000 (e.g., 245000 instead of 245).
    data["Кол-во м.п."] = pd.to_numeric(data["Кол-во м.п."], errors="coerce")
    if data["Кол-во м.п."].dropna().median() > 10000:
        data["Кол-во м.п."] = data["Кол-во м.п."] / 1000.0
    data = data[data["Кол-во м.п."].notna() & (data["Кол-во м.п."] > 0)]

    data["№ заявки"] = request_no
    data["Дата заявки"] = request_date
    data["Column1"] = color_mark
    data["Услуга"] = service
    data["Автор"] = _normalize_author(author)
    data["Клиент"] = client
    return data


def _collect_files(folder: Path, patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in patterns:
        files.extend(folder.rglob(pattern))
    unique = sorted(set(files))
    return [p for p in unique if p.is_file()]


def load_znom_folder(folder: Path, patterns: list[str]) -> EtlResult:
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    files = _collect_files(folder, patterns)
    failed = 0

    for f in files:
        df = _read_excel_safe(f)
        if df is None:
            failed += 1
            errors.append(f"ZNOM: skip {f.name} (cannot read)")
            continue
        if not df.empty:
            df = df.copy()
            if "Услуга" not in df.columns:
                df["Услуга"] = pd.NA
            _svc_empty = df["Услуга"].isna().all()
            if not _svc_empty:
                _svc_empty = bool((df["Услуга"].astype(str).str.strip() == "").all())
            if _svc_empty:
                svc, _ = _extract_meta_lines_from_excel_top(f)
                if svc:
                    df["Услуга"] = svc
        df["Source.Name"] = f.name
        frames.append(df)

    if not frames:
        return EtlResult(pd.DataFrame(), len(files), failed, errors)

    out = pd.concat(frames, ignore_index=True)
    return EtlResult(out, len(files), failed, errors)


def load_znom_uploads(items: list[tuple[bytes, str]]) -> EtlResult:
    """Несколько файлов заявок (ZNOM), переданных как (bytes, имя_файла)."""
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    failed = 0
    if not items:
        return EtlResult(pd.DataFrame(), 0, 0, errors)

    for data, name in items:
        df = read_excel_bytes(data, name)
        if df is None:
            failed += 1
            errors.append(f"ZNOM: skip {name} (cannot read)")
            continue
        if not df.empty:
            df = df.copy()
            if "Услуга" not in df.columns:
                df["Услуга"] = pd.NA
            _svc_empty = df["Услуга"].isna().all()
            if not _svc_empty:
                _svc_empty = bool((df["Услуга"].astype(str).str.strip() == "").all())
            if _svc_empty:
                svc, _ = _extract_meta_lines_from_excel_bytes(data) if Path(name).suffix.lower() != ".xls" else (None, None)
                if svc:
                    df["Услуга"] = svc
        df["Source.Name"] = name
        frames.append(df)

    if not frames:
        return EtlResult(pd.DataFrame(), len(items), failed, errors)

    out = pd.concat(frames, ignore_index=True)
    return EtlResult(out, len(items), failed, errors)


def load_reestr_upload(data: bytes, name: str) -> EtlResult:
    """Один файл реестра готовности."""
    errors: list[str] = []
    suffix = Path(name).suffix.lower()
    if suffix == ".xls":
        try:
            raw = _read_xls_raw(BytesIO(data))
            df = _parse_legacy_reestr_xls(raw)
        except Exception:
            return EtlResult(pd.DataFrame(), 1, 1, [f"REESTR: skip {name} (cannot read)"])
    else:
        df = read_excel_bytes(data, name)
        if df is None:
            return EtlResult(pd.DataFrame(), 1, 1, [f"REESTR: skip {name} (cannot read)"])
    df = df.copy()
    df["Source.Name"] = name
    return EtlResult(df, 1, 0, errors)


def load_latest_reestr(folder: Path, patterns: list[str]) -> EtlResult:
    files = _collect_files(folder, patterns)
    errors: list[str] = []
    if not files:
        return EtlResult(pd.DataFrame(), 0, 0, errors)

    latest = max(files, key=lambda p: p.stat().st_mtime)
    df = _read_excel_safe(latest)
    if df is None:
        return EtlResult(pd.DataFrame(), 1, 1, [f"REESTR: skip {latest.name} (cannot read)"])
    if latest.suffix.lower() == ".xls":
        raw = _read_xls_raw(latest)
        df = _parse_legacy_reestr_xls(raw)
    df["Source.Name"] = latest.name
    return EtlResult(df, 1, 0, errors)


def _parse_legacy_reestr_xls(raw: pd.DataFrame) -> pd.DataFrame:
    txt = raw.copy().applymap(_decode_mojibake)
    rows: list[dict[str, object]] = []

    current_no: str | None = None
    current_date = pd.NaT

    for i in range(len(txt)):
        r = txt.iloc[i].tolist()
        c0 = "" if pd.isna(r[0]) else str(r[0]).strip()
        c1 = "" if pd.isna(r[1]) else str(r[1]).strip()

        # Block header: "000000602 от 01.11.25"
        m = re.search(r"(\d{6,9}).*(\d{2}\.\d{2}\.\d{2,4})", c0)
        if m:
            current_no = m.group(1).zfill(9)
            current_date = pd.to_datetime(m.group(2), dayfirst=True, errors="coerce")
            continue

        # Detail row under current order block.
        if not current_no:
            continue
        if not c1:
            continue
        # In detail rows name typically starts with item code in square brackets.
        if not c1.startswith("["):
            continue

        qty = pd.to_numeric(r[2], errors="coerce")
        moved = pd.to_numeric(r[6], errors="coerce")
        reserve = pd.to_numeric(r[11], errors="coerce")
        processed = pd.to_numeric(r[12], errors="coerce")
        # Skip non-data lines.
        if pd.isna(qty) and pd.isna(moved) and pd.isna(reserve) and pd.isna(processed):
            continue

        rows.append(
            {
                "№ заявки": current_no,
                "Дата заявки": current_date,
                "Наименование": c1,
                "Кол-во м.п. по ЗНОМ": qty,
                "Перемещено": moved,
                "Бронь под обр": reserve,
                "Обработано": processed,
            }
        )

    return pd.DataFrame(rows)


def transform_master(znom_df: pd.DataFrame, reestr_df: pd.DataFrame) -> pd.DataFrame:
    if znom_df.empty:
        return pd.DataFrame(
            columns=[
                "Дата заявки",
                "№ заявки",
                "Наименование",
                "Услуга",
                "Автор",
                "Клиент",
                "Сумма по полю Кол-во м.п.",
                "Сумма по полю Хлысты",
                "Перемещено м.п.",
                "Бронь под обработку м.п.",
                "Обработано м.п.",
                "Кол-во хлыстов обработанных",
                "Вывезено",
                "Осталось вывезти",
                "business_key",
            ]
        )

    z = znom_df.copy()
    needed = [
        "Дата заявки",
        "№ заявки",
        "Наименование",
        "Кол-во м.п.",
        "Хлысты",
        "Услуга",
        "Автор",
        "Клиент",
    ]
    for col in needed:
        if col not in z.columns:
            z[col] = pd.NA

    grouped = (
        z.groupby(["Дата заявки", "№ заявки", "Наименование"], dropna=False, as_index=False)
        .agg(
            {
                "Кол-во м.п.": "sum",
                "Хлысты": "sum",
                "Услуга": "first",
                "Автор": "first",
                "Клиент": "first",
            }
        )
        .rename(columns={"Кол-во м.п.": "Сумма по полю Кол-во м.п.", "Хлысты": "Сумма по полю Хлысты"})
    )

    if not reestr_df.empty:
        r = reestr_df.copy()
        for col in ["№ заявки", "Дата заявки", "Наименование", "Перемещено", "Бронь под обр", "Обработано"]:
            if col not in r.columns:
                r[col] = pd.NA
        r_small = r[["№ заявки", "Дата заявки", "Наименование", "Перемещено", "Бронь под обр", "Обработано"]].copy()
        merged = grouped.merge(r_small, how="left", on=["№ заявки", "Дата заявки", "Наименование"])
    else:
        merged = grouped.copy()
        merged["Перемещено"] = pd.NA
        merged["Бронь под обр"] = pd.NA
        merged["Обработано"] = pd.NA

    merged = merged.rename(
        columns={
            "Перемещено": "Перемещено м.п.",
            "Бронь под обр": "Бронь под обработку м.п.",
            "Обработано": "Обработано м.п.",
        }
    )

    for num_col in [
        "Сумма по полю Кол-во м.п.",
        "Сумма по полю Хлысты",
        "Перемещено м.п.",
        "Бронь под обработку м.п.",
        "Обработано м.п.",
    ]:
        merged[num_col] = pd.to_numeric(merged[num_col], errors="coerce")

    ratio = merged["Сумма по полю Кол-во м.п."] / merged["Сумма по полю Хлысты"]
    merged["Кол-во хлыстов обработанных"] = merged["Обработано м.п."] / ratio
    merged["Вывезено"] = pd.NA
    merged["Осталось вывезти"] = merged["Кол-во хлыстов обработанных"] - pd.to_numeric(merged["Вывезено"], errors="coerce")
    merged["business_key"] = build_business_key(merged)
    return merged
