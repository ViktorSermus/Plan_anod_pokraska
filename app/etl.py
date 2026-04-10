from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from io import BytesIO, StringIO
from pathlib import Path
import xml.etree.ElementTree as ET

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


def _map_dataframe_cells(df: pd.DataFrame, func) -> pd.DataFrame:
    """Совместимость с pandas 2/3: applymap удалён, используем DataFrame.map если доступен."""
    mapper = getattr(df, "map", None)
    if callable(mapper):
        return mapper(func)
    return df.applymap(func)


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


def _decode_web_bytes(data: bytes) -> str | None:
    if not data:
        return None
    if data[:3] == b"\xef\xbb\xbf":
        return data.decode("utf-8-sig", errors="replace")
    if len(data) >= 2 and data[:2] == b"\xff\xfe":
        return data.decode("utf-16-le", errors="replace")
    if len(data) >= 2 and data[:2] == b"\xfe\xff":
        return data.decode("utf-16-be", errors="replace")
    for enc in ("utf-8", "cp1251", "windows-1251", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="replace")


def _pick_best_raw_table(dfs: list[pd.DataFrame]) -> pd.DataFrame | None:
    if not dfs:
        return None
    scored: list[tuple[int, pd.DataFrame]] = []
    for d in dfs:
        if d is None or d.empty:
            continue
        flat = " ".join(str(x) for x in d.values.ravel().tolist()).lower()
        score = min(d.shape[0] * d.shape[1], 8000)
        if "наименование" in flat:
            score += 500
        if "кол" in flat:
            score += 50
        scored.append((score, d))
    if not scored:
        return None
    scored.sort(key=lambda x: -x[0])
    return scored[0][1]


def _read_html_tables_as_raw(data: bytes) -> pd.DataFrame:
    text = _decode_web_bytes(data)
    if text is None:
        raise ValueError("empty")
    last: Exception | None = None
    for flavor in ("lxml", None):
        try:
            kwargs: dict = {"header": None}
            if flavor:
                kwargs["flavor"] = flavor
            dfs = pd.read_html(StringIO(text), **kwargs)
        except Exception as e:
            last = e
            continue
        best = _pick_best_raw_table(dfs)
        if best is not None and not best.empty:
            return best
    assert last is not None
    raise last


def _try_read_excel2003_xml_as_raw(data: bytes) -> pd.DataFrame | None:
    head = data[:512].lstrip()
    if not head.startswith(b"<?xml"):
        return None
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return None
    ns = "urn:schemas-microsoft-com:office:spreadsheet"
    q = lambda t: f"{{{ns}}}{t}"
    rows_el = root.findall(f".//{q('Row')}")
    if not rows_el:
        return None
    grid: list[list[object]] = []
    for row in rows_el:
        cells = row.findall(q("Cell"))
        if not cells:
            grid.append([])
            continue
        cols: dict[int, object] = {}
        next_col = 1
        for cell in cells:
            for ak, av in cell.attrib.items():
                if ak == "Index" or ak.endswith("}Index"):
                    next_col = int(av)
                    break
            data_el = cell.find(q("Data"))
            val: object = ""
            if data_el is not None:
                val = data_el.text if data_el.text is not None else ""
            cols[next_col] = val
            next_col += 1
        if not cols:
            grid.append([])
            continue
        max_c = max(cols.keys())
        row_list: list[object] = [""] * max_c
        for i, v in cols.items():
            row_list[i - 1] = v
        grid.append(row_list)
    if not grid:
        return None
    w = max(len(r) for r in grid)
    for r in grid:
        while len(r) < w:
            r.append("")
    return pd.DataFrame(grid)


def _read_legacy_excel_bytes_as_raw(data: bytes) -> pd.DataFrame:
    """
    Содержимое с расширением .xls: настоящий BIFF, либо xlsx под видом .xls,
    HTML-таблица, либо XML Spreadsheet 2003.
    """
    if not data:
        raise ValueError("empty file")
    if data[:2] == b"PK":
        return pd.read_excel(BytesIO(data), engine="openpyxl", header=None)
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return _read_xls_raw(BytesIO(data))
    sample = data[: min(len(data), 65536)]
    low = sample.lower()
    if b"<table" in low or b"<html" in sample[:4000].lower() or b"<!doctype html" in low[:2000]:
        return _read_html_tables_as_raw(data)
    head = data[:4096].lstrip()
    if head.startswith(b"<?xml"):
        xml_df = _try_read_excel2003_xml_as_raw(data)
        if xml_df is not None and not xml_df.empty:
            return xml_df
    try:
        return _read_xls_raw(BytesIO(data))
    except Exception:
        pass
    try:
        return pd.read_excel(BytesIO(data), engine="openpyxl", header=None)
    except Exception:
        pass
    return _read_html_tables_as_raw(data)


def _format_read_error(exc: Exception) -> str:
    msg = str(exc).strip().replace("\n", " ")
    msg = re.sub(r"\s+", " ", msg)
    if msg:
        if len(msg) > 180:
            msg = msg[:177] + "..."
        return f"{exc.__class__.__name__}: {msg}"
    return exc.__class__.__name__


def _read_excel_safe_with_error(path: Path) -> tuple[pd.DataFrame | None, str | None]:
    try:
        if path.suffix.lower() == ".xls":
            raw = _read_legacy_excel_bytes_as_raw(path.read_bytes())
            return _parse_legacy_znom_xls(raw)
        return pd.read_excel(path, engine="openpyxl"), None
    except Exception as exc:
        return None, _format_read_error(exc)


def _read_excel_safe(path: Path) -> pd.DataFrame | None:
    df, _ = _read_excel_safe_with_error(path)
    return df


def read_excel_bytes_with_error(data: bytes, name: str) -> tuple[pd.DataFrame | None, str | None]:
    """Чтение Excel из памяти (загрузка в Streamlit). `name` — имя файла с расширением."""
    suffix = Path(name).suffix.lower()
    try:
        if suffix == ".xls":
            raw = _read_legacy_excel_bytes_as_raw(data)
            return _parse_legacy_znom_xls(raw)
        return pd.read_excel(BytesIO(data), engine="openpyxl"), None
    except Exception as exc:
        return None, _format_read_error(exc)


def read_excel_bytes(data: bytes, name: str) -> pd.DataFrame | None:
    df, _ = read_excel_bytes_with_error(data, name)
    return df


def _parse_legacy_znom_xls(raw: pd.DataFrame) -> pd.DataFrame:
    # Decode cp1251 text which may be exposed as latin1 mojibake.
    txt = _map_dataframe_cells(raw.copy(), _decode_mojibake)

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
        df, err = _read_excel_safe_with_error(f)
        if df is None:
            failed += 1
            details = f": {err}" if err else ""
            errors.append(f"ZNOM: skip {f.name} (cannot read{details})")
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
        df, err = read_excel_bytes_with_error(data, name)
        if df is None:
            failed += 1
            details = f": {err}" if err else ""
            errors.append(f"ZNOM: skip {name} (cannot read{details})")
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
            raw = _read_legacy_excel_bytes_as_raw(data)
            df = _parse_legacy_reestr_xls(raw)
        except Exception as exc:
            err = _format_read_error(exc)
            return EtlResult(pd.DataFrame(), 1, 1, [f"REESTR: skip {name} (cannot read: {err})"])
    else:
        df, err = read_excel_bytes_with_error(data, name)
        if df is None:
            details = f": {err}" if err else ""
            return EtlResult(pd.DataFrame(), 1, 1, [f"REESTR: skip {name} (cannot read{details})"])
    df = df.copy()
    df["Source.Name"] = name
    return EtlResult(df, 1, 0, errors)


def load_latest_reestr(folder: Path, patterns: list[str]) -> EtlResult:
    files = _collect_files(folder, patterns)
    errors: list[str] = []
    if not files:
        return EtlResult(pd.DataFrame(), 0, 0, errors)

    latest = max(files, key=lambda p: p.stat().st_mtime)
    if latest.suffix.lower() == ".xls":
        try:
            raw = _read_legacy_excel_bytes_as_raw(latest.read_bytes())
            df = _parse_legacy_reestr_xls(raw)
        except Exception as exc:
            err = _format_read_error(exc)
            return EtlResult(pd.DataFrame(), 1, 1, [f"REESTR: skip {latest.name} (cannot read: {err})"])
    else:
        df, err = _read_excel_safe_with_error(latest)
        if df is None:
            details = f": {err}" if err else ""
            return EtlResult(pd.DataFrame(), 1, 1, [f"REESTR: skip {latest.name} (cannot read{details})"])
    df["Source.Name"] = latest.name
    return EtlResult(df, 1, 0, errors)


def _parse_legacy_reestr_xls(raw: pd.DataFrame) -> pd.DataFrame:
    txt = _map_dataframe_cells(raw.copy(), _decode_mojibake)
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
