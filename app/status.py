"""
Статусы строк плана: иерархия вывоз → обработка → бронь → перемещение → новая.
Единицы: м.п. для заявки/перемещения/брони/обработки; хлысты для вывезено vs обработано в хлыстах.
"""

from __future__ import annotations

import math

import pandas as pd

# Допуск для сравнений «равно» (округление Excel / float)
STATUS_EPS_MP = 0.01
STATUS_EPS_BARS = 0.01


def _f(x: object) -> float:
    if pd.isna(x):
        return math.nan
    try:
        return float(x)
    except (TypeError, ValueError):
        return math.nan


def _z(x: float, eps: float) -> bool:
    """Нет этапа: NaN или ~0."""
    return pd.isna(x) or abs(x) <= eps


def _approx(a: float, b: float, eps: float) -> bool:
    if pd.isna(a) or pd.isna(b):
        return False
    return abs(a - b) <= eps


def compute_status_row(row: pd.Series) -> str:
    """
    Ожидаемые поля: qty_mp, qty_bars, moved_mp, reserved_mp, processed_mp, processed_bars, exported, correction.
    """
    qmp = _f(row.get("qty_mp"))
    qbars = _f(row.get("qty_bars"))
    moved = _f(row.get("moved_mp"))
    res = _f(row.get("reserved_mp"))
    proc = _f(row.get("processed_mp"))
    exp = _f(row.get("exported"))
    corr = _f(row.get("correction"))

    emp = STATUS_EPS_MP
    eb = STATUS_EPS_BARS
    exp_total = (0.0 if _z(exp, eb) else exp) + (0.0 if _z(corr, eb) else corr)

    has_order = (not pd.isna(qmp) and qmp > emp) or (not pd.isna(qbars) and qbars > eb)
    if not has_order:
        return "—"

    # Сравнения по заявке в м.п. — без валидного кол-ва м.п. классификация невозможна
    if pd.isna(qmp) or qmp <= emp:
        return "—"

    # --- 1) Вывоз (хлысты): сравнение с количеством хлыстов по заявке (qty_bars) ---
    if not _z(exp_total, eb):
        if not _z(qbars, eb):
            if _approx(exp_total, qbars, eb):
                return "Вывезена"
            if exp_total + eb < qbars:
                return "Частично вывезено"
            if exp_total > qbars + eb:
                return "Вывезена"

    # --- 2) Обработка (м.п.) ---
    if not _z(proc, emp):
        if _approx(proc, qmp, emp):
            return "Готово"
        if proc + emp < qmp:
            return "Частично готово"

    # --- 3) Бронь ---
    if not _z(res, emp) and _approx(res, qmp, emp) and _z(proc, emp) and _z(exp_total, eb):
        return "Забронировано"

    # --- 4) Перемещение (м.п., без брони/обработки/вывоза по определению 2–3) ---
    if not _z(moved, emp) and _z(res, emp) and _z(proc, emp) and _z(exp_total, eb):
        if _approx(moved, qmp, emp):
            return "У переработчика"
        if emp < moved < qmp - emp:
            return "Частично у переработчика"

    # --- 5) Новая ---
    if _z(moved, emp) and _z(res, emp) and _z(proc, emp) and _z(exp_total, eb):
        return "Новая"

    return "—"


def add_status_column(df: pd.DataFrame) -> pd.Series:
    return df.apply(compute_status_row, axis=1)
