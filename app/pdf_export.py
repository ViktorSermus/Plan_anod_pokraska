"""Экспорт отфильтрованной таблицы в PDF (кириллица через системный TTF)."""

from __future__ import annotations

import os
import platform
from io import BytesIO
from pathlib import Path

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.lib.styles import ParagraphStyle


_FONT_REGISTERED = False
FONT_NAME = "PlanExportSans"


def _find_cyrillic_font() -> Path | None:
    if platform.system() == "Windows":
        windir = Path(os.environ.get("WINDIR", r"C:\Windows"))
        for name in ("arial.ttf", "Arial.ttf", "arialuni.ttf", "calibri.ttf"):
            p = windir / "Fonts" / name
            if p.is_file():
                return p
    for p in (
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
    ):
        if p.is_file():
            return p
    return None


def _ensure_font() -> str:
    global _FONT_REGISTERED
    if _FONT_REGISTERED:
        return FONT_NAME
    fp = _find_cyrillic_font()
    if fp is None:
        raise FileNotFoundError(
            "Не найден TTF-шрифт с кириллицей (Arial/DejaVu). Установите шрифт или добавьте путь."
        )
    pdfmetrics.registerFont(TTFont(FONT_NAME, str(fp)))
    _FONT_REGISTERED = True
    return FONT_NAME


def _cell_txt(v: object, max_len: int = 48) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip().replace("\r", " ").replace("\n", " ")
    if len(s) > max_len:
        return s[: max_len - 1] + "…"
    return s


def build_filtered_plan_pdf(
    df: pd.DataFrame,
    period_from: object,
    period_to: object,
    title: str = "Производственный план (по текущим фильтрам)",
) -> bytes:
    """
    df — те же колонки, что в таблице на экране, без служебных полей (_key, st_row, rem_full).
    """
    _ensure_font()
    if df.empty:
        df = pd.DataFrame({"—": ["Нет строк для выгрузки"]})

    buf = BytesIO()
    page = landscape(A4)
    doc = SimpleDocTemplate(
        buf,
        pagesize=page,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
    )
    font = FONT_NAME
    story = []

    sub = f"Период: {period_from} — {period_to}    Строк: {len(df)}"
    story.append(
        Paragraph(
            f"<b>{_cell_txt(title, 120)}</b><br/><font size=8>{_cell_txt(sub, 160)}</font>",
            ParagraphStyle(
                name="T",
                fontName=font,
                fontSize=10,
                leading=12,
                textColor=colors.HexColor("#222222"),
            ),
        )
    )
    story.append(Spacer(1, 4 * mm))

    headers = [_cell_txt(c, 80) for c in df.columns.tolist()]
    rows_data = [headers]
    for _, row in df.iterrows():
        rows_data.append([_cell_txt(x, 52) for x in row.tolist()])

    col_count = len(headers)
    total_w = float(page[0] - 24 * mm)
    col_w = total_w / max(col_count, 1)

    tbl = Table(rows_data, colWidths=[col_w] * col_count, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font),
                ("FONTSIZE", (0, 0), (-1, -1), 6),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8e8e8")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f9f9f9")]),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    story.append(tbl)

    doc.build(story)
    return buf.getvalue()
