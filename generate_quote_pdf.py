"""
generate_quote_pdf.py
CotizaExpress — Generador de cotizaciones en PDF
Uso:
    from generate_quote_pdf import build_quote_pdf
    pdf_bytes = build_quote_pdf(company, items, client_phone, folio)
"""

import io
import random
import string
from datetime import datetime
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)
from reportlab.pdfgen import canvas


# ── Paleta de colores ────────────────────────────────────────────────────────
COLOR_PRIMARY = colors.HexColor("#1a6b3a")   # verde ferrería
COLOR_LIGHT   = colors.HexColor("#e8f5ee")
COLOR_GRAY    = colors.HexColor("#666666")
COLOR_BLACK   = colors.HexColor("#1a1a1a")
COLOR_WHITE   = colors.white


# ── Folio ────────────────────────────────────────────────────────────────────
def generate_folio() -> str:
    """Genera folio random: CX-XXXXXX  (6 chars alfanuméricos mayúsculas)"""
    chars = string.ascii_uppercase + string.digits
    suffix = "".join(random.choices(chars, k=6))
    return f"CX-{suffix}"


# ── Helpers de estilo ────────────────────────────────────────────────────────
def _styles():
    base = getSampleStyleSheet()

    custom = {
        "company_name": ParagraphStyle(
            "company_name",
            fontSize=16,
            fontName="Helvetica-Bold",
            textColor=COLOR_PRIMARY,
            leading=20,
        ),
        "company_sub": ParagraphStyle(
            "company_sub",
            fontSize=8,
            fontName="Helvetica",
            textColor=COLOR_GRAY,
            leading=12,
        ),
        "folio_label": ParagraphStyle(
            "folio_label",
            fontSize=8,
            fontName="Helvetica",
            textColor=COLOR_GRAY,
            alignment=TA_RIGHT,
        ),
        "folio_value": ParagraphStyle(
            "folio_value",
            fontSize=14,
            fontName="Helvetica-Bold",
            textColor=COLOR_PRIMARY,
            alignment=TA_RIGHT,
        ),
        "section_title": ParagraphStyle(
            "section_title",
            fontSize=9,
            fontName="Helvetica-Bold",
            textColor=COLOR_GRAY,
            spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "body",
            fontSize=9,
            fontName="Helvetica",
            textColor=COLOR_BLACK,
            leading=14,
        ),
        "body_bold": ParagraphStyle(
            "body_bold",
            fontSize=9,
            fontName="Helvetica-Bold",
            textColor=COLOR_BLACK,
            leading=14,
        ),
        "total_label": ParagraphStyle(
            "total_label",
            fontSize=11,
            fontName="Helvetica-Bold",
            textColor=COLOR_WHITE,
            alignment=TA_RIGHT,
        ),
        "total_value": ParagraphStyle(
            "total_value",
            fontSize=13,
            fontName="Helvetica-Bold",
            textColor=COLOR_WHITE,
            alignment=TA_RIGHT,
        ),
        "footer": ParagraphStyle(
            "footer",
            fontSize=7,
            fontName="Helvetica",
            textColor=COLOR_GRAY,
            alignment=TA_CENTER,
        ),
    }
    return custom


# ── Función principal ─────────────────────────────────────────────────────────
def build_quote_pdf(
    company: dict,
    items: list[dict],
    client_phone: str,
    folio: Optional[str] = None,
) -> bytes:
    """
    Genera cotización en PDF y devuelve bytes listos para enviar.

    Parámetros:
        company: dict con claves del registro `companies`:
            - name (str)
            - address (str, opcional)
            - phone / whatsapp_display (str, opcional)
            - rfc (str, opcional)
            - logo_url (str, opcional) — reservado para futuro
        items: lista de dicts con:
            - name (str)       — nombre del producto
            - qty  (float)     — cantidad
            - unit (str)       — unidad (pza, mt, kg, etc.)
            - unit_price (float)
            - subtotal (float)
        client_phone: número del cliente (display)
        folio: si None se genera automáticamente

    Retorna:
        bytes del PDF
    """
    if folio is None:
        folio = generate_folio()

    now = datetime.now()
    fecha_str = now.strftime("%d/%m/%Y  %H:%M")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2.5 * cm,
    )

    S = _styles()
    story = []

    # ── ENCABEZADO ────────────────────────────────────────────────────────────
    company_name = company.get("name", "CotizaExpress")
    address      = company.get("address", "")
    phone_disp   = company.get("phone") or company.get("whatsapp_display", "")
    rfc          = company.get("rfc", "")

    # Bloque izquierdo: datos empresa
    left_lines = [Paragraph(company_name, S["company_name"])]
    if address:
        left_lines.append(Paragraph(address, S["company_sub"]))
    if phone_disp:
        left_lines.append(Paragraph(f"Tel/WA: {phone_disp}", S["company_sub"]))
    if rfc:
        left_lines.append(Paragraph(f"RFC: {rfc}", S["company_sub"]))

    # Bloque derecho: folio + fecha
    right_lines = [
        Paragraph("COTIZACIÓN", S["folio_label"]),
        Paragraph(folio, S["folio_value"]),
        Spacer(1, 6),
        Paragraph(fecha_str, S["folio_label"]),
        Paragraph(f"Cliente: {client_phone}", S["folio_label"]),
    ]

    header_table = Table(
        [[left_lines, right_lines]],
        colWidths=["60%", "40%"],
    )
    header_table.setStyle(
        TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING",  (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ])
    )
    story.append(header_table)
    story.append(Spacer(1, 0.3 * cm))
    story.append(HRFlowable(width="100%", thickness=2, color=COLOR_PRIMARY))
    story.append(Spacer(1, 0.4 * cm))

    # ── TABLA DE PRODUCTOS ────────────────────────────────────────────────────
    col_headers = ["#", "Producto", "Cant.", "Unidad", "P. Unit.", "Subtotal"]
    col_widths  = [1 * cm, 8.5 * cm, 1.8 * cm, 1.8 * cm, 2.2 * cm, 2.5 * cm]

    table_data = [col_headers]
    total = 0.0

    for i, item in enumerate(items, start=1):
        name       = item.get("name", "—")
        qty        = item.get("qty", 1)
        unit       = item.get("unit", "pza")
        unit_price = float(item.get("unit_price", 0))
        subtotal   = float(item.get("subtotal", unit_price * qty))
        total     += subtotal

        table_data.append([
            str(i),
            name,
            _fmt_qty(qty),
            unit,
            _fmt_price(unit_price),
            _fmt_price(subtotal),
        ])

    prod_table = Table(table_data, colWidths=col_widths, repeatRows=1)
    prod_table.setStyle(
        TableStyle([
            # Header
            ("BACKGROUND",   (0, 0), (-1, 0), COLOR_PRIMARY),
            ("TEXTCOLOR",    (0, 0), (-1, 0), COLOR_WHITE),
            ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1, 0), 8),
            ("ALIGN",        (0, 0), (-1, 0), "CENTER"),
            ("BOTTOMPADDING",(0, 0), (-1, 0), 6),
            ("TOPPADDING",   (0, 0), (-1, 0), 6),
            # Body
            ("FONTNAME",     (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",     (0, 1), (-1, -1), 8),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [COLOR_WHITE, COLOR_LIGHT]),
            ("ALIGN",        (0, 1), (0, -1),  "CENTER"),   # #
            ("ALIGN",        (2, 1), (2, -1),  "CENTER"),   # cant
            ("ALIGN",        (3, 1), (3, -1),  "CENTER"),   # unidad
            ("ALIGN",        (4, 1), (-1, -1), "RIGHT"),    # precios
            ("LEFTPADDING",  (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING",(0, 1), (-1, -1), 5),
            ("TOPPADDING",   (0, 1), (-1, -1), 5),
            ("GRID",         (0, 0), (-1, -1), 0.4, colors.HexColor("#dddddd")),
            ("LINEBELOW",    (0, 0), (-1, 0),  1, COLOR_PRIMARY),
        ])
    )
    story.append(prod_table)
    story.append(Spacer(1, 0.4 * cm))

    # ── BLOQUE TOTAL ──────────────────────────────────────────────────────────
    total_table = Table(
        [[
            Paragraph("TOTAL (IVA INCLUIDO)", S["total_label"]),
            Paragraph(_fmt_price(total), S["total_value"]),
        ]],
        colWidths=["70%", "30%"],
    )
    total_table.setStyle(
        TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), COLOR_PRIMARY),
            ("TOPPADDING",   (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 8),
            ("LEFTPADDING",  (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("ALIGN",        (0, 0), (0, 0),   "RIGHT"),
            ("ALIGN",        (1, 0), (1, 0),   "RIGHT"),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ])
    )
    story.append(total_table)
    story.append(Spacer(1, 0.6 * cm))

    # ── NOTA / PIE ────────────────────────────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc")))
    story.append(Spacer(1, 0.2 * cm))
    story.append(
        Paragraph(
            "Esta cotización tiene vigencia de 72 horas. Precios sujetos a cambio sin previo aviso.",
            S["footer"],
        )
    )
    story.append(
        Paragraph(
            f"Generado por CotizaExpress  •  {fecha_str}",
            S["footer"],
        )
    )

    doc.build(story)
    return buf.getvalue()


# ── Utilidades de formato ─────────────────────────────────────────────────────
def _fmt_price(value: float) -> str:
    """$1,234.50"""
    return f"${value:,.2f}"


def _fmt_qty(value) -> str:
    """Muestra entero si no tiene decimales: 2 → '2', 1.5 → '1.5'"""
    try:
        v = float(value)
        return str(int(v)) if v == int(v) else str(v)
    except (ValueError, TypeError):
        return str(value)
