"""
generate_quote_pdf.py
CotizaExpress — Generador de cotizaciones en PDF  (v2 — con logo + colores corporativos)

Uso:
    from generate_quote_pdf import build_quote_pdf
    pdf_bytes = build_quote_pdf(company, items, client_phone, folio)

company dict esperado:
    {
        "name":        str,
        "address":     str  (opcional),
        "phone":       str  (opcional),
        "rfc":         str  (opcional),
        "email":       str  (opcional),
        "logo_url":    str  (opcional — URL pública de la imagen del logo),
        "brand_color": str  (opcional — hex como "#1a6b3a"; default verde CotizaExpress),
    }
"""

import io
import random
import string
import urllib.request
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
    Image,
)

_DEFAULT_PRIMARY = "#1a6b3a"


def _hex_to_color(hex_str: str) -> colors.Color:
    try:
        return colors.HexColor(hex_str)
    except Exception:
        return colors.HexColor(_DEFAULT_PRIMARY)


def generate_folio() -> str:
    chars = string.ascii_uppercase + string.digits
    suffix = "".join(random.choices(chars, k=6))
    return f"CX-{suffix}"


def _fetch_logo(logo_url: str) -> Optional[bytes]:
    """Soporta data URLs (base64 en DB) y URLs públicas (https://)."""
    if not logo_url:
        return None
    if logo_url.startswith("data:"):
        try:
            import base64
            _, data = logo_url.split(",", 1)
            return base64.b64decode(data)
        except Exception:
            return None
    try:
        req = urllib.request.Request(logo_url, headers={"User-Agent": "CotizaExpress-PDF/2.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.read()
    except Exception:
        return None


def _styles(primary: colors.Color):
    r, g, b = primary.red, primary.green, primary.blue
    COLOR_LIGHT = colors.Color(r * 0.12 + 0.88, g * 0.12 + 0.88, b * 0.12 + 0.88)
    COLOR_GRAY  = colors.HexColor("#555555")
    COLOR_BLACK = colors.HexColor("#1a1a1a")
    COLOR_WHITE = colors.white

    return {
        "company_name": ParagraphStyle(
            "company_name", fontSize=17, fontName="Helvetica-Bold",
            textColor=primary, leading=21, spaceAfter=2,
        ),
        "company_sub": ParagraphStyle(
            "company_sub", fontSize=8, fontName="Helvetica",
            textColor=COLOR_GRAY, leading=12,
        ),
        "folio_label": ParagraphStyle(
            "folio_label", fontSize=7.5, fontName="Helvetica",
            textColor=COLOR_GRAY, alignment=TA_RIGHT, spaceAfter=1,
        ),
        "folio_value": ParagraphStyle(
            "folio_value", fontSize=15, fontName="Helvetica-Bold",
            textColor=primary, alignment=TA_RIGHT, spaceAfter=4,
        ),
        "col_header": ParagraphStyle(
            "col_header", fontSize=8, fontName="Helvetica-Bold",
            textColor=COLOR_WHITE, alignment=TA_CENTER,
        ),
        "body": ParagraphStyle(
            "body", fontSize=8.5, fontName="Helvetica",
            textColor=COLOR_BLACK, leading=13,
        ),
        "body_right": ParagraphStyle(
            "body_right", fontSize=8.5, fontName="Helvetica",
            textColor=COLOR_BLACK, alignment=TA_RIGHT, leading=13,
        ),
        "total_label": ParagraphStyle(
            "total_label", fontSize=11, fontName="Helvetica-Bold",
            textColor=COLOR_WHITE, alignment=TA_LEFT,
        ),
        "total_value": ParagraphStyle(
            "total_value", fontSize=13, fontName="Helvetica-Bold",
            textColor=COLOR_WHITE, alignment=TA_RIGHT,
        ),
        "footer": ParagraphStyle(
            "footer", fontSize=7, fontName="Helvetica",
            textColor=COLOR_GRAY, alignment=TA_CENTER, leading=11,
        ),
        "_primary": primary,
        "_light":   COLOR_LIGHT,
        "_white":   COLOR_WHITE,
    }


def build_quote_pdf(
    company: dict,
    items: list[dict],
    client_phone: str,
    folio: Optional[str] = None,
) -> bytes:
    if folio is None:
        folio = generate_folio()

    now = datetime.now()
    fecha_str = now.strftime("%d/%m/%Y  %H:%M")

    brand_hex = (company.get("brand_color") or _DEFAULT_PRIMARY).strip()
    primary   = _hex_to_color(brand_hex)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=1.8*cm, bottomMargin=2.5*cm,
    )

    S = _styles(primary)
    story = []

    # ── ENCABEZADO ────────────────────────────────────────────────────────────
    company_name = company.get("name", "CotizaExpress")
    address      = company.get("address", "")
    phone_disp   = company.get("phone") or company.get("whatsapp_display", "")
    rfc          = company.get("rfc", "")
    email        = company.get("email", "")
    logo_url     = company.get("logo_url", "")

    left_content = []

    # Logo
    if logo_url:
        logo_bytes = _fetch_logo(logo_url)
        if logo_bytes:
            try:
                logo_img = Image(io.BytesIO(logo_bytes), width=3.5*cm, height=1.8*cm)
                logo_img.hAlign = "LEFT"
                left_content.append(logo_img)
                left_content.append(Spacer(1, 5))
            except Exception:
                pass

    left_content.append(Paragraph(company_name, S["company_name"]))
    for line in filter(None, [
        f"RFC: {rfc}" if rfc else None,
        f"Email: {email}" if email else None,
        f"Tel / WhatsApp: {phone_disp}" if phone_disp else None,
        address if address else None,
    ]):
        left_content.append(Paragraph(line, S["company_sub"]))

    right_content = [
        Paragraph("COTIZACIÓN", S["folio_label"]),
        Paragraph(folio, S["folio_value"]),
        Paragraph(f"Fecha: {fecha_str}", S["folio_label"]),
        Spacer(1, 4),
        Paragraph(f"Cliente: {client_phone}", S["folio_label"]),
    ]

    header_table = Table([[left_content, right_content]], colWidths=["62%", "38%"])
    header_table.setStyle(TableStyle([
        ("VALIGN",        (0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",   (0,0),(-1,-1),0),
        ("RIGHTPADDING",  (0,0),(-1,-1),0),
        ("BOTTOMPADDING", (0,0),(-1,-1),0),
        ("TOPPADDING",    (0,0),(-1,-1),0),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 0.35*cm))
    story.append(HRFlowable(width="100%", thickness=2.5, color=primary, spaceAfter=8))

    # ── TABLA DE PRODUCTOS ────────────────────────────────────────────────────
    col_widths = [0.8*cm, 8.6*cm, 1.7*cm, 1.7*cm, 2.2*cm, 2.6*cm]
    headers = [
        Paragraph("#",          S["col_header"]),
        Paragraph("Producto",   S["col_header"]),
        Paragraph("Cant.",      S["col_header"]),
        Paragraph("Unidad",     S["col_header"]),
        Paragraph("P. Unit.",   S["col_header"]),
        Paragraph("Subtotal",   S["col_header"]),
    ]
    table_data = [headers]
    total = 0.0

    for i, item in enumerate(items, start=1):
        name       = item.get("name", "—")
        qty        = item.get("qty", 1)
        unit       = item.get("unit", "pza")
        unit_price = float(item.get("unit_price", 0))
        subtotal   = float(item.get("subtotal", unit_price * qty))
        total     += subtotal
        table_data.append([
            Paragraph(str(i),                  S["body"]),
            Paragraph(name,                    S["body"]),
            Paragraph(_fmt_qty(qty),           S["body_right"]),
            Paragraph(unit,                    S["body"]),
            Paragraph(_fmt_price(unit_price),  S["body_right"]),
            Paragraph(_fmt_price(subtotal),    S["body_right"]),
        ])

    light = S["_light"]
    white = S["_white"]
    prod_table = Table(table_data, colWidths=col_widths, repeatRows=1)
    prod_table.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,0), primary),
        ("TOPPADDING",    (0,0),(-1,0), 7),
        ("BOTTOMPADDING", (0,0),(-1,0), 7),
        ("LEFTPADDING",   (0,0),(-1,0), 5),
        ("RIGHTPADDING",  (0,0),(-1,0), 5),
        ("LINEBELOW",     (0,0),(-1,0), 1.5, primary),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[white, light]),
        ("TOPPADDING",    (0,1),(-1,-1), 5),
        ("BOTTOMPADDING", (0,1),(-1,-1), 5),
        ("LEFTPADDING",   (0,1),(-1,-1), 5),
        ("RIGHTPADDING",  (0,1),(-1,-1), 5),
        ("GRID",          (0,0),(-1,-1), 0.3, colors.HexColor("#dddddd")),
        ("LINEBELOW",     (0,-1),(-1,-1), 0.8, colors.HexColor("#cccccc")),
        ("VALIGN",        (0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(prod_table)
    story.append(Spacer(1, 0.3*cm))

    # ── BLOQUE TOTAL ──────────────────────────────────────────────────────────
    total_table = Table(
        [[Paragraph("TOTAL  (IVA incluido)", S["total_label"]),
          Paragraph(_fmt_price(total),       S["total_value"])]],
        colWidths=["60%", "40%"],
    )
    total_table.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), primary),
        ("TOPPADDING",    (0,0),(-1,-1), 9),
        ("BOTTOMPADDING", (0,0),(-1,-1), 9),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("RIGHTPADDING",  (0,0),(-1,-1), 12),
        ("VALIGN",        (0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(total_table)
    story.append(Spacer(1, 0.7*cm))

    # ── PIE ───────────────────────────────────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc"), spaceAfter=5))
    story.append(Paragraph(
        "Esta cotización tiene vigencia de 72 horas. Precios sujetos a cambio sin previo aviso.",
        S["footer"],
    ))
    story.append(Spacer(1, 3))
    story.append(Paragraph(
        'Generado por <a href="https://cotizaexpress.com"><u>cotizaexpress.com</u></a>'
        f"  •  {fecha_str}",
        S["footer"],
    ))

    doc.build(story)
    return buf.getvalue()


def _fmt_price(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_qty(value) -> str:
    try:
        v = float(value)
        return str(int(v)) if v == int(v) else str(v)
    except (ValueError, TypeError):
        return str(value)
