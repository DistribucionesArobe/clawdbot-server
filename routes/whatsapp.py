"""
routes/whatsapp.py — WhatsApp configuration endpoints for the CotizaExpress dashboard.

Handles: config read/write, activation (code generation), code regeneration.
"""

import logging
import os
import random
import re
import string
from typing import Optional
from urllib.parse import quote as url_quote

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from auth import require_company_id
from db import get_conn

log = logging.getLogger("cotizaexpress.whatsapp")

router = APIRouter()

# The shared CotizaBot WhatsApp number (Twilio sandbox or production)
_COTIZABOT_NUMBER = (os.getenv("COTIZABOT_WHATSAPP_NUMBER") or "5215539815741").strip()


# ── Pydantic models ───────────────────────────────────────────────────────

class WhatsAppConfigUpdate(BaseModel):
    welcome_message: Optional[str] = None
    ai_tone: Optional[str] = None


class RegenerarCodigoBody(BaseModel):
    nuevo_codigo: Optional[str] = None


# ── Helpers ────────────────────────────────────────────────────────────────

def _generate_code(name: str) -> str:
    """Generate a short unique code from company name."""
    # Take first letters/words and make a 4-8 char code
    clean = re.sub(r'[^a-zA-Z0-9\s]', '', (name or "EMPRESA")).upper()
    words = clean.split()
    if len(words) >= 2:
        code = "".join(w[0:3] for w in words[:3])[:8]
    else:
        code = clean[:6]
    # Add random suffix to ensure uniqueness
    code = code[:5] + "".join(random.choices(string.digits, k=2))
    return code.upper()


def _build_whatsapp_link(code: str) -> str:
    """Build wa.me link with pre-filled message containing company code."""
    text = url_quote(f"{code}")
    return f"https://wa.me/{_COTIZABOT_NUMBER}?text={text}"


def _build_qr_url(link: str) -> str:
    """Build QR code URL via free API."""
    encoded = url_quote(link)
    return f"https://api.qrserver.com/v1/create-qr-code/?size=400x400&data={encoded}"


def _build_instrucciones(company_name: str, code: str, number: str) -> str:
    """Build customer instructions text."""
    return (
        f"Para cotizar en {company_name}:\n"
        f"1. Abre WhatsApp y envía '{code}' al número {number}\n"
        f"2. Escribe tu lista de materiales\n"
        f"3. Recibe tu cotización al instante"
    )


def _ensure_whatsapp_columns(conn):
    """Ensure companies table has whatsapp config columns."""
    cur = conn.cursor()
    for col, coltype in [
        ("wa_code", "VARCHAR(20)"),
        ("ai_tone", "VARCHAR(30) DEFAULT 'profesional'"),
    ]:
        cur.execute(f"""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='companies' AND column_name='{col}')
                THEN ALTER TABLE companies ADD COLUMN {col} {coltype};
                END IF;
            END $$;
        """)
    conn.commit()
    cur.close()


# ── Routes ─────────────────────────────────────────────────────────────────

@router.get("/api/whatsapp/configuracion")
def whatsapp_configuracion_get(request: Request):
    """Get WhatsApp config for the current company."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        _ensure_whatsapp_columns(conn)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT name, wa_code, welcome_message, ai_tone, owner_phone,
                   slug, plan_code
            FROM companies WHERE id=%s LIMIT 1
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")

        name, wa_code, welcome_message, ai_tone, owner_phone, slug, plan_code = row

        # Check if they have an active Meta channel
        has_meta_channel = False
        meta_phone_display = None
        try:
            cur.execute(
                """
                SELECT address FROM channels
                WHERE company_id=%s AND is_active=TRUE AND provider='meta'
                LIMIT 1
                """,
                (company_id,),
            )
            ch_row = cur.fetchone()
            if ch_row:
                has_meta_channel = True
                meta_phone_display = ch_row[0]
        except Exception:
            pass

        # Count conversations
        conv_count = 0
        try:
            cur.execute(
                "SELECT COUNT(*) FROM conversations WHERE company_id=%s",
                (company_id,),
            )
            conv_count = cur.fetchone()[0] or 0
        except Exception:
            pass

        configurado = bool(wa_code)

        whatsapp_data = {}
        if configurado:
            link = _build_whatsapp_link(wa_code)
            whatsapp_data = {
                "codigo": wa_code,
                "link": link,
                "qr_url": _build_qr_url(link),
                "instrucciones": _build_instrucciones(name or "tu empresa", wa_code, _COTIZABOT_NUMBER),
            }

        return {
            "ok": True,
            "configurado": configurado,
            "numero_cotizabot": _COTIZABOT_NUMBER,
            "whatsapp": whatsapp_data,
            "empresa": {
                "nombre": name,
                "slug": slug,
            },
            "configuracion": {
                "welcome_message": welcome_message or "",
                "ai_tone": ai_tone or "profesional",
            },
            "estadisticas": {
                "conversaciones": conv_count,
            },
            "meta_channel": {
                "connected": has_meta_channel,
                "phone_display": meta_phone_display,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("WHATSAPP CONFIG GET ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.put("/api/whatsapp/configuracion")
def whatsapp_configuracion_update(request: Request, body: WhatsAppConfigUpdate):
    """Update WhatsApp config (welcome message, AI tone)."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        _ensure_whatsapp_columns(conn)
        cur = conn.cursor()

        sets = []
        vals = []
        if body.welcome_message is not None:
            sets.append("welcome_message = %s")
            vals.append(body.welcome_message.strip() or None)
        if body.ai_tone is not None:
            sets.append("ai_tone = %s")
            vals.append(body.ai_tone.strip() or "profesional")

        if not sets:
            return {"ok": True}

        sets.append("updated_at = now()")
        vals.append(company_id)

        cur.execute(
            f"UPDATE companies SET {', '.join(sets)} WHERE id=%s RETURNING id",
            vals,
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        log.error("WHATSAPP CONFIG UPDATE ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.post("/api/whatsapp/activar")
def whatsapp_activar(request: Request):
    """Activate WhatsApp for a company — generate code, link, QR."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        _ensure_whatsapp_columns(conn)
        cur = conn.cursor()

        # Get company name to generate code
        cur.execute("SELECT name, wa_code FROM companies WHERE id=%s LIMIT 1", (company_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")

        name, existing_code = row

        if existing_code:
            return {"ok": True, "mensaje": "WhatsApp ya está activado", "codigo": existing_code}

        # Generate unique code
        code = _generate_code(name)
        # Ensure it's unique
        for _attempt in range(10):
            cur.execute("SELECT 1 FROM companies WHERE wa_code=%s AND id!=%s LIMIT 1", (code, company_id))
            if not cur.fetchone():
                break
            code = _generate_code(name)

        cur.execute(
            "UPDATE companies SET wa_code=%s, updated_at=now() WHERE id=%s RETURNING id",
            (code, company_id),
        )
        conn.commit()

        link = _build_whatsapp_link(code)
        return {
            "ok": True,
            "mensaje": f"¡WhatsApp activado! Tu código es {code}",
            "codigo": code,
            "link": link,
            "qr_url": _build_qr_url(link),
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("WHATSAPP ACTIVAR ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.post("/api/whatsapp/regenerar-codigo")
def whatsapp_regenerar_codigo(request: Request, body: RegenerarCodigoBody):
    """Regenerate the company's WhatsApp code."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        _ensure_whatsapp_columns(conn)
        cur = conn.cursor()

        nuevo = (body.nuevo_codigo or "").strip().upper()
        if nuevo:
            # Validate custom code
            if len(nuevo) < 3 or len(nuevo) > 10:
                raise HTTPException(status_code=400, detail="El código debe tener entre 3 y 10 caracteres")
            if not re.match(r'^[A-Z0-9]+$', nuevo):
                raise HTTPException(status_code=400, detail="El código solo puede contener letras y números")
            # Check uniqueness
            cur.execute("SELECT 1 FROM companies WHERE wa_code=%s AND id!=%s LIMIT 1", (nuevo, company_id))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail=f"El código '{nuevo}' ya está en uso")
            code = nuevo
        else:
            # Auto-generate
            cur.execute("SELECT name FROM companies WHERE id=%s LIMIT 1", (company_id,))
            row = cur.fetchone()
            name = row[0] if row else "EMPRESA"
            code = _generate_code(name)
            for _attempt in range(10):
                cur.execute("SELECT 1 FROM companies WHERE wa_code=%s AND id!=%s LIMIT 1", (code, company_id))
                if not cur.fetchone():
                    break
                code = _generate_code(name)

        cur.execute(
            "UPDATE companies SET wa_code=%s, updated_at=now() WHERE id=%s RETURNING id",
            (code, company_id),
        )
        conn.commit()

        link = _build_whatsapp_link(code)
        return {
            "ok": True,
            "mensaje": f"Código actualizado a {code}",
            "codigo": code,
            "link": link,
            "qr_url": _build_qr_url(link),
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("WHATSAPP REGENERAR ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()
