"""
routes/empresa.py — Empresa profile, onboarding, and WhatsApp embedded signup.

Business profile setup, onboarding flow, Meta OAuth integration.
"""

import logging
import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from auth import require_company_id
from db import get_conn
import migrations

log = logging.getLogger("cotizaexpress.empresa")

router = APIRouter()


# ── Helpers ─────────────────────────────────────────────────────────────────

def _run_onboarding_migrations(conn):
    """Add onboarding columns to companies table (idempotent)."""
    cur = conn.cursor()
    try:
        for col, col_type in [
            ("giro", "TEXT"),
            ("ciudad", "TEXT"),
            ("descripcion", "TEXT"),
            ("onboarding_completed", "BOOLEAN DEFAULT FALSE"),
            ("construccion_ligera_enabled", "BOOLEAN DEFAULT FALSE"),
            ("rejacero_enabled", "BOOLEAN DEFAULT FALSE"),
            ("pintura_enabled", "BOOLEAN DEFAULT FALSE"),
            ("impermeabilizante_enabled", "BOOLEAN DEFAULT FALSE"),
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'companies'
                        AND column_name = '{col}'
                    ) THEN
                        ALTER TABLE companies ADD COLUMN {col} {col_type};
                    END IF;
                END $$;
            """)
        log.info("ONBOARDING MIGRATIONS: OK")
    except Exception as e:
        log.error("ONBOARDING MIGRATION ERROR: %s", repr(e))
    finally:
        cur.close()


def _generate_tenant_context(giro, ciudad, descripcion, hours_semana, hours_sabado, hours_domingo):
    """Auto-generate tenant_context string from onboarding data."""
    parts = []

    if giro:
        parts.append(giro.strip())
    if ciudad:
        parts.append(f"en {ciudad.strip()}")
    if descripcion:
        parts.append(descripcion.strip().rstrip("."))

    horario_parts = []
    if hours_semana:
        horario_parts.append(f"L-V {hours_semana}")
    if hours_sabado and hours_sabado.lower() != "cerrado":
        horario_parts.append(f"Sáb {hours_sabado}")
    elif hours_sabado and hours_sabado.lower() == "cerrado":
        horario_parts.append("Sáb cerrado")
    if hours_domingo and hours_domingo.lower() != "cerrado":
        horario_parts.append(f"Dom {hours_domingo}")
    elif hours_domingo and hours_domingo.lower() == "cerrado":
        horario_parts.append("Dom cerrado")

    if horario_parts:
        parts.append("Horario: " + ", ".join(horario_parts))

    return ". ".join(parts) + "." if parts else ""


# ── Pydantic models ────────────────────────────────────────────────────────

class EmpresaPerfilBody(BaseModel):
    giro: Optional[str] = None
    ciudad: Optional[str] = None
    descripcion: Optional[str] = None
    whatsapp: Optional[str] = None
    empresa_nombre: Optional[str] = None
    horario_semana: Optional[str] = None
    horario_sabado: Optional[str] = None
    horario_domingo: Optional[str] = None


class EmbeddedSignupBody(BaseModel):
    code: str
    phone_number_id: Optional[str] = None
    waba_id: Optional[str] = None


# ── Routes ──────────────────────────────────────────────────────────────────

@router.put("/api/empresa/perfil")
def empresa_perfil_update(request: Request, body: EmpresaPerfilBody):
    """Save onboarding business profile and auto-generate tenant_context."""
    company_id = require_company_id(request)

    giro = (body.giro or "").strip() or None
    ciudad = (body.ciudad or "").strip() or None
    descripcion = (body.descripcion or "").strip() or None
    whatsapp = (body.whatsapp or "").strip().replace(" ", "") or None
    empresa_nombre = (body.empresa_nombre or "").strip() or None
    horario_semana = (body.horario_semana or "").strip() or None
    horario_sabado = (body.horario_sabado or "").strip() or None
    horario_domingo = (body.horario_domingo or "").strip() or None

    # Build hours_text for the existing field
    hours_parts = []
    if horario_semana:
        hours_parts.append(f"Lunes a Viernes: {horario_semana}")
    if horario_sabado:
        hours_parts.append(f"Sábado: {horario_sabado}")
    if horario_domingo:
        hours_parts.append(f"Domingo: {horario_domingo}")
    hours_text = " | ".join(hours_parts) if hours_parts else None

    # Auto-generate tenant_context
    tenant_context = _generate_tenant_context(
        giro, ciudad, descripcion, horario_semana, horario_sabado, horario_domingo
    )

    conn = None
    cur = None
    try:
        conn = get_conn()
        # Run migrations first (idempotent)
        migrations.run_pricebook_migrations(conn)
        _run_onboarding_migrations(conn)

        cur = conn.cursor()
        cur.execute(
            """
            UPDATE companies
            SET giro = COALESCE(%s, giro),
                ciudad = COALESCE(%s, ciudad),
                descripcion = COALESCE(%s, descripcion),
                owner_phone = COALESCE(%s, owner_phone),
                name = COALESCE(%s, name),
                hours_text = COALESCE(%s, hours_text),
                tenant_context = %s,
                updated_at = now()
            WHERE id = %s
            RETURNING id
            """,
            (giro, ciudad, descripcion, whatsapp, empresa_nombre,
             hours_text, tenant_context or None, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        conn.commit()

        # Regenerate LLM context in background (uses giro + catalog + marcas)
        try:
            from llm_context_generator import generate_and_store_llm_context
            import threading
            threading.Thread(
                target=generate_and_store_llm_context,
                args=(company_id,),
                daemon=True,
            ).start()
        except Exception as lce:
            log.error("LLM CONTEXT REGEN ERROR: %s", repr(lce))

        return {"ok": True, "tenant_context": tenant_context}
    except HTTPException:
        raise
    except Exception as e:
        log.error("EMPRESA PERFIL ERROR: %s", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error guardando perfil")
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.post("/api/empresa/onboarding-complete")
def empresa_onboarding_complete(request: Request):
    """Mark company onboarding as completed."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        _run_onboarding_migrations(conn)
        cur = conn.cursor()
        cur.execute(
            "UPDATE companies SET onboarding_completed = TRUE, updated_at = now() WHERE id = %s RETURNING id",
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        conn.commit()
        return {"ok": True, "onboarding_completed": True}
    except HTTPException:
        raise
    except Exception as e:
        log.error("ONBOARDING COMPLETE ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail="Error actualizando onboarding")
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.get("/api/empresa/onboarding-status")
def empresa_onboarding_status(request: Request):
    """Check if company has completed onboarding."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        _run_onboarding_migrations(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT onboarding_completed FROM companies WHERE id = %s LIMIT 1",
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        return {"ok": True, "onboarding_completed": bool(row[0])}
    except HTTPException:
        raise
    except Exception as e:
        log.error("ONBOARDING STATUS ERROR: %s", repr(e))
        return {"ok": True, "onboarding_completed": False}
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ── WhatsApp Embedded Signup ────────────────────────────────────────────────

_META_APP_ID = "1461694011992339"
_META_APP_SECRET = "28989d2a761e5be1b8ff4eb77c795715"
_META_GRAPH_VERSION = "v21.0"
_META_GRAPH_URL = f"https://graph.facebook.com/{_META_GRAPH_VERSION}"


@router.post("/api/whatsapp/embedded-signup")
def whatsapp_embedded_signup(request: Request, body: EmbeddedSignupBody):
    """Exchange Meta OAuth code for token, create channel, subscribe webhook."""
    import requests as http_requests
    company_id = require_company_id(request)
    code = (body.code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="Missing code")

    phone_number_id = (body.phone_number_id or "").strip()
    waba_id = (body.waba_id or "").strip()

    # Step 1: Exchange code for access token
    log.info("EMBEDDED SIGNUP: exchanging code for company=%s", company_id)
    token_resp = http_requests.get(f"{_META_GRAPH_URL}/oauth/access_token", params={
        "client_id": _META_APP_ID,
        "client_secret": _META_APP_SECRET,
        "code": code,
    })
    if token_resp.status_code != 200:
        log.error("EMBEDDED SIGNUP TOKEN ERROR: %s %s", token_resp.status_code, token_resp.text)
        raise HTTPException(status_code=400, detail="Error al obtener token de Meta")
    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        log.info("EMBEDDED SIGNUP: no access_token in response: %s", token_data)
        raise HTTPException(status_code=400, detail="No se obtuvo token de acceso")

    log.info("EMBEDDED SIGNUP: got access token for company=%s", company_id)

    # Step 2: If we don't have phone_number_id/waba_id from session, try to get from API
    if not waba_id:
        try:
            debug_resp = http_requests.get(
                f"{_META_GRAPH_URL}/debug_token",
                params={"input_token": access_token, "access_token": access_token}
            )
            debug_data = debug_resp.json().get("data", {})
            granular = debug_data.get("granular_scopes", [])
            for scope in granular:
                if scope.get("scope") == "whatsapp_business_management":
                    target_ids = scope.get("target_ids", [])
                    if target_ids:
                        waba_id = target_ids[0]
                        break
            log.info("EMBEDDED SIGNUP: resolved waba_id=%s from debug_token", waba_id)
        except Exception as e:
            log.error("EMBEDDED SIGNUP: debug_token error: %s", repr(e))

    if not phone_number_id and waba_id:
        try:
            phones_resp = http_requests.get(
                f"{_META_GRAPH_URL}/{waba_id}/phone_numbers",
                params={"access_token": access_token}
            )
            phones_data = phones_resp.json().get("data", [])
            if phones_data:
                phone_number_id = phones_data[0].get("id")
                log.info("EMBEDDED SIGNUP: resolved phone_number_id=%s", phone_number_id)
        except Exception as e:
            log.error("EMBEDDED SIGNUP: phone_numbers error: %s", repr(e))

    if not phone_number_id:
        log.info("EMBEDDED SIGNUP: could not resolve phone_number_id")
        raise HTTPException(status_code=400, detail="No se pudo obtener el número de teléfono. Intenta de nuevo.")

    # Step 3: Get phone number display info
    phone_display = ""
    try:
        phone_info_resp = http_requests.get(
            f"{_META_GRAPH_URL}/{phone_number_id}",
            params={"access_token": access_token, "fields": "display_phone_number,verified_name"}
        )
        phone_info = phone_info_resp.json()
        phone_display = phone_info.get("display_phone_number", "")
        verified_name = phone_info.get("verified_name", "")
        log.info("EMBEDDED SIGNUP: phone=%s name=%s", phone_display, verified_name)
    except Exception as e:
        log.error("EMBEDDED SIGNUP: phone info error: %s", repr(e))

    # Step 4: Subscribe app to WABA (to receive webhooks)
    if waba_id:
        try:
            sub_resp = http_requests.post(
                f"{_META_GRAPH_URL}/{waba_id}/subscribed_apps",
                params={"access_token": access_token}
            )
            log.info("EMBEDDED SIGNUP: subscribe app to WABA: %s %s", sub_resp.status_code, sub_resp.text)
        except Exception as e:
            log.error("EMBEDDED SIGNUP: subscribe error: %s", repr(e))

    # Step 5: Register phone number for Cloud API
    try:
        reg_resp = http_requests.post(
            f"{_META_GRAPH_URL}/{phone_number_id}/register",
            json={"messaging_product": "whatsapp", "pin": "123456"},
            params={"access_token": access_token}
        )
        log.info("EMBEDDED SIGNUP: register phone: %s %s", reg_resp.status_code, reg_resp.text)
    except Exception as e:
        log.error("EMBEDDED SIGNUP: register phone error: %s", repr(e))

    # Step 6: Save to database — create/update channel + update company
    conn = get_conn()
    try:
        cur = conn.cursor()
        # Ensure channels table exists with needed columns
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='channels') THEN
                    CREATE TABLE channels (
                        id SERIAL PRIMARY KEY,
                        company_id UUID NOT NULL REFERENCES companies(id),
                        provider VARCHAR(50) DEFAULT 'meta',
                        channel_type VARCHAR(50) DEFAULT 'whatsapp',
                        meta_phone_number_id VARCHAR(100),
                        meta_waba_id VARCHAR(100),
                        address VARCHAR(100),
                        access_token TEXT,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT now(),
                        updated_at TIMESTAMP DEFAULT now()
                    );
                END IF;
            END $$;
        """)
        # Add columns if they don't exist
        for col, coltype in [
            ("meta_waba_id", "VARCHAR(100)"),
            ("access_token", "TEXT"),
        ]:
            cur.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                        WHERE table_name='channels' AND column_name='{col}')
                    THEN ALTER TABLE channels ADD COLUMN {col} {coltype};
                    END IF;
                END $$;
            """)

        # Deactivate any existing channels for this company
        cur.execute("UPDATE channels SET is_active = FALSE WHERE company_id = %s", (company_id,))

        # Insert new channel
        cur.execute("""
            INSERT INTO channels (company_id, provider, channel_type, meta_phone_number_id,
                                  meta_waba_id, address, access_token, is_active)
            VALUES (%s, 'meta', 'whatsapp', %s, %s, %s, %s, TRUE)
            RETURNING id
        """, (company_id, phone_number_id, waba_id, phone_display, access_token))
        channel_id = cur.fetchone()[0]

        # Also update company's wa fields for backward compat
        cur.execute("""
            UPDATE companies
            SET wa_api_key = %s, wa_phone_number_id = %s, updated_at = now()
            WHERE id = %s
        """, (access_token, phone_number_id, company_id))

        conn.commit()
        log.info("EMBEDDED SIGNUP: SUCCESS company=%s channel=%s phone=%s", company_id, channel_id, phone_display)
        return {
            "ok": True,
            "channel_id": channel_id,
            "phone_number_id": phone_number_id,
            "waba_id": waba_id,
            "phone_display": phone_display,
        }
    except Exception as e:
        conn.rollback()
        log.error("EMBEDDED SIGNUP DB ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail="Error al guardar la configuración")
    finally:
        cur.close()
        conn.close()
