"""
routes/company.py — Company settings and logo endpoints for CotizaExpress.

Company settings GET/POST, logo upload/delete.
"""

import base64
import logging
from typing import Optional

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from pydantic import BaseModel, validator

from auth import require_company_id
from db import get_conn
from whatsapp_api import update_wa_profile_photo, combine_with_cotizabot, _prepare_profile_image

log = logging.getLogger("cotizaexpress.company")

router = APIRouter()


# ── Pydantic models ────────────────────────────────────────────────────────

class CompanySettingsBody(BaseModel):
    hours_text: Optional[str] = None
    address_text: Optional[str] = None
    google_maps_url: Optional[str] = None
    mercadopago_url: Optional[str] = None
    bank_name: Optional[str] = None
    bank_account_name: Optional[str] = None
    bank_clabe: Optional[str] = None
    bank_account_number: Optional[str] = None
    owner_phone: Optional[str] = None
    email: Optional[str] = None
    welcome_products_hint: Optional[str] = None
    rfc: Optional[str] = None
    brand_color: Optional[str] = None
    discount_threshold: Optional[float] = None
    discount_percent: Optional[float] = None
    company_name: Optional[str] = None
    construccion_ligera_enabled: Optional[bool] = None
    rejacero_enabled: Optional[bool] = None
    pintura_enabled: Optional[bool] = None
    impermeabilizante_enabled: Optional[bool] = None
    welcome_message: Optional[str] = None
    telefono_atencion: Optional[str] = None
    marcas_propias: Optional[str] = None
    marcas_competencia: Optional[str] = None
    giro: Optional[str] = None
    giro_otro: Optional[str] = None
    # Structured attention schedule: {"tz": "America/Monterrey",
    #   "days": {"mon": {"open": "09:00", "close": "18:00", "closed": false}, ...}}
    attention_schedule: Optional[dict] = None

    @validator('discount_threshold', 'discount_percent', pre=True)
    def coerce_empty_to_none(cls, v):
        if v == '' or v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


# ── Routes ──────────────────────────────────────────────────────────────────


@router.post("/api/company/settings")
def company_settings_update(request: Request, body: CompanySettingsBody):
    company_id = require_company_id(request)

    # Build fully dynamic SET clause — only update fields that were actually sent
    _sets = []
    _vals = []

    def _add_str(field_val, col_name, strip_spaces=False, upper_case=False):
        if field_val is not None:
            v = field_val.strip()
            if strip_spaces:
                v = v.replace(" ", "")
            if upper_case:
                v = v.upper()
            _sets.append(f"{col_name}=%s")
            _vals.append(v or None)

    def _add_bool(field_val, col_name):
        if field_val is not None:
            _sets.append(f"{col_name}=%s")
            _vals.append(field_val)

    _add_str(body.hours_text, "hours_text")
    _add_str(body.address_text, "address_text")
    _add_str(body.google_maps_url, "google_maps_url")
    _add_str(body.mercadopago_url, "mercadopago_url")
    _add_str(body.bank_name, "bank_name")
    _add_str(body.bank_account_name, "bank_account_name")
    _add_str(body.bank_clabe, "bank_clabe", strip_spaces=True)
    _add_str(body.bank_account_number, "bank_account_number", strip_spaces=True)
    _add_str(body.owner_phone, "owner_phone", strip_spaces=True)
    _add_str(body.email, "email")
    _add_str(body.rfc, "rfc", upper_case=True)
    _add_str(body.brand_color, "brand_color")
    _add_str(body.welcome_products_hint, "welcome_products_hint")
    _add_str(body.company_name, "name")
    _add_str(body.welcome_message, "welcome_message")
    _add_str(body.telefono_atencion, "telefono_atencion", strip_spaces=True)

    # Numeric fields
    if body.discount_threshold is not None:
        _sets.append("discount_threshold=%s")
        _vals.append(float(body.discount_threshold) if body.discount_threshold > 0 else None)
    if body.discount_percent is not None:
        _sets.append("discount_percent=%s")
        _vals.append(float(body.discount_percent) if 0 < body.discount_percent <= 100 else None)

    # Brand context fields
    _add_str(body.marcas_propias, "marcas_propias")
    _add_str(body.marcas_competencia, "marcas_competencia")

    # Giro (business type)
    _add_str(body.giro, "giro")
    _add_str(body.giro_otro, "giro_otro")

    # Module toggles
    _add_bool(body.construccion_ligera_enabled, "construccion_ligera_enabled")
    _add_bool(body.rejacero_enabled, "rejacero_enabled")
    _add_bool(body.pintura_enabled, "pintura_enabled")
    _add_bool(body.impermeabilizante_enabled, "impermeabilizante_enabled")

    if not _sets and body.attention_schedule is None:
        return {"ok": True}

    # CLABE validation
    if body.bank_clabe is not None:
        clabe_clean = body.bank_clabe.strip().replace(" ", "")
        if clabe_clean and (not clabe_clean.isdigit() or len(clabe_clean) != 18):
            raise HTTPException(status_code=400, detail="CLABE inválida (debe ser 18 dígitos)")

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Ensure module columns exist (idempotent migration)
        for _mcol in ("construccion_ligera_enabled", "rejacero_enabled", "pintura_enabled", "impermeabilizante_enabled"):
            cur.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='companies' AND column_name='{_mcol}')
                    THEN ALTER TABLE companies ADD COLUMN {_mcol} BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            """)
        # welcome_message column (TEXT)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='companies' AND column_name='welcome_message')
                THEN ALTER TABLE companies ADD COLUMN welcome_message TEXT;
                END IF;
            END $$;
        """)
        # Brand context & giro columns
        for _bcol in ("marcas_propias", "marcas_competencia", "giro", "giro_otro"):
            cur.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='companies' AND column_name='{_bcol}')
                    THEN ALTER TABLE companies ADD COLUMN {_bcol} TEXT;
                    END IF;
                END $$;
            """)
        # Attention schedule (JSONB)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='companies' AND column_name='attention_schedule')
                THEN ALTER TABLE companies ADD COLUMN attention_schedule JSONB;
                END IF;
            END $$;
        """)
        conn.commit()

        # attention_schedule (JSONB) — set via json string
        if body.attention_schedule is not None:
            import json as _json
            _sets.append("attention_schedule=%s::jsonb")
            _vals.append(_json.dumps(body.attention_schedule))

        _sets.append("updated_at=now()")
        _vals.append(company_id)
        set_clause = ", ".join(_sets)
        cur.execute(
            f"UPDATE companies SET {set_clause} WHERE id=%s RETURNING id",
            tuple(_vals),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")

        # Rebuild tenant_context if brand fields were updated
        if body.marcas_propias is not None or body.marcas_competencia is not None:
            cur.execute(
                "SELECT tenant_context, marcas_propias, marcas_competencia FROM companies WHERE id=%s",
                (company_id,),
            )
            _tc_row = cur.fetchone()
            if _tc_row:
                _base_ctx = (_tc_row[0] or "")
                # Strip old brand context lines if present
                _ctx_lines = [l for l in _base_ctx.split(". ") if not l.startswith("Marcas que manejo:") and not l.startswith("Marcas de competencia:")]
                _base_ctx = ". ".join(_ctx_lines).rstrip(". ").strip()
                _mp = (_tc_row[1] or "").strip()
                _mc = (_tc_row[2] or "").strip()
                if _mp:
                    _base_ctx += f". Marcas que manejo: {_mp}"
                if _mc:
                    _base_ctx += f". Marcas de competencia: {_mc}. Cuando el cliente pida productos de estas marcas, busca el equivalente en mis marcas"
                _base_ctx = _base_ctx.strip(". ").strip()
                if _base_ctx:
                    _base_ctx += "."
                cur.execute("UPDATE companies SET tenant_context=%s WHERE id=%s", (_base_ctx or None, company_id))

        conn.commit()

        # Regenerate LLM context if brand/giro-relevant fields changed
        if body.marcas_propias is not None or body.marcas_competencia is not None or body.giro is not None:
            try:
                from llm_context_generator import generate_and_store_llm_context
                import threading
                threading.Thread(
                    target=generate_and_store_llm_context,
                    args=(company_id,),
                    daemon=True,
                ).start()
            except Exception as lce:
                import logging
                logging.getLogger("cotizaexpress.company").error(
                    "LLM CONTEXT REGEN ERROR: %s", repr(lce)
                )

        return {"ok": True}
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.get("/api/company/settings")
def company_settings_get(request: Request):
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Ensure new columns exist before querying them
        for _mcol, _mtype in [
            ("construccion_ligera_enabled", "BOOLEAN DEFAULT FALSE"),
            ("rejacero_enabled", "BOOLEAN DEFAULT FALSE"),
            ("pintura_enabled", "BOOLEAN DEFAULT FALSE"),
            ("impermeabilizante_enabled", "BOOLEAN DEFAULT FALSE"),
            ("welcome_message", "TEXT"),
            ("telefono_atencion", "VARCHAR(30)"),
            ("marcas_propias", "TEXT"),
            ("marcas_competencia", "TEXT"),
            ("giro", "TEXT"),
            ("giro_otro", "TEXT"),
            ("plan_code", "VARCHAR(30) DEFAULT 'free'"),
            ("attention_schedule", "JSONB"),
        ]:
            cur.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='companies' AND column_name='{_mcol}')
                    THEN ALTER TABLE companies ADD COLUMN {_mcol} {_mtype};
                    END IF;
                END $$;
            """)
        conn.commit()

        cur.execute(
            """
            SELECT hours_text, address_text, google_maps_url,
                   mercadopago_url, bank_name, bank_account_name, bank_clabe, bank_account_number,
                   owner_phone, email, rfc, brand_color, logo_url,
                   discount_threshold, discount_percent, welcome_products_hint, welcome_message,
                   telefono_atencion, marcas_propias, marcas_competencia,
                   giro, giro_otro, plan_code, attention_schedule
            FROM companies WHERE id=%s LIMIT 1
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        return {
            "ok": True,
            "company_id": company_id,
            "settings": {
                "hours_text": row[0], "address_text": row[1], "google_maps_url": row[2],
                "mercadopago_url": row[3], "bank_name": row[4], "bank_account_name": row[5],
                "bank_clabe": row[6], "bank_account_number": row[7], "owner_phone": row[8],
                "email": row[9], "rfc": row[10], "brand_color": row[11], "logo_url": row[12],
                "discount_threshold": float(row[13]) if row[13] else None,
                "discount_percent":   float(row[14]) if row[14] else None,
                "welcome_products_hint": row[15] or None,
                "welcome_message": row[16] or None,
                "telefono_atencion": row[17] or None,
                "marcas_propias": row[18] or None,
                "marcas_competencia": row[19] or None,
                "giro": row[20] or None,
                "giro_otro": row[21] or None,
                "plan_code": row[22] or "free",
                "attention_schedule": row[23] or None,
            },
        }
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.post("/api/company/logo")
async def upload_company_logo(
    request: Request,
    file: UploadFile = File(...),
    with_cotizabot: bool = False,
):
    company_id = require_company_id(request)

    content = await file.read()
    if len(content) > 2 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Imagen demasiado grande (máx 2 MB)")

    ext = (file.filename or "logo.png").rsplit(".", 1)[-1].lower()
    if ext not in ("png", "jpg", "jpeg", "webp"):
        raise HTTPException(status_code=400, detail="Formato no soportado (usa PNG, JPG o WEBP)")

    mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
    b64  = base64.b64encode(content).decode()
    data_url = f"data:{mime};base64,{b64}"

    conn = None
    cur  = None
    wa_profile_result = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "UPDATE companies SET logo_url=%s, updated_at=now() WHERE id=%s RETURNING id",
            (data_url, company_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Company no encontrada")
        conn.commit()

        # Also update WhatsApp Business profile photo if WA is configured
        cur.execute(
            "SELECT wa_api_key, wa_phone_number_id FROM companies WHERE id=%s",
            (company_id,),
        )
        wa_row = cur.fetchone()
        if wa_row and wa_row[0] and wa_row[1]:
            log.info("LOGO UPLOAD: also updating WhatsApp profile photo for company=%s", company_id)
            wa_profile_result = update_wa_profile_photo(
                wa_api_key=wa_row[0],
                phone_number_id=wa_row[1],
                img_bytes=content,
                mime_type=mime,
                with_cotizabot=with_cotizabot,
            )
            log.info("LOGO UPLOAD: WA profile result: %s", wa_profile_result)
        else:
            log.info("LOGO UPLOAD: WhatsApp not configured, skipping profile photo update")

        return {
            "ok": True,
            "logo_url": data_url,
            "wa_profile_updated": wa_profile_result.get("ok") if wa_profile_result else None,
        }
    finally:
        if cur:  cur.close()
        if conn: conn.close()


@router.get("/api/company/logo/wa-preview")
def wa_profile_preview(request: Request, with_cotizabot: bool = False):
    """Return a preview of what the WhatsApp profile photo will look like."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT logo_url FROM companies WHERE id=%s", (company_id,))
        row = cur.fetchone()
        if not row or not row[0] or not row[0].startswith("data:"):
            raise HTTPException(status_code=400, detail="Primero sube un logo")

        header, b64_data = row[0].split(",", 1)
        img_bytes = base64.b64decode(b64_data)

        if with_cotizabot:
            result_bytes = combine_with_cotizabot(img_bytes)
        else:
            result_bytes = _prepare_profile_image(img_bytes)

        result_b64 = base64.b64encode(result_bytes).decode()
        return {"ok": True, "preview": f"data:image/png;base64,{result_b64}"}
    finally:
        if cur:  cur.close()
        if conn: conn.close()


@router.post("/api/company/logo/update-wa-profile")
def update_wa_profile_from_logo(request: Request, with_cotizabot: bool = False):
    """Re-upload existing logo to WhatsApp profile, optionally with CotizaBot branding."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT logo_url, wa_api_key, wa_phone_number_id FROM companies WHERE id=%s",
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")

        logo_url, wa_key, wa_phone = row
        if not logo_url or not logo_url.startswith("data:"):
            raise HTTPException(status_code=400, detail="Primero sube un logo")
        if not wa_key or not wa_phone:
            raise HTTPException(status_code=400, detail="WhatsApp no configurado")

        # Parse data URL
        header, b64_data = logo_url.split(",", 1)
        mime_type = header.split(":")[1].split(";")[0]
        img_bytes = base64.b64decode(b64_data)

        result = update_wa_profile_photo(
            wa_api_key=wa_key,
            phone_number_id=wa_phone,
            img_bytes=img_bytes,
            mime_type=mime_type,
            with_cotizabot=with_cotizabot,
        )
        return result
    finally:
        if cur:  cur.close()
        if conn: conn.close()


@router.delete("/api/company/logo")
def delete_company_logo(request: Request):
    company_id = require_company_id(request)
    conn = None
    cur  = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "UPDATE companies SET logo_url=NULL, updated_at=now() WHERE id=%s RETURNING id",
            (company_id,),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Company no encontrada")
        conn.commit()
        return {"ok": True}
    finally:
        if cur:  cur.close()
        if conn: conn.close()
