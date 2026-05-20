"""
routes/afiliados.py — Affiliate program endpoints for CotizaExpress.

Affiliate registration, referral tracking, commission calculation, and admin management.
Commission model: 1 month upfront bonus + 15% recurring monthly.
"""

import logging
import os
import uuid
import secrets
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, EmailStr

from auth import get_user_from_session, require_company_id
from db import get_conn

log = logging.getLogger("cotizaexpress.afiliados")

router = APIRouter()


# ── Config ───────────────────────────────────────────────────────────────────

COMISION_RECURRENTE = 0.15  # 15% recurring monthly
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://cotizaexpress.com")

_PLAN_PRICES_MXN = {
    "cotizabot":   1000.00,   # Precio base sin IVA
    "pro":         2000.00,
    "enterprise":  4000.00,
}


# ── Pydantic models ────────────────────────────────────────────────────────

class AfiliadoRegistro(BaseModel):
    nombre: str
    email: str
    telefono: str
    empresa: Optional[str] = None
    zona: Optional[str] = None  # Zona geográfica que cubre
    notas: Optional[str] = None


class AfiliadoUpdate(BaseModel):
    nombre: Optional[str] = None
    telefono: Optional[str] = None
    empresa: Optional[str] = None
    zona: Optional[str] = None
    activo: Optional[bool] = None
    mp_email: Optional[str] = None  # Email de Mercado Pago para recibir comisiones


# ── Admin helper ────────────────────────────────────────────────────────────

def _require_admin(request: Request):
    from server import ADMIN_EMAILS
    u = get_user_from_session(request)
    if u["email"].lower() not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="No autorizado")
    return u


def _generate_referral_code(nombre: str) -> str:
    """Generate a unique, friendly referral code based on affiliate name."""
    # Take first part of name, clean it, add random suffix
    base = nombre.strip().split()[0].lower()[:8]
    # Remove accents and special chars
    import unicodedata
    base = unicodedata.normalize('NFKD', base).encode('ascii', 'ignore').decode('ascii')
    base = ''.join(c for c in base if c.isalnum())
    if not base:
        base = "ref"
    suffix = secrets.token_hex(3)  # 6 char hex
    return f"{base}-{suffix}"


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC: Affiliate Registration
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/api/afiliados/registro")
def afiliado_registro(body: AfiliadoRegistro):
    """Public: register as a new affiliate."""
    email = (body.email or "").strip().lower()
    nombre = (body.nombre or "").strip()
    telefono = (body.telefono or "").strip()

    if not email or not nombre or not telefono:
        raise HTTPException(status_code=400, detail="Nombre, email y teléfono son requeridos")

    referral_code = _generate_referral_code(nombre)

    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()

        # Check if email already registered
        cur.execute("SELECT id FROM affiliates WHERE email=%s LIMIT 1", (email,))
        if cur.fetchone():
            raise HTTPException(status_code=409, detail="Este email ya está registrado como afiliado")

        cur.execute(
            """
            INSERT INTO affiliates (nombre, email, telefono, empresa, zona, notas, referral_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, referral_code, created_at
            """,
            (nombre, email, telefono, body.empresa, body.zona, body.notas, referral_code),
        )
        row = cur.fetchone()
        conn.commit()

        affiliate_id = str(row[0])
        code = row[1]
        referral_link = f"{FRONTEND_URL}/registro?ref={code}"

        log.info("AFILIADO REGISTRADO: id=%s email=%s code=%s", affiliate_id, email, code)

        return {
            "ok": True,
            "affiliate": {
                "id": affiliate_id,
                "nombre": nombre,
                "email": email,
                "referral_code": code,
                "referral_link": referral_link,
            },
            "mensaje": f"¡Registro exitoso! Tu código de referido es: {code}",
        }
    except HTTPException:
        raise
    except Exception as e:
        if conn: conn.rollback()
        log.error("AFILIADO REGISTRO ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC: Validate referral code (used during company registration)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/api/afiliados/validar-codigo")
def validar_codigo(ref: str = Query(...)):
    """Validate a referral code and return affiliate name."""
    code = (ref or "").strip().lower()
    if not code:
        return {"ok": False, "valid": False}

    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT id, nombre, activo FROM affiliates WHERE referral_code=%s LIMIT 1",
            (code,),
        )
        row = cur.fetchone()
        if not row:
            return {"ok": False, "valid": False, "reason": "Código no encontrado"}
        if not row[2]:
            return {"ok": False, "valid": False, "reason": "Afiliado inactivo"}

        return {
            "ok": True,
            "valid": True,
            "affiliate_name": row[1],
            "mensaje": f"Referido por: {row[1]}",
        }
    except Exception as e:
        log.error("VALIDAR CODIGO ERROR: %s", repr(e))
        return {"ok": False, "valid": False}
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL: Track referral when a company registers with ?ref= code
# ══════════════════════════════════════════════════════════════════════════════

def track_referral(company_id: str, referral_code: str):
    """Called from registration endpoint to link a company to an affiliate.
    This is NOT an API endpoint — it's called internally."""
    if not referral_code:
        return

    code = referral_code.strip().lower()
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT id FROM affiliates WHERE referral_code=%s AND activo=TRUE LIMIT 1",
            (code,),
        )
        row = cur.fetchone()
        if not row:
            log.warning("TRACK REFERRAL: code=%s not found or inactive", code)
            return

        affiliate_id = row[0]

        # Check if already tracked
        cur.execute(
            "SELECT 1 FROM affiliate_referrals WHERE company_id=%s LIMIT 1",
            (company_id,),
        )
        if cur.fetchone():
            log.warning("TRACK REFERRAL: company=%s already tracked", company_id)
            return

        cur.execute(
            """
            INSERT INTO affiliate_referrals (affiliate_id, company_id, referral_code)
            VALUES (%s, %s, %s)
            """,
            (affiliate_id, company_id, code),
        )
        conn.commit()
        log.info("REFERRAL TRACKED: affiliate=%s company=%s code=%s", affiliate_id, company_id, code)
    except Exception as e:
        log.error("TRACK REFERRAL ERROR: %s", repr(e))
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL: Calculate and record commission when a payment is received
# ══════════════════════════════════════════════════════════════════════════════

def process_commission(company_id: str, plan: str, payment_id: str):
    """Called from pagos webhook when a payment is approved.
    Calculates commission for the affiliate (if any) and records it."""
    if not company_id or not plan:
        return

    base_price = _PLAN_PRICES_MXN.get(plan)
    if not base_price:
        return

    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()

        # Find affiliate for this company
        cur.execute(
            """
            SELECT ar.affiliate_id, ar.id as referral_id, a.nombre, a.email
            FROM affiliate_referrals ar
            JOIN affiliates a ON a.id = ar.affiliate_id
            WHERE ar.company_id = %s AND a.activo = TRUE
            LIMIT 1
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            return  # No affiliate for this company

        affiliate_id, referral_id, aff_name, aff_email = row

        # Check if this is the first payment (upfront bonus) or recurring
        cur.execute(
            "SELECT COUNT(*) FROM affiliate_commissions WHERE referral_id=%s",
            (referral_id,),
        )
        payment_count = cur.fetchone()[0]

        if payment_count == 0:
            # First payment: upfront bonus = 1 full month base price
            commission_amount = base_price
            commission_type = "upfront"
            description = f"Bono primer mes - Plan {plan} (${base_price:,.0f} MXN)"
        else:
            # Recurring: 15% of base price
            commission_amount = round(base_price * COMISION_RECURRENTE, 2)
            commission_type = "recurring"
            description = f"Comisión mensual {int(COMISION_RECURRENTE*100)}% - Plan {plan} (${commission_amount:,.0f} MXN)"

        cur.execute(
            """
            INSERT INTO affiliate_commissions
                (affiliate_id, referral_id, company_id, payment_id, plan, commission_type,
                 base_amount, commission_amount, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (affiliate_id, referral_id, company_id, payment_id, plan,
             commission_type, base_price, commission_amount, description),
        )
        commission_id = cur.fetchone()[0]

        # Update affiliate totals
        cur.execute(
            """
            UPDATE affiliates
            SET total_earned = total_earned + %s,
                total_referrals = (SELECT COUNT(DISTINCT company_id) FROM affiliate_referrals WHERE affiliate_id=%s),
                updated_at = now()
            WHERE id = %s
            """,
            (commission_amount, affiliate_id, affiliate_id),
        )
        conn.commit()

        log.info(
            "COMMISSION CREATED: affiliate=%s (%s) type=%s amount=$%.2f plan=%s company=%s",
            affiliate_id, aff_name, commission_type, commission_amount, plan, company_id
        )
    except Exception as e:
        log.error("PROCESS COMMISSION ERROR: %s", repr(e))
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# AFFILIATE DASHBOARD: View own stats (authenticated by email token)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/api/afiliados/mi-panel")
def mi_panel(email: str = Query(...), token: str = Query(...)):
    """Affiliate dashboard: view referrals, commissions, stats."""
    email = email.strip().lower()
    token = token.strip()

    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()

        # Verify affiliate + token
        cur.execute(
            "SELECT id, nombre, referral_code, total_earned, total_referrals, activo FROM affiliates WHERE email=%s AND access_token=%s LIMIT 1",
            (email, token),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        affiliate_id, nombre, referral_code, total_earned, total_referrals, activo = row
        referral_link = f"{FRONTEND_URL}/registro?ref={referral_code}"

        # Get referrals with company info
        cur.execute(
            """
            SELECT ar.company_id, c.name as company_name, c.plan_code, ar.created_at,
                   COALESCE(SUM(ac.commission_amount), 0) as total_comision
            FROM affiliate_referrals ar
            LEFT JOIN companies c ON c.id = ar.company_id
            LEFT JOIN affiliate_commissions ac ON ac.referral_id = ar.id
            WHERE ar.affiliate_id = %s
            GROUP BY ar.company_id, c.name, c.plan_code, ar.created_at
            ORDER BY ar.created_at DESC
            """,
            (affiliate_id,),
        )
        referrals = []
        for r in cur.fetchall():
            referrals.append({
                "company_name": r[1] or "Sin nombre",
                "plan": r[2] or "free",
                "fecha_registro": r[3].isoformat() if r[3] else None,
                "total_comision": float(r[4]),
            })

        # Get recent commissions
        cur.execute(
            """
            SELECT commission_type, commission_amount, description, status, created_at
            FROM affiliate_commissions
            WHERE affiliate_id = %s
            ORDER BY created_at DESC
            LIMIT 20
            """,
            (affiliate_id,),
        )
        commissions = []
        for r in cur.fetchall():
            commissions.append({
                "tipo": r[0],
                "monto": float(r[1]),
                "descripcion": r[2],
                "status": r[3],
                "fecha": r[4].isoformat() if r[4] else None,
            })

        # Pending vs paid totals
        cur.execute(
            """
            SELECT status, COALESCE(SUM(commission_amount), 0)
            FROM affiliate_commissions
            WHERE affiliate_id = %s
            GROUP BY status
            """,
            (affiliate_id,),
        )
        totals_by_status = {}
        for r in cur.fetchall():
            totals_by_status[r[0]] = float(r[1])

        return {
            "ok": True,
            "affiliate": {
                "nombre": nombre,
                "referral_code": referral_code,
                "referral_link": referral_link,
                "total_earned": float(total_earned),
                "total_referrals": total_referrals,
                "activo": activo,
                "pendiente_pago": totals_by_status.get("pending", 0),
                "pagado": totals_by_status.get("paid", 0),
            },
            "referrals": referrals,
            "commissions": commissions,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("MI PANEL ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: Manage affiliates
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/api/afiliados/admin/listar")
def admin_listar_afiliados(request: Request):
    """Admin: list all affiliates with stats."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            """
            SELECT a.id, a.nombre, a.email, a.telefono, a.empresa, a.zona,
                   a.referral_code, a.total_earned, a.total_referrals, a.activo,
                   a.created_at, a.mp_email,
                   COALESCE((SELECT SUM(ac.commission_amount) FROM affiliate_commissions ac WHERE ac.affiliate_id=a.id AND ac.status='pending'), 0) as pendiente
            FROM affiliates a
            ORDER BY a.total_earned DESC, a.created_at DESC
            """
        )
        rows = cur.fetchall()
        afiliados = []
        for r in rows:
            afiliados.append({
                "id": str(r[0]), "nombre": r[1], "email": r[2], "telefono": r[3],
                "empresa": r[4], "zona": r[5], "referral_code": r[6],
                "total_earned": float(r[7]), "total_referrals": r[8], "activo": r[9],
                "created_at": r[10].isoformat() if r[10] else None,
                "mp_email": r[11], "pendiente_pago": float(r[12]),
            })
        return {"ok": True, "afiliados": afiliados}
    except Exception as e:
        log.error("ADMIN LISTAR AFILIADOS ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.patch("/api/afiliados/admin/{affiliate_id}")
def admin_update_afiliado(affiliate_id: str, request: Request, body: AfiliadoUpdate):
    """Admin: update affiliate details."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()

        updates = []
        params = []
        if body.nombre is not None:
            updates.append("nombre=%s"); params.append(body.nombre)
        if body.telefono is not None:
            updates.append("telefono=%s"); params.append(body.telefono)
        if body.empresa is not None:
            updates.append("empresa=%s"); params.append(body.empresa)
        if body.zona is not None:
            updates.append("zona=%s"); params.append(body.zona)
        if body.activo is not None:
            updates.append("activo=%s"); params.append(body.activo)
        if body.mp_email is not None:
            updates.append("mp_email=%s"); params.append(body.mp_email)

        if not updates:
            raise HTTPException(status_code=400, detail="Nada que actualizar")

        updates.append("updated_at=now()")
        params.append(affiliate_id)

        cur.execute(
            f"UPDATE affiliates SET {', '.join(updates)} WHERE id=%s RETURNING id, nombre, activo",
            params,
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Afiliado no encontrado")
        conn.commit()

        return {"ok": True, "id": str(row[0]), "nombre": row[1], "activo": row[2]}
    except HTTPException:
        raise
    except Exception as e:
        log.error("ADMIN UPDATE AFILIADO ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.post("/api/afiliados/admin/{affiliate_id}/marcar-pagado")
def admin_marcar_pagado(affiliate_id: str, request: Request):
    """Admin: mark all pending commissions as paid for an affiliate."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            """
            UPDATE affiliate_commissions
            SET status='paid', paid_at=now()
            WHERE affiliate_id=%s AND status='pending'
            RETURNING id
            """,
            (affiliate_id,),
        )
        count = cur.rowcount
        conn.commit()

        log.info("ADMIN MARCAR PAGADO: affiliate=%s commissions=%d", affiliate_id, count)
        return {"ok": True, "commissions_paid": count}
    except Exception as e:
        log.error("ADMIN MARCAR PAGADO ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.get("/api/afiliados/admin/resumen")
def admin_resumen(request: Request):
    """Admin: affiliate program summary stats."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM affiliates WHERE activo=TRUE")
        total_afiliados = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT company_id) FROM affiliate_referrals")
        total_referidos = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(commission_amount), 0) FROM affiliate_commissions")
        total_comisiones = float(cur.fetchone()[0])

        cur.execute("SELECT COALESCE(SUM(commission_amount), 0) FROM affiliate_commissions WHERE status='pending'")
        pendiente_pago = float(cur.fetchone()[0])

        cur.execute("SELECT COALESCE(SUM(commission_amount), 0) FROM affiliate_commissions WHERE status='paid'")
        total_pagado = float(cur.fetchone()[0])

        # Top affiliates
        cur.execute(
            """
            SELECT a.nombre, a.total_earned, a.total_referrals
            FROM affiliates a
            WHERE a.activo = TRUE
            ORDER BY a.total_earned DESC
            LIMIT 5
            """
        )
        top = [{"nombre": r[0], "total_earned": float(r[1]), "referrals": r[2]} for r in cur.fetchall()]

        return {
            "ok": True,
            "resumen": {
                "total_afiliados": total_afiliados,
                "total_referidos": total_referidos,
                "total_comisiones": total_comisiones,
                "pendiente_pago": pendiente_pago,
                "total_pagado": total_pagado,
                "top_afiliados": top,
            },
        }
    except Exception as e:
        log.error("ADMIN RESUMEN ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()
