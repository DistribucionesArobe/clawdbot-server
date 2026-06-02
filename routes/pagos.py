"""
routes/pagos.py — Payment (Mercado Pago) and promo code endpoints for CotizaExpress.

Checkout creation, payment status, webhooks, promo code CRUD.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import mercadopago
from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2 import IntegrityError
from pydantic import BaseModel

from auth import get_user_from_session, require_company_id
from db import get_conn
from routes.afiliados import process_commission

log = logging.getLogger("cotizaexpress.pagos")

router = APIRouter()


# ── Mercado Pago config ──────────────────────────────────────────────────────

_MP_ACCESS_TOKEN = (os.getenv("MP_ACCESS_TOKEN") or "").strip()

_MP_PLAN_PRICES = {
    "cotizabot":   1000.00,   # $1,000 MXN neto
    "pro":         2000.00,   # $2,000 MXN neto
    "enterprise":  4000.00,   # $4,000 MXN neto
}

_MP_PLAN_NAMES = {
    "cotizabot":   "CotizaBot - Plan Mensual",
    "pro":         "CotizaBot Pro - Plan Mensual",
    "enterprise":  "CotizaBot Enterprise - Plan Mensual",
}


# ── Pydantic models ────────────────────────────────────────────────────────

class CheckoutBody(BaseModel):
    plan: str
    success_url: str
    cancel_url: str


class PromoCodeCreate(BaseModel):
    code: str
    discount_type: str = "trial_days"
    discount_value: float = 10
    max_uses: Optional[int] = None
    one_per_customer: bool = True


class PromoCodeApply(BaseModel):
    code: str


class PromoCodeToggle(BaseModel):
    active: bool


# ── Admin helper ────────────────────────────────────────────────────────────

def _require_admin(request: Request):
    """Lazy import to avoid circular dependency."""
    from server import ADMIN_EMAILS
    u = get_user_from_session(request)
    if u["email"].lower() not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="No autorizado")
    return u


# ── Mercado Pago checkout ────────────────────────────────────────────────────

@router.post("/api/pagos/crear-checkout")
def crear_checkout(request: Request, body: CheckoutBody):
    if not _MP_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="MP_ACCESS_TOKEN no configurada")

    plan = (body.plan or "").strip().lower()
    price = _MP_PLAN_PRICES.get(plan)
    if not price:
        raise HTTPException(status_code=400, detail=f"Plan inválido: {plan}")

    company_id = require_company_id(request)

    sdk = mercadopago.SDK(_MP_ACCESS_TOKEN)
    try:
        preference_data = {
            "items": [
                {
                    "title": _MP_PLAN_NAMES.get(plan, f"CotizaBot {plan}"),
                    "quantity": 1,
                    "unit_price": price,
                    "currency_id": "MXN",
                    "description": f"Suscripción mensual CotizaExpress - Plan {plan.capitalize()}",
                }
            ],
            "back_urls": {
                "success": body.success_url,
                "failure": body.cancel_url,
                "pending": body.success_url,
            },
            "auto_return": "approved",
            "external_reference": f"{company_id}|{plan}",
            "notification_url": os.getenv("MP_WEBHOOK_URL", "").strip() or None,
            "statement_descriptor": "CotizaExpress",
        }

        # Remove notification_url if empty
        if not preference_data["notification_url"]:
            del preference_data["notification_url"]

        result = sdk.preference().create(preference_data)
        pref = result["response"]

        return {
            "ok": True,
            "checkout_url": pref["init_point"],
            "session_id": pref["id"],
        }
    except Exception as e:
        log.error("MP CHECKOUT ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=f"Error creando checkout: {str(e)}")


@router.get("/api/pagos/estado")
def pago_estado(request: Request, session_id: str = Query(None), payment_id: str = Query(None),
                collection_id: str = Query(None), collection_status: str = Query(None),
                external_reference: str = Query(None), preference_id: str = Query(None)):
    """Verifica estado de pago. Acepta parámetros de MP redirect o session_id directo."""
    if not _MP_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="MP_ACCESS_TOKEN no configurada")

    sdk = mercadopago.SDK(_MP_ACCESS_TOKEN)

    # MP redirect sends collection_id and collection_status
    _payment_id = payment_id or collection_id
    _status = collection_status

    try:
        if _payment_id:
            # Verify directly with MP API
            payment_info = sdk.payment().get(int(_payment_id))
            payment = payment_info["response"]
            paid = payment.get("status") == "approved"
            ext_ref = payment.get("external_reference", "")
            parts = ext_ref.split("|") if ext_ref else []
            plan = parts[1] if len(parts) > 1 else None
            mp_company_id = parts[0] if len(parts) > 0 else None

            # If paid, activate plan
            if paid and mp_company_id and plan:
                try:
                    conn = get_conn()
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE companies
                        SET plan_code=%s, mp_payment_id=%s, updated_at=now()
                        WHERE id=%s
                        RETURNING id
                        """,
                        (plan, str(_payment_id), mp_company_id),
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                    log.info("MP PLAN ACTIVADO: company=%s plan=%s", mp_company_id, plan)
                    # Process affiliate commission
                    try:
                        process_commission(mp_company_id, plan, str(_payment_id))
                    except Exception as ce:
                        log.error("MP ESTADO COMMISSION ERROR: %s", repr(ce))
                except Exception as e:
                    log.error("MP ESTADO DB ERROR: %s", repr(e))

            return {"ok": True, "paid": paid, "plan": plan, "status": payment.get("status", "unknown")}

        elif _status:
            # Trust the redirect params but also check external_reference
            paid = _status == "approved"
            parts = (external_reference or "").split("|")
            plan = parts[1] if len(parts) > 1 else None
            mp_company_id = parts[0] if len(parts) > 0 else None

            if paid and mp_company_id and plan:
                try:
                    conn = get_conn()
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE companies
                        SET plan_code=%s, updated_at=now()
                        WHERE id=%s
                        RETURNING id
                        """,
                        (plan, mp_company_id),
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                    log.info("MP PLAN ACTIVADO (redirect): company=%s plan=%s", mp_company_id, plan)
                except Exception as e:
                    log.error("MP ESTADO REDIRECT DB ERROR: %s", repr(e))

            return {"ok": True, "paid": paid, "plan": plan, "status": _status}

        else:
            return {"ok": False, "paid": False, "plan": None, "status": "unknown"}

    except Exception as e:
        log.error("MP ESTADO ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/pagos/webhook")
async def mp_webhook(request: Request):
    """Webhook de Mercado Pago (IPN) para notificaciones de pago."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    action = body.get("action", "")
    data_id = (body.get("data") or {}).get("id")

    log.info("MP WEBHOOK: action=%s data_id=%s", action, data_id)

    if not _MP_ACCESS_TOKEN:
        return {"ok": True}  # Silently accept if not configured

    # Only process payment notifications
    if action == "payment.created" or body.get("type") == "payment":
        if data_id:
            sdk = mercadopago.SDK(_MP_ACCESS_TOKEN)
            try:
                payment_info = sdk.payment().get(int(data_id))
                payment = payment_info["response"]
                status = payment.get("status")
                ext_ref = payment.get("external_reference", "")
                parts = ext_ref.split("|") if ext_ref else []
                company_id = parts[0] if len(parts) > 0 else None
                plan = parts[1] if len(parts) > 1 else None

                if status == "approved" and company_id and plan:
                    try:
                        conn = get_conn()
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE companies
                            SET plan_code=%s, mp_payment_id=%s, updated_at=now()
                            WHERE id=%s
                            RETURNING id
                            """,
                            (plan, str(data_id), company_id),
                        )
                        conn.commit()
                        cur.close()
                        conn.close()
                        log.info("MP WEBHOOK PLAN ACTIVADO: company=%s plan=%s", company_id, plan)
                        # Process affiliate commission
                        try:
                            process_commission(company_id, plan, str(data_id))
                        except Exception as ce:
                            log.error("MP WEBHOOK COMMISSION ERROR: %s", repr(ce))
                    except Exception as e:
                        log.error("MP WEBHOOK DB ERROR: %s", repr(e))

                elif status == "refunded" and company_id:
                    try:
                        conn = get_conn()
                        cur = conn.cursor()
                        cur.execute(
                            "UPDATE companies SET plan_code='free', updated_at=now() WHERE id=%s",
                            (company_id,),
                        )
                        conn.commit()
                        cur.close()
                        conn.close()
                        log.info("MP WEBHOOK REFUND: company=%s", company_id)
                    except Exception as e:
                        log.error("MP WEBHOOK REFUND DB ERROR: %s", repr(e))

            except Exception as e:
                log.error("MP WEBHOOK PAYMENT ERROR: %s", repr(e))

    return {"ok": True}


# ── Cancelar plan ──────────────────────────────────────────────────────────

@router.post("/api/pagos/cancelar")
def cancelar_plan(request: Request):
    """Cancelar suscripción: regresa a plan free."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE companies
            SET plan_code = 'free', trial_end = NULL, updated_at = now()
            WHERE id = %s
            RETURNING plan_code
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Empresa no encontrada")
        conn.commit()
        log.info("PLAN CANCELADO: company=%s", company_id)
        return {"ok": True, "plan_code": "free"}
    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        log.error("CANCELAR PLAN ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ── Promo Codes ─────────────────────────────────────────────────────────────

@router.post("/api/pagos/promo/crear")
def promo_crear(request: Request, body: PromoCodeCreate):
    """Admin: crear un código promo."""
    _require_admin(request)
    code = (body.code or "").strip().upper()
    if not code or len(code) < 3:
        raise HTTPException(status_code=400, detail="Código debe tener al menos 3 caracteres")

    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO promo_codes (code, discount_type, discount_value, max_uses, one_per_customer)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, code, discount_type, discount_value, max_uses, times_used, one_per_customer, active, created_at
            """,
            (code, body.discount_type, body.discount_value, body.max_uses, body.one_per_customer),
        )
        row = cur.fetchone()
        conn.commit()
        return {
            "ok": True,
            "promo": {
                "id": str(row[0]), "code": row[1], "discount_type": row[2],
                "discount_value": float(row[3]), "max_uses": row[4],
                "times_used": row[5], "one_per_customer": row[6],
                "active": row[7], "created_at": row[8].isoformat() if row[8] else None,
            },
        }
    except IntegrityError:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=409, detail=f"El código '{code}' ya existe")
    except Exception as e:
        if conn:
            conn.rollback()
        log.error("PROMO CREAR ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.get("/api/pagos/promo/listar")
def promo_listar(request: Request):
    """Admin: listar todos los códigos promo."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            """
            SELECT id, code, discount_type, discount_value, max_uses, times_used,
                   one_per_customer, active, created_at, expires_at
            FROM promo_codes
            ORDER BY created_at DESC
            """
        )
        rows = cur.fetchall()
        promos = []
        for r in rows:
            promos.append({
                "id": str(r[0]), "code": r[1], "discount_type": r[2],
                "discount_value": float(r[3]), "max_uses": r[4],
                "times_used": r[5], "one_per_customer": r[6],
                "active": r[7],
                "created_at": r[8].isoformat() if r[8] else None,
                "expires_at": r[9].isoformat() if r[9] else None,
            })
        return {"ok": True, "promos": promos}
    except Exception as e:
        log.error("PROMO LISTAR ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.patch("/api/pagos/promo/{promo_id}/toggle")
def promo_toggle(promo_id: str, request: Request, body: PromoCodeToggle):
    """Admin: activar/desactivar un código promo."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE promo_codes SET active=%s WHERE id=%s RETURNING id, code, active",
            (body.active, promo_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Código no encontrado")
        conn.commit()
        return {"ok": True, "id": str(row[0]), "active": row[2]}
    except HTTPException:
        raise
    except Exception as e:
        log.error("PROMO TOGGLE ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.delete("/api/pagos/promo/{promo_id}")
def promo_delete(promo_id: str, request: Request):
    """Admin: eliminar un código promo."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        # Borrar usos primero (FK)
        cur.execute("DELETE FROM promo_code_uses WHERE promo_code_id=%s", (promo_id,))
        cur.execute("DELETE FROM promo_codes WHERE id=%s RETURNING id", (promo_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Código no encontrado")
        conn.commit()
        return {"ok": True, "deleted": str(row[0])}
    except HTTPException:
        raise
    except Exception as e:
        log.error("PROMO DELETE ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.post("/api/pagos/promo/validar")
def promo_validar(body: PromoCodeApply):
    """Público: validar si un código es válido (para mostrar en registro)."""
    code = (body.code or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="Código requerido")
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            """
            SELECT id, code, discount_type, discount_value, max_uses, times_used, active, expires_at
            FROM promo_codes WHERE code=%s LIMIT 1
            """,
            (code,),
        )
        row = cur.fetchone()
        if not row:
            return {"ok": False, "valid": False, "reason": "Código no encontrado"}
        promo_id, p_code, dtype, dval, max_uses, times_used, active, expires_at = row
        if not active:
            return {"ok": False, "valid": False, "reason": "Código inactivo"}
        if expires_at and datetime.now(timezone.utc) > expires_at:
            return {"ok": False, "valid": False, "reason": "Código expirado"}
        if max_uses is not None and times_used >= max_uses:
            return {"ok": False, "valid": False, "reason": "Código agotado"}
        return {
            "ok": True, "valid": True,
            "discount_type": dtype,
            "discount_value": float(dval),
            "description": f"Trial gratis {int(dval)} días" if dtype == "trial_days" else f"{int(dval)}% descuento",
        }
    except Exception as e:
        log.error("PROMO VALIDAR ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@router.post("/api/pagos/promo/aplicar")
def promo_aplicar(request: Request, body: PromoCodeApply):
    """Autenticado: aplicar un código promo a la empresa del usuario."""
    company_id = require_company_id(request)
    code = (body.code or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="Código requerido")
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()

        # 1. Buscar el código
        cur.execute(
            """
            SELECT id, discount_type, discount_value, max_uses, times_used,
                   one_per_customer, active, expires_at
            FROM promo_codes WHERE code=%s LIMIT 1
            """,
            (code,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Código no encontrado")
        promo_id, dtype, dval, max_uses, times_used, one_per, active, expires_at = row

        if not active:
            raise HTTPException(status_code=400, detail="Código inactivo")
        if expires_at and datetime.now(timezone.utc) > expires_at:
            raise HTTPException(status_code=400, detail="Código expirado")
        if max_uses is not None and times_used >= max_uses:
            raise HTTPException(status_code=400, detail="Código agotado")

        # 2. Verificar si ya lo usó esta empresa
        if one_per:
            cur.execute(
                "SELECT 1 FROM promo_code_uses WHERE promo_code_id=%s AND company_id=%s LIMIT 1",
                (promo_id, company_id),
            )
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="Ya usaste este código")

        # 3. Aplicar según tipo
        now_utc = datetime.now(timezone.utc)
        if dtype == "trial_days":
            trial_days = int(dval)
            trial_end = now_utc + timedelta(days=trial_days)
            cur.execute(
                """
                UPDATE companies
                SET plan_code='pro', trial_end=%s, updated_at=now()
                WHERE id=%s
                """,
                (trial_end, company_id),
            )
            result_msg = f"Trial Pro activado por {trial_days} días (hasta {trial_end.strftime('%Y-%m-%d')})"
        else:
            # percentage — solo marcar, el descuento se aplica en checkout
            result_msg = f"Descuento de {int(dval)}% aplicado"

        # 4. Registrar uso
        cur.execute(
            "INSERT INTO promo_code_uses (promo_code_id, company_id) VALUES (%s, %s)",
            (promo_id, company_id),
        )
        cur.execute(
            "UPDATE promo_codes SET times_used = times_used + 1 WHERE id=%s",
            (promo_id,),
        )
        conn.commit()

        log.info("PROMO APLICADO: company=%s code=%s type=%s value=%s", company_id, code, dtype, dval)
        return {"ok": True, "message": result_msg, "discount_type": dtype, "discount_value": float(dval)}
    except HTTPException:
        raise
    except Exception as e:
        log.error("PROMO APLICAR ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()
