"""
routes/pagos.py — Payment (Stripe) and promo code endpoints for CotizaExpress.

Checkout creation, payment status, webhooks, promo code CRUD.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import stripe as _stripe
from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2 import IntegrityError
from pydantic import BaseModel

from auth import get_user_from_session, require_company_id
from db import get_conn

log = logging.getLogger("cotizaexpress.pagos")

router = APIRouter()


# ── Stripe config ───────────────────────────────────────────────────────────

_STRIPE_SECRET_KEY     = (os.getenv("STRIPE_SECRET_KEY") or "").strip()
_STRIPE_WEBHOOK_SECRET = (os.getenv("STRIPE_WEBHOOK_SECRET") or "").strip()

_STRIPE_PRICES = {
    "cotizabot": "price_1TCSlDF3nSPXsrl4Q05iH98d",
    "pro":        "price_1TCSlcF3nSPXsrl4mDPdBvN3",
    "enterprise": "price_1TCSlvF3nSPXsrl41CLP8xv7",
}

_PRICE_TO_PLAN = {v: k for k, v in _STRIPE_PRICES.items()}


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


# ── Stripe checkout ─────────────────────────────────────────────────────────

@router.post("/api/pagos/crear-checkout")
def crear_checkout(request: Request, body: CheckoutBody):
    if not _STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="STRIPE_SECRET_KEY no configurada")

    plan = (body.plan or "").strip().lower()
    price_id = _STRIPE_PRICES.get(plan)
    if not price_id:
        raise HTTPException(status_code=400, detail=f"Plan inválido: {plan}")

    company_id = require_company_id(request)

    _stripe.api_key = _STRIPE_SECRET_KEY
    try:
        session = _stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=body.success_url + "?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=body.cancel_url,
            metadata={"company_id": company_id, "plan": plan},
            currency="mxn",
            allow_promotion_codes=True,
        )
        return {"ok": True, "checkout_url": session.url, "session_id": session.id}
    except Exception as e:
        log.error("STRIPE CHECKOUT ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=f"Error creando checkout: {str(e)}")


@router.get("/api/pagos/estado")
def pago_estado(request: Request, session_id: str = Query(...)):
    if not _STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="STRIPE_SECRET_KEY no configurada")

    _stripe.api_key = _STRIPE_SECRET_KEY
    try:
        session = _stripe.checkout.Session.retrieve(session_id)
        # "paid" for normal payments, "no_payment_required" for 100% off promo codes
        paid = session.payment_status in ("paid", "no_payment_required")
        meta = dict(session.metadata) if session.metadata else {}
        plan = meta.get("plan")
        return {"ok": True, "paid": paid, "plan": plan, "status": session.payment_status}
    except Exception as e:
        log.error("STRIPE ESTADO ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pagos/stripe-check")
def stripe_check():
    """Temporary diagnostic: verify Stripe key works."""
    if not _STRIPE_SECRET_KEY:
        return {"ok": False, "error": "STRIPE_SECRET_KEY not set", "key_prefix": ""}
    _stripe.api_key = _STRIPE_SECRET_KEY
    try:
        # Just list 1 product to verify the key works
        products = _stripe.Product.list(limit=1)
        return {"ok": True, "stripe_version": _stripe.VERSION, "key_prefix": _STRIPE_SECRET_KEY[:8] + "..."}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {str(e)}", "key_prefix": _STRIPE_SECRET_KEY[:8] + "..."}


@router.get("/api/pagos/checkout-status/{session_id}")
def pago_checkout_status(session_id: str):
    """Frontend-facing endpoint for PagoExitoso page polling."""
    if not _STRIPE_SECRET_KEY:
        log.error("CHECKOUT STATUS: STRIPE_SECRET_KEY no configurada")
        raise HTTPException(status_code=500, detail="STRIPE_SECRET_KEY no configurada")

    _stripe.api_key = _STRIPE_SECRET_KEY
    try:
        log.info("CHECKOUT STATUS: Retrieving session %s...", session_id[:30])
        session = _stripe.checkout.Session.retrieve(session_id)
        log.info("CHECKOUT STATUS: payment_status=%s status=%s", session.payment_status, session.status)

        # "paid" for normal payments, "no_payment_required" for 100% off promo codes
        paid = session.payment_status in ("paid", "no_payment_required")
        # stripe v15: metadata is a StripeObject, convert to dict for .get()
        meta = dict(session.metadata) if session.metadata else {}
        plan = meta.get("plan")
        company_id = meta.get("company_id")
        status = session.status  # "complete", "expired", "open"

        plan_activado = False
        if paid and company_id:
            # Verify the plan was actually activated in DB
            conn = None
            cur = None
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("SELECT plan_code FROM companies WHERE id=%s LIMIT 1", (company_id,))
                row = cur.fetchone()
                if row and row[0] and row[0] != "free":
                    plan_activado = True
                else:
                    # Plan not yet activated by webhook — activate it now as fallback
                    log.warning("CHECKOUT STATUS: Plan not activated yet for %s, activating now", company_id)
                    cur.execute(
                        "UPDATE companies SET plan_code=%s, updated_at=now() WHERE id=%s",
                        (plan, company_id),
                    )
                    conn.commit()
                    plan_activado = True
            except Exception as db_err:
                log.error("CHECKOUT STATUS DB ERROR: %s", repr(db_err))
                plan_activado = paid  # Assume OK if DB check fails but Stripe says paid
            finally:
                if cur: cur.close()
                if conn: conn.close()

        _plan_names = {
            "cotizabot": "Plan Completo",
            "pro": "Plan Pro",
            "enterprise": "Plan Enterprise",
        }
        plan_name = _plan_names.get(plan, "Plan")

        return {
            "ok": True,
            "payment_status": session.payment_status,
            "status": status,
            "plan": plan,
            "plan_activado": plan_activado,
            "mensaje": f"¡Pago exitoso! Tu {plan_name} está activo." if paid else None,
        }
    except Exception as e:
        err_type = type(e).__name__
        err_msg = str(e)
        log.error("CHECKOUT STATUS ERROR: %s: %s", err_type, err_msg)
        # Return the full error detail so we can debug from the frontend
        raise HTTPException(status_code=500, detail=f"{err_type}: {err_msg}")


@router.post("/api/pagos/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not _STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="STRIPE_WEBHOOK_SECRET no configurada")

    _stripe.api_key = _STRIPE_SECRET_KEY
    try:
        event = _stripe.Webhook.construct_event(payload, sig_header, _STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        log.error("STRIPE WEBHOOK SIGNATURE ERROR: %s", repr(e))
        raise HTTPException(status_code=400, detail="Invalid signature")

    event_type = event.get("type") if hasattr(event, "get") else getattr(event, "type", None)
    log.info("STRIPE EVENT: %s", event_type)

    if event_type == "checkout.session.completed":
        session = event["data"]["object"]
        # stripe v15: objects may be StripeObject instead of dict
        meta = dict(session.get("metadata", {})) if hasattr(session, "get") else dict(getattr(session, "metadata", {}) or {})
        company_id = meta.get("company_id")
        plan       = meta.get("plan")
        stripe_customer_id = session.get("customer") if hasattr(session, "get") else getattr(session, "customer", None)

        if company_id and plan:
            try:
                conn = get_conn()
                cur  = conn.cursor()
                cur.execute(
                    """
                    UPDATE companies
                    SET plan_code=%s, stripe_customer_id=%s, updated_at=now()
                    WHERE id=%s
                    RETURNING id
                    """,
                    (plan, stripe_customer_id, company_id),
                )
                row = cur.fetchone()
                conn.commit()
                cur.close()
                conn.close()
                log.info("STRIPE PLAN ACTIVADO: company=%s plan=%s", company_id, plan)
            except Exception as e:
                log.error("STRIPE WEBHOOK DB ERROR: %s", repr(e))

    elif event_type == "customer.subscription.deleted":
        subscription = event["data"]["object"]
        stripe_customer_id = subscription.get("customer")
        if stripe_customer_id:
            try:
                conn = get_conn()
                cur  = conn.cursor()
                cur.execute(
                    "UPDATE companies SET plan_code='free', updated_at=now() WHERE stripe_customer_id=%s",
                    (stripe_customer_id,),
                )
                conn.commit()
                cur.close()
                conn.close()
                log.info("STRIPE SUSCRIPCION CANCELADA: customer=%s", stripe_customer_id)
            except Exception as e:
                log.error("STRIPE CANCEL DB ERROR: %s", repr(e))

    return {"ok": True}


# ── Promo Codes ─────────────────────────────────────────────────────────────

@router.post("/api/pagos/promo/crear")
def promo_crear(request: Request, body: PromoCodeCreate):
    """Admin: crear un código promo (DB + Stripe sync)."""
    _require_admin(request)
    code = (body.code or "").strip().upper()
    if not code or len(code) < 3:
        raise HTTPException(status_code=400, detail="Código debe tener al menos 3 caracteres")

    # 1. Create in Stripe first
    stripe_promo_id = None
    stripe_coupon_id = None
    if _STRIPE_SECRET_KEY:
        _stripe.api_key = _STRIPE_SECRET_KEY
        try:
            # Create Stripe coupon based on discount type
            # NOTE: currency is only for amount_off, NOT percent_off
            coupon_params = {
                "name": f"Promo {code}",
            }
            if body.discount_type == "trial_days":
                # For trial days: 100% off for the trial duration
                coupon_params["percent_off"] = 100.0
                coupon_params["duration"] = "repeating"
                coupon_params["duration_in_months"] = max(1, int(body.discount_value / 30))
            elif body.discount_type == "percentage":
                pct = min(float(body.discount_value), 100.0)
                coupon_params["percent_off"] = pct
                # 100% off = forever free (family/VIP codes)
                coupon_params["duration"] = "forever" if pct >= 100 else "once"
            else:
                coupon_params["percent_off"] = min(float(body.discount_value), 100.0)
                coupon_params["duration"] = "once"

            coupon = _stripe.Coupon.create(**coupon_params)
            stripe_coupon_id = coupon.id

            # Create Stripe promotion code (the actual code string)
            # Try v15+ API first (promotion={type,coupon}), fall back to legacy (coupon=)
            promo_base = {"code": code, "active": True}
            if body.max_uses is not None:
                promo_base["max_redemptions"] = body.max_uses
            if body.one_per_customer:
                promo_base["restrictions"] = {"first_time_transaction": True}

            try:
                # stripe v15+
                stripe_promo = _stripe.PromotionCode.create(
                    promotion={"type": "coupon", "coupon": coupon.id},
                    **promo_base,
                )
            except (TypeError, _stripe.InvalidRequestError):
                # stripe < v15 (legacy param)
                stripe_promo = _stripe.PromotionCode.create(
                    coupon=coupon.id,
                    **promo_base,
                )
            stripe_promo_id = stripe_promo.id
            log.info("STRIPE PROMO CREATED: code=%s coupon=%s promo=%s", code, stripe_coupon_id, stripe_promo_id)
        except Exception as e:
            log.error("STRIPE PROMO CREATE ERROR: %s", repr(e))
            raise HTTPException(
                status_code=500,
                detail=f"Error creando código en Stripe: {str(e)}. El código NO se creó."
            )

    # 2. Save in local DB
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
                "stripe_promo_id": stripe_promo_id,
                "stripe_synced": stripe_promo_id is not None,
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
    """Admin: activar/desactivar un código promo (DB + Stripe sync)."""
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

        # Sync with Stripe — find and update the promotion code
        promo_code_str = row[1]
        if _STRIPE_SECRET_KEY:
            _stripe.api_key = _STRIPE_SECRET_KEY
            try:
                # Search for the promotion code in Stripe
                stripe_promos = _stripe.PromotionCode.list(code=promo_code_str, limit=1)
                if stripe_promos.data:
                    _stripe.PromotionCode.modify(stripe_promos.data[0].id, active=body.active)
                    log.info("STRIPE PROMO TOGGLED: code=%s active=%s", promo_code_str, body.active)
            except Exception as e:
                log.error("STRIPE PROMO TOGGLE ERROR: %s", repr(e))

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
