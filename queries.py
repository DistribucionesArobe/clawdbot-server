"""
queries.py — Database queries for CotizaExpress.

Company lookups, quote state management, usage tracking, billing.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg2

from db import get_conn
from generate_quote_pdf import generate_folio

log = logging.getLogger("cotizaexpress.queries")

WA_LIMIT_COMPLETE = int((os.getenv("WA_LIMIT_COMPLETE") or "1000").strip())
WA_LIMIT_PRO = int((os.getenv("WA_LIMIT_PRO") or "2000").strip())
WA_CONVERSATION_WINDOW_HOURS = int((os.getenv("WA_CONVERSATION_WINDOW_HOURS") or "24").strip())


# ── Phone normalization ──────────────────────────────────────────────────

def normalize_wa(phone: str) -> str:
    """Strip 'whatsapp:' prefix and normalize phone."""
    return (phone or "").replace("whatsapp:", "").replace("+", "").strip()


# ── Company lookups ──────────────────────────────────────────────────────

def get_company_by_twilio_number(to_phone: str):
    to_phone = normalize_wa(to_phone)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            select c.id::text, c.name
            from channels ch
            join companies c on c.id = ch.company_id
            where ch.provider='twilio'
              and ch.channel_type='whatsapp'
              and ch.address=%s
              and ch.is_active=true
            limit 1
        """, (to_phone,))
        row = cur.fetchone()
        if not row:
            return None
        return {"company_id": row[0], "name": row[1]}
    finally:
        cur.close()
        conn.close()


def get_company_by_phone_number_id(phone_number_id: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT c.id, c.wa_api_key, c.wa_phone_number_id
            FROM channels ch
            JOIN companies c ON c.id = ch.company_id
            WHERE ch.meta_phone_number_id = %s
              AND ch.is_active = true
            LIMIT 1
            """,
            (phone_number_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"company_id": str(row[0]), "wa_api_key": row[1], "wa_phone_number_id": row[2]}
    finally:
        cur.close()
        conn.close()


# ── Quote state ──────────────────────────────────────────────────────────

def get_quote_state(company_id: str, wa_from: str):
    if not wa_from:
        return None
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT state FROM wa_quote_state WHERE company_id=%s AND wa_from=%s LIMIT 1",
            (company_id, wa_from),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except psycopg2.errors.UndefinedTable:
        return None
    finally:
        cur.close()
        conn.close()


def upsert_quote_state(company_id: str, wa_from: str, state: dict):
    if not wa_from:
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            insert into wa_quote_state(company_id, wa_from, state, updated_at)
            values (%s, %s, %s::jsonb, now())
            on conflict (company_id, wa_from)
            do update set state=excluded.state, updated_at=now()
            """,
            (company_id, wa_from, json.dumps(state)),
        )
    except psycopg2.errors.UndefinedTable:
        return
    finally:
        cur.close()
        conn.close()


def clear_quote_state(company_id: str, wa_from: str):
    if not wa_from:
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "delete from wa_quote_state where company_id=%s and wa_from=%s",
            (company_id, wa_from),
        )
    except psycopg2.errors.UndefinedTable:
        return
    finally:
        cur.close()
        conn.close()


# ── Quotes ───────────────────────────────────────────────────────────────

def save_quote(company_id: str, client_phone: str, cart: list, existing_folio: str = None) -> str:
    client_phone = (client_phone or "").replace("whatsapp:", "").strip()
    total = sum(float(it.get("price", 0)) * int(it.get("qty", 0)) for it in cart)

    items_json = [
        {
            "name":       it.get("name", ""),
            "qty":        int(it.get("qty", 0)),
            "unit":       it.get("unit", "pza"),
            "unit_price": float(it.get("price", 0)),
            "subtotal":   float(it.get("price", 0)) * int(it.get("qty", 0)),
            "sku":        it.get("sku", ""),
        }
        for it in cart
    ]

    folio = existing_folio or generate_folio()

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO quotes (folio, company_id, client_phone, items, total)
            VALUES (%s, %s::uuid, %s, %s::jsonb, %s)
            ON CONFLICT (folio) DO UPDATE
              SET items = EXCLUDED.items,
                  total = EXCLUDED.total,
                  client_phone = EXCLUDED.client_phone
            """,
            (folio, company_id, client_phone, json.dumps(items_json), total),
        )
    except Exception as e:
        log.error("SAVE QUOTE ERROR: %s", repr(e))
    finally:
        cur.close()
        conn.close()

    return folio


# ── Billing / usage tracking ────────────────────────────────────────────

def _year_month_utc() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def get_company_plan_code(company_id: str) -> str:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT plan_code, trial_end FROM companies WHERE id=%s LIMIT 1", (company_id,))
        row = cur.fetchone()
        if not row:
            return "free"
        plan_code = (row[0] or "free").strip()
        trial_end = row[1]
        if trial_end and plan_code in ("pro", "complete"):
            if datetime.now(timezone.utc) > trial_end:
                try:
                    cur.execute(
                        "UPDATE companies SET plan_code='free', trial_end=NULL, updated_at=now() WHERE id=%s",
                        (company_id,),
                    )
                    log.warning("TRIAL EXPIRADO: company=%s plan=%s -> free", company_id, plan_code)
                except Exception as e:
                    log.error("TRIAL EXPIRE ERROR: %s", repr(e))
                return "free"
        return plan_code
    finally:
        cur.close()
        conn.close()


def get_plan_limit(plan_code: str) -> int:
    p = (plan_code or "free").strip().lower()
    if p == "complete":
        return WA_LIMIT_COMPLETE
    if p == "pro":
        return WA_LIMIT_PRO
    return 0


def get_monthly_usage(company_id: str, year_month: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT conversations_count FROM wa_usage_monthly WHERE company_id=%s AND year_month=%s LIMIT 1",
            (company_id, year_month),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except psycopg2.errors.UndefinedTable:
        return 0
    finally:
        cur.close()
        conn.close()


def increment_monthly_usage(company_id: str, year_month: str, delta: int = 1) -> int:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO wa_usage_monthly(company_id, year_month, conversations_count, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (company_id, year_month)
            DO UPDATE SET conversations_count = wa_usage_monthly.conversations_count + EXCLUDED.conversations_count,
                          updated_at = now()
            RETURNING conversations_count
            """,
            (company_id, year_month, int(delta)),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except psycopg2.errors.UndefinedTable:
        return 0
    finally:
        cur.close()
        conn.close()


def track_conversation_if_new(company_id: str, wa_from: str) -> dict:
    wa_from = (wa_from or "").strip()
    if not wa_from:
        return {"counted": False, "usage": 0, "limit": 0, "plan_code": "free", "year_month": _year_month_utc()}
    plan_code = get_company_plan_code(company_id)
    limit = get_plan_limit(plan_code)
    ym = _year_month_utc()
    if limit <= 0:
        return {"counted": False, "usage": get_monthly_usage(company_id, ym), "limit": limit, "plan_code": plan_code, "year_month": ym}
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT last_started_at FROM wa_conversation_windows WHERE company_id=%s AND wa_from=%s LIMIT 1",
            (company_id, wa_from),
        )
        row = cur.fetchone()
        now_utc = datetime.now(timezone.utc)
        window_hours = WA_CONVERSATION_WINDOW_HOURS
        should_count = False
        if not row:
            should_count = True
        else:
            last_started_at = row[0]
            if last_started_at is None:
                should_count = True
            else:
                age = now_utc - last_started_at
                if age.total_seconds() >= window_hours * 3600:
                    should_count = True
        if should_count:
            cur.execute(
                """
                INSERT INTO wa_conversation_windows(company_id, wa_from, last_started_at, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (company_id, wa_from)
                DO UPDATE SET last_started_at=EXCLUDED.last_started_at, updated_at=now()
                """,
                (company_id, wa_from, now_utc),
            )
        usage = get_monthly_usage(company_id, ym)
        if should_count:
            usage = increment_monthly_usage(company_id, ym, 1)
        return {"counted": bool(should_count), "usage": int(usage), "limit": int(limit), "plan_code": plan_code, "year_month": ym}
    except psycopg2.errors.UndefinedTable:
        return {"counted": False, "usage": 0, "limit": limit, "plan_code": plan_code, "year_month": ym}
    finally:
        cur.close()
        conn.close()
