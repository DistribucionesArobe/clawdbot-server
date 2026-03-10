from prompts_cotizabot import COTIZABOT_SYSTEM_PROMPT
from fastapi.background import BackgroundTasks
import json
import os
import re
import hashlib
import string
import secrets
import traceback
import requests
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Optional
from twilio.rest import Client
from rapidfuzz import fuzz

import bcrypt
import psycopg2
from psycopg2 import IntegrityError

from openai import OpenAI
from semantic_search import smart_search, rebuild_embeddings_for_company, upsert_single_embedding

from fastapi import (
    FastAPI,
    Header,
    HTTPException,
    Request,
    Response,
    UploadFile,
    File,
    Query,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from openpyxl import load_workbook, Workbook

from fastapi import Form

# -------------------------
# Prompts (otras apps)
# -------------------------
DONDEVER_SYSTEM_PROMPT = """
Eres DóndeVer..
Tu trabajo es decir dónde ver partidos según país (MX/USA).
Reglas:
- Pregunta solo lo mínimo: partido y país.
- Responde con lista clara de opciones oficiales.
- Si no estás seguro, dilo y pide el dato faltante.
"""

ENTIENDEUSA_SYSTEM_PROMPT = """
Eres EntiendeUSA.
Traduces y explicas textos ES/EN de forma natural.
- Mantén el sentido original.
- Si hay ambigüedad, ofrece opciones.
- Si es para enviar, entrégalo listo para copiar.
"""


# -------------------------
# App
# -------------------------
app = FastAPI(title="Clawdbot Server", version="1.0")


# -------------------------
# Config
# -------------------------
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

API_KEY_PREFIX_LEN = 10
SESSION_COOKIE_NAME = "session"
SESSION_TTL_DAYS = int((os.getenv("SESSION_TTL_DAYS") or "14").strip())
DEFAULT_COMPANY_ID = (os.getenv("DEFAULT_COMPANY_ID") or "").strip()
WA_LIMIT_COMPLETE = int((os.getenv("WA_LIMIT_COMPLETE") or "1000").strip())
WA_LIMIT_PRO = int((os.getenv("WA_LIMIT_PRO") or "2000").strip())
WA_CONVERSATION_WINDOW_HOURS = int((os.getenv("WA_CONVERSATION_WINDOW_HOURS") or "24").strip())


# -------------------------
# Helpers password
# -------------------------
def _pw_bytes(password: str) -> bytes:
    return (password or "").strip().encode("utf-8")


def hash_password(password: str) -> str:
    pw = _pw_bytes(password)
    if len(pw) > 72:
        raise HTTPException(status_code=400, detail="Password demasiado largo (máx 72 bytes)")
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(pw, salt).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    pw = _pw_bytes(password)
    if len(pw) > 72:
        return False
    if not password_hash:
        return False
    return bcrypt.checkpw(pw, password_hash.encode("utf-8"))

def looks_like_product_phrase(text: str) -> bool:
    t = norm_name(text)
    if not t:
        return False

    t = t.replace("cotización", "cotizacion")
    t = re.sub(r"[^a-z0-9áéíóúñü\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    stop_phrases = {
        "hola", "buenas", "buenos dias", "buenas tardes", "buenas noches",
        "gracias", "muchas gracias", "ok", "va", "sale", "listo", "perfecto",
        "dale", "ayuda", "menu", "inicio", "salir", "cancelar", "cancel",
        "reiniciar", "reset", "nueva cotizacion", "nueva cotizacion por favor",
        "nueva cotizacion porfa", "empezar de nuevo", "borrar carrito",
        "vaciar carrito", "limpiar carrito",
    }

    if t in stop_phrases:
        return False

    strong_cmds = {
        "salir", "cancelar", "cancel", "reiniciar", "reset", "nueva cotizacion",
        "empezar de nuevo", "borrar carrito", "vaciar carrito", "limpiar carrito",
    }
    if any(cmd in t for cmd in strong_cmds):
        return False

    tokens = [w for w in t.split() if len(w) >= 3]
    if not tokens:
        return False

    blacklist_tokens = {
        "hola", "buenas", "gracias", "ok", "sale", "perfecto", "listo", "vale",
        "va", "dale", "porfa", "favor", "por", "si", "no", "quiero", "necesito",
        "dame", "manda", "pasame", "cotiza", "cotizar", "cotizacion", "precio",
        "precios", "salir", "cancelar", "cancel", "reiniciar", "reset", "nueva",
        "inicio", "menu", "ayuda",
    }

    if all(tok in blacklist_tokens for tok in tokens):
        return False

    return True

def normalize_wa(addr: str) -> str:
    a = (addr or "").strip().replace(" ", "")
    if a and not a.startswith("whatsapp:"):
        a = "whatsapp:" + a
    return a

def norm_name(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

# -------------------------
# Normalización universal
# -------------------------

SPANISH_STOPWORDS = {
    "de", "del", "la", "el", "un", "una", "unos", "unas",
    "por", "para", "con", "sin", "y", "o", "me", "dime", "oye",
    "precio", "precios", "cuanto", "cuánto", "cuesta", "vale", "costo", "cost",
    "cotiza", "cotizacion", "cotización", "presupuesto", "lista", "saber",
    "quiero", "necesito", "dame", "manda", "pasame", "pásame",
}

SYNONYMS = {
    "pste": "poste", "psts": "postes", "ptr": "poste",
    "tblrc": "tablaroca", "tablarok": "tablaroca",
    "durok": "durock", "perf": "perfacinta", "perfa": "perfacinta",
}

def singularize_token(tok: str) -> str:
    t = (tok or "").strip()
    if len(t) < 5:
        return t
    if re.search(r"\d", t):
        return t
    if t.endswith("llas"):
        return t[:-1]
    if t.endswith("illas"):
        return t[:-1]
    if t.endswith("es"):
        base = t[:-2]
        if len(base) >= 3:
            return base
    if t.endswith("s"):
        base = t[:-1]
        if len(base) >= 3:
            return base
    return t

def normalize_product_text(text: str) -> str:
    t = (text or "").lower().strip()
    for k, v in SYNONYMS.items():
        t = re.sub(rf"\b{re.escape(k)}\b", v, t)
    t = re.sub(r"[^a-z0-9áéíóúñü\.\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    out_tokens = []
    for w in t.split():
        if not w:
            continue
        if w in SPANISH_STOPWORDS:
            continue
        if re.match(r"^\d+(?:\.\d+)?$", w):
            out_tokens.append(w)
            continue
        out_tokens.append(singularize_token(w))
    return " ".join(out_tokens).strip()

def twilio_client():
    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not sid or not token:
        raise RuntimeError("Falta TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN en Render")
    return Client(sid, token)


def twilio_send_whatsapp(to_user_whatsapp: str, text: str):
    client = twilio_client()
    msid = (os.getenv("TWILIO_MESSAGING_SERVICE_SID") or "").strip()
    if not msid:
        raise RuntimeError("Falta TWILIO_MESSAGING_SERVICE_SID en env vars")
    to_user_whatsapp = (to_user_whatsapp or "").strip()
    if not to_user_whatsapp.startswith("whatsapp:"):
        to_user_whatsapp = f"whatsapp:{to_user_whatsapp}"
    text = (text or "").strip()
    if not text:
        raise ValueError("text vacío")
    client.messages.create(messaging_service_sid=msid, to=to_user_whatsapp, body=text)

def extract_product_query(text: str) -> str:
    t = normalize_product_text(text)
    if not t:
        raw = (text or "").lower().strip()
        raw = re.sub(r"[^a-z0-9áéíóúñü\.\s]", " ", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw
    return t


def extract_qty_and_product(text: str):
    t = (text or "").strip().lower()
    m = re.match(r"^\s*(\d+)(?!\.)\s+(.+?)\s*$", t)
    if not m:
        return None, None
    qty = int(m.group(1))
    product = m.group(2).strip()
    return qty, product


def is_specs_only(text: str) -> bool:
    t = norm_name(text)
    if not t:
        return False
    has_measure = bool(re.search(r"\b\d+(?:\.\d+)?\b", t))
    has_spec_word = any(w in t for w in ["cal", "calibre", "mm", "cm", "mts", "m", "pulg", "pulgada", "x"])
    looks_product = looks_like_product_phrase(t)
    return (has_measure or has_spec_word) and not looks_product

def split_clarifications(text: str):
    t = (text or "").strip()
    chunks = [c.strip() for c in re.split(r"[\n\r]+", t) if c.strip()]
    out = []
    for ch in chunks:
        s = ch.lower().strip()
        s = s.replace("+", " ")
        s = re.sub(r"\s+", " ", s)
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for p in parts:
            out.extend([x.strip() for x in re.split(r"\s+y\s+", p) if x.strip()])
    return [x for x in out if x]

def looks_like_price_question(text: str) -> bool:
    t = (text or "").lower()
    triggers = [
        "precio", "cuánto", "cuanto", "vale", "costo", "cost",
        "$", "cotiza", "cotización", "cotizacion", "presupuesto",
        "lista de precios", "price", "cuesta",
    ]
    return any(x in t for x in triggers)

def looks_like_hours_question(text: str) -> bool:
    t = norm_name(text)
    return any(x in t for x in [
        "a que hora", "a qué hora", "que hora abren", "qué hora abren",
        "horario", "horarios", "abren hoy", "abren mañana", "abierto", "abiertos",
        "cierran", "cierre", "ubicacion", "ubicación", "direccion", "dirección",
    ])


# -------------------------
# API keys
# -------------------------
def generate_api_key() -> str:
    return secrets.token_urlsafe(32)

def cart_add_item(state: dict, item: dict):
    state = state or {}
    cart = state.get("cart") or []
    sku = (item.get("sku") or "").strip()
    name = (item.get("name") or "").strip()
    unit = (item.get("unit") or "unidad").strip()
    price = float(item.get("price") or 0.0)
    vat_rate = item.get("vat_rate")
    vat_rate = float(vat_rate) if vat_rate is not None else 0.16
    qty = int(item.get("qty") or 0)
    if qty <= 0:
        return state
    merged = False
    for it in cart:
        if sku and it.get("sku") == sku:
            it["qty"] = int(it.get("qty") or 0) + qty
            merged = True
            break
        if (not sku) and it.get("name") == name:
            it["qty"] = int(it.get("qty") or 0) + qty
            merged = True
            break
    if not merged:
        cart.append({"sku": sku, "name": name, "unit": unit, "price": price, "vat_rate": vat_rate, "qty": qty})
    state["cart"] = cart
    return state

def extract_specs(text: str):
    t = norm_name(text)
    m = re.search(r"\b(\d+(?:\.\d+)?)\b", t)
    medida = m.group(1) if m else None
    cal = None
    mc = re.search(r"\bcal(?:ibre)?\s*(\d+)\b", t)
    if mc:
        cal = mc.group(1)
    return {"medida": medida, "cal": cal}

def passes_constraints(item_name: str, specs: dict) -> bool:
    n = norm_name(item_name)
    medida = specs.get("medida")
    cal = specs.get("cal")
    if medida and medida not in n:
        return False
    if cal and (f"cal {cal}" not in n and f"cal{cal}" not in n):
        return False
    return True

def rank_best_match(query: str, items: list):
    qn = norm_name(query)
    scored = []
    for it in items:
        name = it.get("name") or ""
        sn = norm_name(name)
        sku = norm_name(it.get("sku") or "")
        s1 = fuzz.token_set_ratio(qn, sn)
        s2 = fuzz.token_set_ratio(qn, sku) if sku else 0
        score = max(s1, s2 + 5)
        scored.append((score, it))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1] if scored else None, (scored[0][0] if scored else 0)

def search_pricebook_best(conn, company_id: str, q: str, limit: int = 12):
    q = (q or "").strip()
    if not q:
        return None
    q_clean = extract_product_query(q)
    qn = norm_name(q_clean)
    specs = extract_specs(q_clean)
    tokens = [t for t in qn.split() if len(t) >= 3] or [qn]
    where_parts = []
    params = [company_id]
    for tok in tokens[:6]:
        where_parts.append("name_norm LIKE %s")
        params.append(f"%{tok}%")
    where_sql = " AND ".join(where_parts)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT sku, name, unit, price, vat_rate, updated_at
            FROM pricebook_items
            WHERE company_id=%s
              AND ({where_sql} OR sku ILIKE %s OR name ILIKE %s)
            LIMIT %s
            """,
            (*params, f"%{q_clean}%", f"%{q_clean}%", max(limit, 12)),
        )
        rows = cur.fetchall()
        items = []
        for sku, name, unit, price, vat_rate, _updated_at in rows:
            items.append({"sku": sku, "name": name, "unit": unit,
                          "price": float(price) if price is not None else None,
                          "vat_rate": float(vat_rate) if vat_rate is not None else None})
        if not items:
            return None
        constrained = [it for it in items if passes_constraints(it["name"], specs)]
        pool = constrained if constrained else items
        best, score = rank_best_match(q_clean, pool)
        if not best or score < 72:
            return None
        return best
    finally:
        cur.close()

def search_pricebook_candidates(conn, company_id: str, q: str, limit: int = 5):
    q = (q or "").strip()
    if not q:
        return []
    q_clean = extract_product_query(q)
    qn = norm_name(q_clean)
    tokens = [t for t in qn.split() if len(t) >= 3] or [qn]
    where_parts = []
    params = [company_id]
    for tok in tokens[:6]:
        where_parts.append("name_norm LIKE %s")
        params.append(f"%{tok}%")
    where_sql = " OR ".join(where_parts)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT sku, name, unit, price, vat_rate, synonyms
            FROM pricebook_items
            WHERE company_id=%s
              AND ({where_sql} OR sku ILIKE %s OR name ILIKE %s OR synonyms ILIKE %s)
            LIMIT 30
            """,
            (*params, f"%{q_clean}%", f"%{q_clean}%", f"%{q_clean}%"),
        )
        rows = cur.fetchall()
        items = []
        for sku, name, unit, price, vat_rate, synonyms in rows:
            it = {"sku": sku, "name": name, "unit": unit,
                  "price": float(price) if price is not None else None,
                  "vat_rate": float(vat_rate) if vat_rate is not None else None}
            sn = norm_name(name or "")
            sku_n = norm_name(sku or "")
            syn_n = norm_name(synonyms or "")
            it["_score"] = max(
                fuzz.token_set_ratio(qn, sn),
                fuzz.token_set_ratio(qn, sku_n) if sku else 0,
                fuzz.token_set_ratio(qn, syn_n) if synonyms else 0,
            )
            items.append(it)
        items.sort(key=lambda x: x.get("_score", 0), reverse=True)
        out = []
        for it in items[:max(1, int(limit or 5))]:
            it.pop("_score", None)
            out.append(it)
        return out
    finally:
        cur.close()

def render_pending_suggestions(pending: list) -> str:
    if not pending:
        return ""
    letters = string.ascii_uppercase
    lines = ["¿Cuál de estas opciones buscas? 👇\n\nSi no encuentras lo que necesitas, escribe *asesor*."]
    for i, p in enumerate(pending[:6]):
        tag = letters[i]
        qty = int(p.get("qty") or 0)
        raw = (p.get("raw") or "").strip()
        cands = p.get("candidates") or []
        lines.append(f"\n❓ ({tag}) {qty} x {raw}")
        if not cands:
            lines.append("   (sin sugerencias) Mándalo más exacto o escribe *asesor*.")
            continue
        for j, it in enumerate(cands[:5], start=1):
            unit = it.get("unit") or "unidad"
            price = float(it.get("price") or 0.0)
            sku = (it.get("sku") or "").strip()
            sku_txt = f" (SKU {sku})" if sku else ""
            lines.append(f"   {tag}{j}) {it['name']}{sku_txt} — ${price:,.2f} / {unit}")
    lines.append("\n✅ Responde con: A1, B2, C1 o escribe *asesor*.")
    return "\n".join(lines)

def parse_pending_picks(text: str):
    t = (text or "").upper()
    t = t.replace(" ", "")
    return [(m[0], int(m[1])) for m in re.findall(r"\b([A-Z])(\d+)\b", t)]

def cart_render_quote(state: dict) -> str:
    cart = (state or {}).get("cart") or []
    if not cart:
        return ""
    lines = []
    total = 0.0
    for it in cart:
        qty = int(it.get("qty") or 0)
        price = float(it.get("price") or 0.0)
        name = it.get("name") or ""
        subtotal = qty * price
        total += subtotal
        lines.append(f"• {qty} x {name} — ${subtotal:,.2f}")
    return (
        "Cotización:\n"
        + "\n".join(lines)
        + f"\n\n*Total: ${total:,.2f}* (IVA incluido)\n\n"
        "💳 Escribe *pagar* y te mandamos datos bancarios o link para pago con tarjeta."
    )

def api_key_prefix(token: str) -> str:
    return token[:API_KEY_PREFIX_LEN]

def api_key_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# -------------------------
# DB
# -------------------------
def get_conn():
    dsn = (os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        raise RuntimeError("DATABASE_URL missing")
    if "sslmode=" not in dsn:
        dsn = dsn + ("&" if "?" in dsn else "?") + "sslmode=require"
    conn = psycopg2.connect(dsn, connect_timeout=5)
    conn.autocommit = True
    return conn

def print_db_fingerprint():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("select inet_server_addr(), inet_server_port(), current_database()")
        print("DB FINGERPRINT (Render):", cur.fetchone())
        conn.close()
    except Exception as e:
        print("DB FINGERPRINT ERROR:", repr(e))

print_db_fingerprint()

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

import psycopg2

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

def _year_month_utc() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"

def get_company_plan_code(company_id: str) -> str:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT plan_code FROM companies WHERE id=%s LIMIT 1", (company_id,))
        row = cur.fetchone()
        return (row[0] or "free").strip() if row else "free"
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

def send_whatsapp_list(wa_api_key: str, phone_number_id: str, to: str,
                       body_text: str, options: list, button_label: str = "Ver opciones"):
    rows = [{"id": f"spec_{i}", "title": opt} for i, opt in enumerate(options[:10])]
    payload = {
        "messaging_product": "whatsapp", "to": to, "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {"button": button_label, "sections": [{"title": "Opciones", "rows": rows}]},
        },
    }
    url = f"https://graph.facebook.com/v19.0/{phone_number_id}/messages"
    headers = {"Authorization": f"Bearer {wa_api_key}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"WA list failed {r.status_code}: {r.text[:400]}")

def send_whatsapp_text(wa_api_key: str, phone_number_id: str, to: str, text: str):
    url = f"https://graph.facebook.com/v19.0/{phone_number_id}/messages"
    headers = {"Authorization": f"Bearer {wa_api_key}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": text}}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"WhatsApp send failed {r.status_code}: {r.text[:400]}")

def download_whatsapp_media(image_id: str, wa_api_key: str) -> bytes:
    url_resp = requests.get(
        f"https://graph.facebook.com/v19.0/{image_id}",
        headers={"Authorization": f"Bearer {wa_api_key}"},
        timeout=10,
    )
    url_resp.raise_for_status()
    media_url = url_resp.json()["url"]
    img_resp = requests.get(media_url, headers={"Authorization": f"Bearer {wa_api_key}"}, timeout=15)
    img_resp.raise_for_status()
    return img_resp.content

def extract_text_from_image(image_bytes: bytes) -> str | None:
    if not openai_client:
        return None
    try:
        import base64
        b64 = base64.b64encode(image_bytes).decode()
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "low"}},
                    {"type": "text", "text": (
                        "Eres asistente de ferretería mexicana. Esta imagen es una lista "
                        "de materiales manuscrita. Extrae SOLO los productos con sus cantidades. "
                        "Formato estricto: CANTIDAD PRODUCTO, un item por línea. "
                        "Productos comunes: poste, tablaroca, cemento, varilla, block, "
                        "malla, perfacinta, redimix, canal, tornillo, clavo, tubo. "
                        "Si una palabra parece un producto de ferretería con error ortográfico, corrígela. "
                        "Ejemplo: 'paste' → 'poste', 'tablroca' → 'tablaroca'. "
                        "Ignora palabras sueltas que no sean productos (Menu, Total, Fecha, etc). "
                        "Si un renglón existe pero no puedes leerlo claramente, escribe: 1 ???. "
                        "Ejemplo de salida:\n10 sacos cemento\n5 varilla 3/8\n1 ???\n2 cubetas pintura\n"
                        "Si no hay lista de productos en absoluto, responde exactamente: NO_LIST"
                    )}
                ]
            }],
            max_tokens=300, temperature=0.1,
        )
        result = (resp.choices[0].message.content or "").strip()
        return None if result == "NO_LIST" else result
    except Exception as e:
        print("VISION ERROR:", repr(e))
        return None

def notify_owner_escalation(wa_api_key: str, phone_number_id: str, owner_phone: str,
                             client_phone: str, reason: str, state: dict):
    cart = (state or {}).get("cart") or []
    cart_txt = ""
    if cart:
        lines = [f"• {it['qty']}x {it['name']} — ${float(it.get('price',0))*int(it.get('qty',0)):,.2f}" for it in cart]
        cart_txt = "\n" + "\n".join(lines)
    msg = (
        f"⚠️ *Cliente necesita un asesor*\n"
        f"📱 {client_phone}\n"
        f"❓ Motivo: {reason}\n"
        f"🛒 Carrito actual:{cart_txt if cart_txt else ' (vacío)'}\n\n"
        f"Responde directo a ese número."
    )
    send_whatsapp_text(wa_api_key, phone_number_id, owner_phone, msg)

def notify_owner_comprobante(wa_api_key: str, phone_number_id: str, owner_phone: str,
                              client_phone: str, state: dict):
    cart = (state or {}).get("cart") or []
    cart_txt = ""
    if cart:
        lines = [f"• {it['qty']}x {it['name']} — ${float(it.get('price',0))*int(it.get('qty',0)):,.2f}" for it in cart]
        cart_txt = "\n" + "\n".join(lines)
    msg = (
        f"💰 *Comprobante de pago recibido*\n"
        f"📱 Cliente: {client_phone}\n"
        f"🛒 Cotización:{cart_txt if cart_txt else ' (sin carrito)'}\n\n"
        f"Revisa tu WhatsApp — el cliente acaba de mandar el comprobante."
    )
    send_whatsapp_text(wa_api_key, phone_number_id, owner_phone, msg)

# -------------------------
# Sessions
# -------------------------
def create_session(conn, user_id: int) -> str:
    sid = secrets.token_urlsafe(32)
    exp = datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO sessions (id, user_id, expires_at) VALUES (%s, %s, %s)", (sid, user_id, exp))
        return sid
    finally:
        cur.close()

def get_user_from_session(request: Request):
    sid = request.cookies.get(SESSION_COOKIE_NAME)
    if not sid:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.id, u.email, u.company_id::text
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.id=%s AND s.expires_at > now()
            LIMIT 1
            """,
            (sid,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Not authenticated")
        user_id, email, company_id = row
        if not company_id:
            raise HTTPException(status_code=400, detail="User sin company_id asignado")
        return {"id": int(user_id), "email": email, "company_id": company_id}
    finally:
        if cur: cur.close()
        if conn: conn.close()

def require_company_id(request: Request) -> str:
    u = get_user_from_session(request)
    cid = (u.get("company_id") or "").strip()
    if not cid:
        raise HTTPException(status_code=400, detail="No pude resolver company_id")
    return cid

def get_company_from_bearer(authorization: str):
    auth = (authorization or "").strip()
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth.split(" ", 1)[1].strip()
    if len(token) < 20:
        raise HTTPException(status_code=401, detail="Invalid token")
    prefix = api_key_prefix(token)
    key_hash = api_key_hash(token)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, company_id FROM api_keys
            WHERE prefix = %s AND key_hash = %s AND revoked_at IS NULL
            LIMIT 1
            """,
            (prefix, key_hash),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid or revoked token")
        api_key_id, company_id = row
        cur.execute("UPDATE api_keys SET last_used_at = now() WHERE id = %s", (api_key_id,))
        return {"company_id": str(company_id), "api_key_id": str(api_key_id)}
    finally:
        if cur: cur.close()
        if conn: conn.close()


# -------------------------
# Models
# -------------------------
class RegisterBody(BaseModel):
    email: str
    password: str

class LoginBody(BaseModel):
    email: str
    password: str

class ChatRequest(BaseModel):
    app: str = "cotizabot"
    message: str
    user_id: Optional[str] = None
    source: str = "web"
    country: str = "MX"

class CompanyCreateBody(BaseModel):
    name: str
    slug: Optional[str] = None
    key_name: str = "default"


# -------------------------
# Middleware (CORS)
# -------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://cotizaexpress.com",
        "https://www.cotizaexpress.com",
        "https://buildquote-12.preview.emergentagent.com",
        "https://ferreteria-whatsapp.emergent.host",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------
# Basic endpoints
# -------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "clawdbot-server"}

# -------------------------
# WhatsApp webhook receive
# -------------------------

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    payload = await request.json()
    try:
        phone_number_id = payload["entry"][0]["changes"][0]["value"]["metadata"]["phone_number_id"]
    except Exception:
        return {"ok": True}

    company = get_company_by_phone_number_id(phone_number_id)
    if not company:
        return {"ok": True}

    value = payload["entry"][0]["changes"][0]["value"]
    messages = value.get("messages") or []
    if not messages:
        return {"ok": True}

    msg = messages[0]
    from_phone = msg.get("from")
    msg_type = msg.get("type", "text")

    text = ""
    if msg_type == "text":
        text = (msg.get("text") or {}).get("body") or ""

    elif msg_type == "image":
        image_id = (msg.get("image") or {}).get("id")
        caption = (msg.get("image") or {}).get("caption") or ""

        # ── Comprobante de pago ──────────────────────────────────────────
        _st_check = get_quote_state(company["company_id"], from_phone) or {}
        if _st_check.get("awaiting_comprobante"):
            # Notificar al dueño
            try:
                owner_row = None
                conn2 = get_conn()
                cur2 = conn2.cursor()
                cur2.execute(
                    "SELECT owner_phone, wa_api_key, wa_phone_number_id FROM companies WHERE id=%s",
                    (company["company_id"],)
                )
                owner_row = cur2.fetchone()
                cur2.close()
                conn2.close()
                if owner_row and owner_row[0]:
                    notify_owner_comprobante(
                        wa_api_key=owner_row[1],
                        phone_number_id=owner_row[2],
                        owner_phone=owner_row[0],
                        client_phone=from_phone,
                        state=_st_check,
                    )
            except Exception as e:
                print("COMPROBANTE NOTIFY ERROR:", repr(e))
            # Limpiar flag
            _st_check.pop("awaiting_comprobante", None)
            upsert_quote_state(company["company_id"], from_phone, _st_check)
            # Confirmar al cliente
            send_whatsapp_text(
                wa_api_key=company["wa_api_key"],
                phone_number_id=company["wa_phone_number_id"],
                to=from_phone,
                text="✅ ¡Comprobante recibido! Le avisamos a la empresa y en breve te confirman tu pedido. 🙏",
            )
            return {"ok": True}
        # ────────────────────────────────────────────────────────────────

        if image_id and company.get("wa_api_key"):
            try:
                img_bytes = download_whatsapp_media(image_id, company["wa_api_key"])
                extracted = extract_text_from_image(img_bytes)
                if extracted:
                    text = f"{caption}\n{extracted}".strip() if caption else extracted
                    print("IMAGE EXTRACTED:", text[:200])
                else:
                    send_whatsapp_text(
                        wa_api_key=company["wa_api_key"],
                        phone_number_id=company["wa_phone_number_id"],
                        to=from_phone,
                        text="📷 Vi tu imagen pero no encontré una lista de productos.\n\nMándame el pedido así:\n10 cemento, 5 varilla 3/8",
                    )
                    return {"ok": True}
            except Exception as e:
                print("IMAGE PROCESSING ERROR:", repr(e))
                send_whatsapp_text(
                    wa_api_key=company["wa_api_key"],
                    phone_number_id=company["wa_phone_number_id"],
                    to=from_phone,
                    text="No pude leer la imagen 😔 Intenta enviarla más clara o escribe el pedido.",
                )
                return {"ok": True}

    elif msg_type == "interactive":
        interactive = msg.get("interactive") or {}
        itype = interactive.get("type")
        if itype == "list_reply":
            list_reply = interactive.get("list_reply") or {}
            lr_id = (list_reply.get("id") or "").strip()
            lr_title = (list_reply.get("title") or "").strip()
            text = lr_id if lr_id.upper().startswith("PICK_") else lr_title
        elif itype == "button_reply":
            text = (interactive.get("button_reply") or {}).get("title") or ""

    reply = build_reply_for_company(
        company["company_id"], text,
        wa_from=from_phone,
        is_interactive=(msg_type == "interactive"),
    )

    if isinstance(reply, dict) and reply.get("type") == "list":
        send_whatsapp_list(
            wa_api_key=company["wa_api_key"],
            phone_number_id=company["wa_phone_number_id"],
            to=from_phone,
            body_text=reply.get("body") or "",
            options=reply.get("options") or [],
            button_label=reply.get("button_label", "Ver opciones"),
        )
    elif isinstance(reply, dict) and reply.get("type") == "text_then_list_sections":
        send_whatsapp_text(
            wa_api_key=company["wa_api_key"],
            phone_number_id=company["wa_phone_number_id"],
            to=from_phone,
            text=reply["text"],
        )
        send_whatsapp_list_sections(
            wa_api_key=company["wa_api_key"],
            phone_number_id=company["wa_phone_number_id"],
            to=from_phone,
            body_text=reply["body"],
            sections=reply["sections"],
            button_label=reply.get("button_label", "Ver opciones"),
        )
    elif isinstance(reply, dict) and reply.get("type") == "list_sections":
        send_whatsapp_list_sections(
            wa_api_key=company["wa_api_key"],
            phone_number_id=company["wa_phone_number_id"],
            to=from_phone,
            body_text=reply.get("body") or "",
            sections=reply.get("sections") or [],
            button_label=reply.get("button_label", "Ver opciones"),
        )
    else:
        text_body = reply.get("body") if isinstance(reply, dict) else reply
        text_body = (text_body or "").strip() or "¿Me repites eso?"
        send_whatsapp_text(
            wa_api_key=company["wa_api_key"],
            phone_number_id=company["wa_phone_number_id"],
            to=from_phone,
            text=text_body,
        )

    return {"ok": True}

def send_whatsapp_list_sections(wa_api_key: str, phone_number_id: str, to: str,
                                 body_text: str, sections: list, button_label: str = "Ver opciones"):
    payload = {
        "messaging_product": "whatsapp", "to": to, "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {"button": button_label, "sections": sections},
        },
    }
    url = f"https://graph.facebook.com/v19.0/{phone_number_id}/messages"
    headers = {"Authorization": f"Bearer {wa_api_key}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"WA list sections failed {r.status_code}: {r.text[:400]}")


def build_reply_for_company(company_id: str, user_text: str, wa_from: str = "", is_interactive: bool = False) -> str:
    if is_interactive:
        user_text = (user_text or "").strip()
    else:
        user_text = (user_text or "").strip().replace('\u201c', '').replace('\u201d', '').replace('"', '')
    wa_from = (wa_from or "").strip()

    try:
        usage_info = track_conversation_if_new(company_id, wa_from)
        if usage_info.get("limit", 0) > 0 and usage_info.get("usage", 0) > usage_info.get("limit", 0):
            print("WA LIMIT EXCEEDED:", usage_info)
    except Exception as e:
        print("WA TRACK ERROR:", repr(e))

    import string as _string
    from spec_definitions import get_spec_steps, already_has_specs, build_spec_query

    def _search_pricebook_candidates(conn, company_id: str, q: str, limit: int = 5):
        q = (q or "").strip()
        if not q:
            return []
        q_clean = extract_product_query(q)
        qn = norm_name(q_clean)
        tokens = [t for t in qn.split() if len(t) >= 3] or [qn]
        where_parts = []
        params = [company_id]
        for tok in tokens[:6]:
            where_parts.append("name_norm LIKE %s")
            params.append(f"%{tok}%")
        where_sql = " OR ".join(where_parts)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT sku, name, unit, price, vat_rate, synonyms
                FROM pricebook_items
                WHERE company_id=%s
                  AND ({where_sql} OR sku ILIKE %s OR name ILIKE %s OR synonyms ILIKE %s)
                LIMIT 30
                """,
                (*params, f"%{q_clean}%", f"%{q_clean}%", f"%{q_clean}%"),
            )
            rows = cur.fetchall()
            items = []
            for sku, name, unit, price, vat_rate, synonyms in rows:
                it = {"sku": sku, "name": name, "unit": unit,
                      "price": float(price) if price is not None else None,
                      "vat_rate": float(vat_rate) if vat_rate is not None else None}
                sn = norm_name(name or "")
                sku_n = norm_name(sku or "")
                syn_n = norm_name(synonyms or "")
                it["_score"] = max(
                    fuzz.token_set_ratio(qn, sn),
                    fuzz.token_set_ratio(qn, sku_n) if sku else 0,
                    fuzz.token_set_ratio(qn, syn_n) if synonyms else 0,
                )
                items.append(it)
            items.sort(key=lambda x: x.get("_score", 0), reverse=True)
            out = []
            for it in items[:max(1, int(limit or 5))]:
                it.pop("_score", None)
                out.append(it)
            return out
        finally:
            cur.close()

    def _parse_pending_picks(text: str):
        t = (text or "").upper().replace(" ", "")
        t = t.replace("PICK_", "")
        return [(m[0], int(m[1])) for m in re.findall(r"\b([A-Z])(\d+)\b", t)]

    def _is_greeting_like(tnorm: str) -> bool:
        t = (tnorm or "").strip()
        if not t:
            return False
        if t in {"hola", "buenas", "hey", "holi", "menu", "menú", "ayuda", "inicio"}:
            return True
        if t.startswith("hola"):
            return True
        if t.startswith("buenos") or t.startswith("buenas"):
            return True
        return False

    # =========================================================
    # _build_reply_with_pending — NUEVO: muestra UN pendiente a la vez
    # =========================================================
    def _build_reply_with_pending(state: dict):
        pending = state.get("pending") or []

        if pending:
            # Mostrar SOLO el primero como lista interactiva
            p = pending[0]
            qty = int(p.get("qty") or 0)
            raw = (p.get("raw") or "").strip()
            cands = p.get("candidates") or []
            remaining = len(pending) - 1

            if cands:
                rows = []
                for j, it in enumerate(cands[:10], start=1):
                    price = float(it.get("price") or 0.0)
                    sku = (it.get("sku") or "").strip()
                    sku_txt = f" ({sku})" if sku else ""
                    title = f"{it['name']}{sku_txt}"[:24]
                    description = f"${price:,.2f} / {it.get('unit') or 'unidad'}"
                    rows.append({
                        "id": f"pick_A{j}",
                        "title": title,
                        "description": description,
                    })
                suffix = f"\n\n_(quedan {remaining} producto(s) más)_" if remaining > 0 else ""
                return {
                    "type": "list_sections",
                    "body": f"❓ {qty} x *{raw}*\n¿Cuál de estas opciones buscas?{suffix}",
                    "sections": [{"title": f"{qty}x {raw}"[:24], "rows": rows}],
                    "button_label": "Ver opciones",
                }
            else:
                # Sin candidatos — pedir aclaración
                suffix = f" (quedan {remaining} más)" if remaining > 0 else ""
                return (
                    f"❓ No encontré *{raw}*{suffix}\n\n"
                    "¿Me lo puedes describir mejor o escribir el SKU?\n"
                    "O escribe *asesor* para que te ayude alguien."
                )

        # Sin pendientes — mostrar carrito
        msg = cart_render_quote(state) if (state.get("cart") or []) else ""
        msg += (
            "\n\n¿Agregamos algo más?\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )
        return msg

    # =========================================================
    # 0) COMANDOS (reset / salir)
    # =========================================================
    tnorm = norm_name(user_text).replace("cotización", "cotizacion")

    # =========================================================
    # 0.1) PAGAR
    # =========================================================
    pagar_triggers = {"pagar", "pago", "como pago", "cómo pago", "quiero pagar", "datos de pago", "datos bancarios", "transferencia"}
    if any(pt == tnorm or pt in tnorm for pt in pagar_triggers):
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT bank_name, bank_account_name, bank_clabe, bank_account_number, mercadopago_url FROM companies WHERE id=%s",
                (company_id,)
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
        except Exception:
            row = None

        parts = []
        if row:
            bank_name = (row[0] or "").strip()
            bank_acc_name = (row[1] or "").strip()
            bank_clabe = (row[2] or "").strip()
            bank_acc_num = (row[3] or "").strip()
            mp_url = (row[4] or "").strip()

            if bank_clabe or bank_acc_num:
                lines = ["🏦 *Datos bancarios:*"]
                if bank_name: lines.append(f"Banco: {bank_name}")
                if bank_acc_name: lines.append(f"A nombre de: {bank_acc_name}")
                if bank_clabe: lines.append(f"CLABE: {bank_clabe}")
                if bank_acc_num: lines.append(f"Cuenta: {bank_acc_num}")
                parts.append("\n".join(lines))

            if mp_url:
                parts.append(f"💳 *Pago con tarjeta:*\n{mp_url}")

        if parts:
            _st = get_quote_state(company_id, wa_from) if wa_from else {}
            _st = _st or {}
            _st["awaiting_comprobante"] = True
            if wa_from:
                upsert_quote_state(company_id, wa_from, _st)
            return (
                "\n\n".join(parts)
                + "\n\n📎 Cuando realices tu pago, *manda el comprobante* por aquí y avisamos a la empresa."
            )
        else:
            return (
                "Para recibir los datos de pago, escribe *asesor* y un representante te los enviará. 🙏"
            )

    reset_triggers = {
        "salir", "cancelar", "cancel", "reset", "reiniciar",
        "nueva cotizacion", "nuevo", "empezar de nuevo",
        "borrar", "borrar carrito", "vaciar carrito",
        "limpiar", "limpiar carrito",
    }

    if any(rt == tnorm or rt in tnorm for rt in reset_triggers):
        if wa_from:
            clear_quote_state(company_id, wa_from)
        return (
            "✅ Listo. Empezamos de cero.\n\n"
            "Mándame tu cotización así:\n"
            "Ej: 10 cemento, 5 varilla 3/8\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

    # =========================================================
    # 0.25) GRACIAS
    # =========================================================
    thanks_triggers = {"gracias", "muchas gracias", "mil gracias", "thx", "thanks"}
    if tnorm in thanks_triggers:
        return (
            "¡Con gusto! 🙌\n"
            "Si quieres otra cotización, mándame: 10 cemento, 5 varilla 3/8\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

    # =========================================================
    # 0.3) ESCALADO A ASESOR
    # =========================================================
    escalation_triggers = {
        "asesor", "asesor humano", "humano", "persona", "agente",
        "hablar con alguien", "hablar con una persona", "quiero hablar",
        "necesito ayuda", "ayuda humana",
    }
    if any(rt == tnorm or rt in tnorm for rt in escalation_triggers):
        state = get_quote_state(company_id, wa_from) if wa_from else {}
        state = state or {}
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT owner_phone, wa_api_key, wa_phone_number_id FROM companies WHERE id=%s", (company_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row and row[0]:
                notify_owner_escalation(
                    wa_api_key=row[1], phone_number_id=row[2], owner_phone=row[0],
                    client_phone=wa_from, reason="Cliente solicitó hablar con un asesor", state=state,
                )
        except Exception as e:
            print("ESCALATION ERROR:", repr(e))
        return (
            "Un asesor te contactará pronto 🙏\n\n"
            "Mientras tanto puedes seguir agregando productos "
            "o esperar a que te contacten."
        )

    # =========================================================
    # 0.5) SALUDOS
    # =========================================================
    if _is_greeting_like(tnorm):
        if wa_from:
            st = get_quote_state(company_id, wa_from) or {}
            if (st.get("cart") or []) or (st.get("pending") or []):
                clear_quote_state(company_id, wa_from)
                return (
                    "👋 ¡Hola! Empezamos una cotización nueva.\n\n"
                    "Mándame tu pedido así:\n"
                    "👉 10 cemento, 5 varilla 3/8\n\n"
                    "🧭 Comandos:\n"
                    "• 'nueva cotizacion' → empezar de cero\n"
                    "• 'salir' → cancelar"
                )
        return (
            "👋 ¡Hola! Puedo cotizarte materiales de construcción y ferretería.\n\n"
            "Mándame tu pedido así:\n"
            "👉 10 cemento, 5 varilla 3/8, 2 martillos\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

    # =========================================================
    # 0.75) RESOLVER SPECS PENDIENTES
    # =========================================================
    _state_specs = get_quote_state(company_id, wa_from) if wa_from else {}
    _state_specs = _state_specs or {}

    if _state_specs.get("pending_specs"):
        ps = _state_specs["pending_specs"]
        current = ps[0]
        steps = current["steps"]
        step_idx = current["step_idx"]
        current_step = steps[step_idx]

        t_low = user_text.strip().lower()
        chosen = next(
            (opt for opt in current_step["options"] if t_low == opt.lower() or opt.lower() in t_low),
            None
        )

        if chosen:
            current["resolved"][current_step["key"]] = chosen
            current["step_idx"] += 1

            if current["step_idx"] >= len(steps):
                full_query = build_spec_query(current["raw"], current["resolved"])
                conn = get_conn()
                try:
                    result = smart_search(conn, company_id, full_query, current["qty"])
                finally:
                    conn.close()

                ps.pop(0)
                if ps:
                    _state_specs["pending_specs"] = ps
                else:
                    _state_specs.pop("pending_specs", None)

                if result["status"] == "found":
                    _state_specs = cart_add_item(_state_specs, {
                        "sku": result["item"].get("sku"),
                        "name": result["item"].get("name"),
                        "unit": result["item"].get("unit") or "unidad",
                        "price": float(result["item"].get("price") or 0.0),
                        "vat_rate": result["item"].get("vat_rate"),
                        "qty": current["qty"],
                    })
                else:
                    pend = _state_specs.get("pending") or []
                    pend.append({
                        "qty": current["qty"],
                        "raw": full_query,
                        "candidates": result.get("candidates") or [],
                    })
                    _state_specs["pending"] = pend

                if wa_from:
                    upsert_quote_state(company_id, wa_from, _state_specs)

                if _state_specs.get("pending_specs"):
                    next_p = _state_specs["pending_specs"][0]
                    next_step = next_p["steps"][next_p["step_idx"]]
                    return {
                        "type": "list",
                        "body": f"✅ Anotado. Ahora el {next_p['raw']} — {next_step['question']}",
                        "options": next_step["options"],
                        "button_label": "Ver opciones",
                    }

                return _build_reply_with_pending(_state_specs)

            else:
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _state_specs)
                next_step = steps[current["step_idx"]]
                return {
                    "type": "list",
                    "body": f"✅ {chosen} ✓  {next_step['question']}",
                    "options": next_step["options"],
                    "button_label": "Ver opciones",
                }
        else:
            return {
                "type": "list",
                "body": f"Por favor elige una opción 👇\n{current_step['question']}",
                "options": current_step["options"],
                "button_label": "Ver opciones",
            }

    # =========================================================
    # 0.8) PICKS — ahora siempre resuelve pending[0] (el primero)
    # =========================================================
    _quick_picks = _parse_pending_picks(user_text)
    _state_picks = get_quote_state(company_id, wa_from) if wa_from else {}
    _state_picks = _state_picks or {}

    if _quick_picks and _state_picks.get("pending"):
        state = _state_picks
        pend = state.get("pending") or []

        if pend:
            # Siempre resolvemos pending[0] — el bot solo muestra uno a la vez
            p = pend[0]
            cands = p.get("candidates") or []
            _, opt = _quick_picks[0]  # tomar el número elegido (la letra ya no importa)

            if cands and 1 <= opt <= len(cands):
                chosen = cands[opt - 1]
                qty = int(p.get("qty") or 0)
                state = cart_add_item(state, {
                    "sku": chosen.get("sku"),
                    "name": chosen.get("name"),
                    "unit": chosen.get("unit") or "unidad",
                    "price": float(chosen.get("price") or 0.0),
                    "vat_rate": chosen.get("vat_rate"),
                    "qty": qty,
                })
                pend.pop(0)  # quitar el primero resuelto
                if pend:
                    state["pending"] = pend
                else:
                    state.pop("pending", None)

                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)

                return _build_reply_with_pending(state)

        # Si no pudo resolver (opt fuera de rango), volver a mostrar el pendiente actual
        if wa_from:
            upsert_quote_state(company_id, wa_from, state)
        return _build_reply_with_pending(state)

    # =========================================================
    # 1) MULTI-ITEMS + CARRITO
    # =========================================================
    multi = extract_qty_items_robust(user_text)
    if not multi:
        multi = ner_extract_items(user_text)
    if multi:
        conn = get_conn()
        try:
            state = get_quote_state(company_id, wa_from) if wa_from else None
            if not state:
                state = {}
            state.pop("pending_specs", None)
            missing = []
            for qty, prod_raw in multi:
                if not looks_like_product_phrase(prod_raw):
                    continue
                if prod_raw.strip() == "???":
                    missing.append({"qty": qty, "raw": "producto ilegible", "candidates": []})
                    continue
                steps = get_spec_steps(prod_raw)
                if steps and not already_has_specs(prod_raw, steps):
                    specs_pending = state.get("pending_specs") or []
                    specs_pending.append({"raw": prod_raw, "qty": qty, "steps": steps, "step_idx": 0, "resolved": {}})
                    state["pending_specs"] = specs_pending
                    continue
                try:
                    result = smart_search(conn, company_id, prod_raw, qty)
                except Exception as e:
                    print("SMART SEARCH ERROR:", repr(e))
                    result = {"status": "not_found", "item": None, "candidates": []}
                if result["status"] == "found":
                    state = cart_add_item(state, {
                        "sku": result["item"].get("sku"),
                        "name": result["item"].get("name"),
                        "unit": result["item"].get("unit") or "unidad",
                        "price": float(result["item"].get("price") or 0.0),
                        "vat_rate": result["item"].get("vat_rate"),
                        "qty": qty,
                    })
                else:
                    missing.append({"qty": qty, "raw": prod_raw, "candidates": result["candidates"]})
            if missing:
                state["pending"] = missing
            else:
                state.pop("pending", None)
            if wa_from:
                upsert_quote_state(company_id, wa_from, state)
            if state.get("pending_specs"):
                first = state["pending_specs"][0]
                first_step = first["steps"][first["step_idx"]]
                prefix = ""
                if state.get("cart"):
                    prefix = cart_render_quote(state) + "\n\n"
                n_specs = len(state["pending_specs"])
                intro = f"Encontré {n_specs} producto(s) que necesitan especificaciones.\n\n" if n_specs > 1 else ""
                return {
                    "type": "list",
                    "body": prefix + intro + first_step["question"],
                    "options": first_step["options"],
                    "button_label": "Ver opciones",
                }
            if not state.get("cart") and not missing:
                return "No encontré esos productos en el catálogo."
            return _build_reply_with_pending(state)
        finally:
            conn.close()

    # =========================================================
    # 2) SINGLE ITEM + CARRITO
    # =========================================================
    qty, prod_query = extract_qty_and_product(user_text)
    if qty and prod_query:
        steps = get_spec_steps(prod_query)
        if steps and not already_has_specs(prod_query, steps):
            state = get_quote_state(company_id, wa_from) if wa_from else {}
            state = state or {}
            state["pending_specs"] = [{"raw": prod_query, "qty": qty, "steps": steps, "step_idx": 0, "resolved": {}}]
            if wa_from:
                upsert_quote_state(company_id, wa_from, state)
            return {"type": "list", "body": steps[0]["question"], "options": steps[0]["options"], "button_label": "Ver opciones"}

        conn = get_conn()
        try:
            try:
                result = smart_search(conn, company_id, prod_query, qty)
            except Exception as e:
                print("SMART SEARCH ERROR:", repr(e))
                result = {"status": "not_found", "item": None, "candidates": []}

            if result["status"] == "found":
                state = get_quote_state(company_id, wa_from) if wa_from else {}
                state = state or {}
                state = cart_add_item(state, {
                    "sku": result["item"].get("sku"),
                    "name": result["item"].get("name"),
                    "unit": result["item"].get("unit") or "unidad",
                    "price": float(result["item"].get("price") or 0.0),
                    "vat_rate": result["item"].get("vat_rate"),
                    "qty": qty,
                })
                state.pop("pending", None)
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state)

            elif result["status"] == "ambiguous":
                pending = [{"qty": qty, "raw": prod_query, "candidates": result["candidates"]}]
                state = get_quote_state(company_id, wa_from) if wa_from else {}
                state = state or {}
                state["pending"] = pending
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state)

        finally:
            conn.close()

    # =========================================================
    # 3) PREGUNTA DE PRECIO
    # =========================================================
    if looks_like_price_question(user_text):
        conn = get_conn()
        try:
            items = search_pricebook(conn, company_id, user_text, limit=8)
        finally:
            conn.close()
        if items:
            lines = []
            for it in items[:8]:
                unit = f" / {it['unit']}" if it.get("unit") else ""
                lines.append(f"- {it['name']}: ${float(it['price']):,.2f}{unit}")
            return (
                "Encontré estos precios:\n"
                + "\n".join(lines)
                + "\n\nDime cantidades para cotizar (ej: 10 cemento, 5 varilla 3/8).\n\n"
                "🧭 Comandos:\n"
                "• 'nueva cotizacion' → empezar de cero\n"
                "• 'salir' → cancelar"
            )

    # =========================================================
    # 3.5) CONTEXTO — picks A1/B2 o aclaraciones
    # =========================================================
    state = get_quote_state(company_id, wa_from) if wa_from else None
    if state and state.get("pending"):
        pend = state.get("pending") or []

        picks = _parse_pending_picks(user_text)
        if picks:
            # Siempre resolvemos pending[0]
            if pend:
                p = pend[0]
                cands = p.get("candidates") or []
                _, opt = picks[0]

                if cands and 1 <= opt <= len(cands):
                    chosen = cands[opt - 1]
                    qty = int(p.get("qty") or 0)
                    state = cart_add_item(state, {
                        "sku": chosen.get("sku"),
                        "name": chosen.get("name"),
                        "unit": chosen.get("unit") or "unidad",
                        "price": float(chosen.get("price") or 0.0),
                        "vat_rate": chosen.get("vat_rate"),
                        "qty": qty,
                    })
                    pend.pop(0)
                    if pend:
                        state["pending"] = pend
                    else:
                        state.pop("pending", None)

            try:
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
            except Exception as e:
                print("UPSERT STATE ERROR:", repr(e))
                return "Error guardando cotización. Intenta de nuevo."

            return _build_reply_with_pending(state)

        clarifs_raw = split_clarifications(user_text)
        clarifs = [c for c in clarifs_raw if c.strip()]

        if clarifs:
            conn = get_conn()
            try:
                still = []
                for i, p in enumerate(pend):
                    qty = int(p.get("qty") or 0)
                    raw = (p.get("raw") or "").strip()
                    prod_raw = clarifs[i] if i < len(clarifs) else raw
                    if is_specs_only(prod_raw):
                        prod_raw = f"{raw} {prod_raw}"

                    try:
                        result = smart_search(conn, company_id, prod_raw, qty)
                    except Exception as e:
                        print("SMART SEARCH ERROR:", repr(e))
                        result = {"status": "not_found", "item": None, "candidates": []}

                    if result["status"] == "found":
                        state = cart_add_item(state, {
                            "sku": result["item"].get("sku"),
                            "name": result["item"].get("name"),
                            "unit": result["item"].get("unit") or "unidad",
                            "price": float(result["item"].get("price") or 0.0),
                            "vat_rate": result["item"].get("vat_rate"),
                            "qty": qty,
                        })
                    else:
                        attempts = int(p.get("clarification_attempts") or 0) + 1
                        still.append({
                            "qty": qty, "raw": prod_raw,
                            "candidates": result["candidates"],
                            "clarification_attempts": attempts,
                        })

                if still:
                    max_attempts = max(int(p.get("clarification_attempts") or 0) for p in still)
                    if max_attempts >= 2:
                        try:
                            conn2 = get_conn()
                            cur2 = conn2.cursor()
                            cur2.execute(
                                "SELECT owner_phone, wa_api_key, wa_phone_number_id FROM companies WHERE id=%s",
                                (company_id,)
                            )
                            row2 = cur2.fetchone()
                            cur2.close()
                            conn2.close()
                            if row2 and row2[0]:
                                productos_txt = ", ".join([p["raw"] for p in still])
                                notify_owner_escalation(
                                    wa_api_key=row2[1], phone_number_id=row2[2], owner_phone=row2[0],
                                    client_phone=wa_from,
                                    reason=f"Producto no encontrado después de 2 intentos: {productos_txt}",
                                    state=state,
                                )
                        except Exception as e:
                            print("AUTO ESCALATION ERROR:", repr(e))

                        state.pop("pending", None)
                        if wa_from:
                            upsert_quote_state(company_id, wa_from, state)
                        cart_txt = cart_render_quote(state) + "\n\n" if state.get("cart") else ""
                        return (
                            f"{cart_txt}"
                            "No encontré esos productos en el catálogo 😔\n\n"
                            "Un asesor te contactará pronto para ayudarte 🙏"
                        )

                    state["pending"] = still
                else:
                    state.pop("pending", None)

                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)

                return _build_reply_with_pending(state)
            finally:
                conn.close()

        # Si hay pendiente sin resolver, mostrar el primero
        if pend:
            return _build_reply_with_pending(state)

    # =========================================================
    # 4) GUARD — mensaje con números pero sin producto
    # =========================================================
    if re.search(r"\b\d+\b", user_text):
        qty, prod_query = extract_qty_and_product(user_text)
        if qty and prod_query:
            conn = get_conn()
            try:
                result = smart_search(conn, company_id, prod_query, qty)
            finally:
                conn.close()

            if result["status"] == "found":
                state = get_quote_state(company_id, wa_from) if wa_from else {}
                state = state or {}
                state = cart_add_item(state, {
                    "sku": result["item"].get("sku"),
                    "name": result["item"].get("name"),
                    "unit": result["item"].get("unit") or "unidad",
                    "price": float(result["item"].get("price") or 0.0),
                    "vat_rate": result["item"].get("vat_rate"),
                    "qty": qty,
                })
                state.pop("pending", None)
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state)

            elif result["status"] == "ambiguous":
                state = get_quote_state(company_id, wa_from) if wa_from else {}
                state = state or {}
                state["pending"] = [{"qty": qty, "raw": prod_query, "candidates": result["candidates"]}]
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state)

    # =========================================================
    # 4.5) HORARIOS / UBICACIÓN
    # =========================================================
    if looks_like_hours_question(user_text):
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT hours_text, address_text, google_maps_url FROM companies WHERE id=%s", (company_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
        except Exception:
            row = None

        if row:
            hours = (row[0] or "").strip()
            address = (row[1] or "").strip()
            maps_url = (row[2] or "").strip()
            parts = []
            if hours: parts.append(f"🕐 *Horarios:* {hours}")
            if address: parts.append(f"📍 *Dirección:* {address}")
            if maps_url: parts.append(f"🗺️ *Google Maps:* {maps_url}")
            if parts:
                return "\n".join(parts) + "\n\n¿Cotizamos algo? Mándame ej: 10 cemento, 5 varilla 3/8"

        return (
            "📍 Escríbenos directamente para darte la ubicación y horarios.\n\n"
            "Si quieres cotizar: mándame ej: 10 cemento, 5 varilla 3/8"
        )

    # 4.75) Parece producto sin cantidad
    if looks_like_product_phrase(user_text) and not re.search(r"\b\d+\b", user_text):
        return (
            "¿Cuántas piezas necesitas?\n"
            "Ejemplo: '10 sacos cemento' o '5 varilla 3/8'\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

    # =========================================================
    # 5) OPENAI FALLBACK — extracción inteligente
    # =========================================================
    if not openai_client:
        return "Estoy en mantenimiento. Intenta más tarde."

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Eres un asistente de ferretería. El usuario manda un pedido en español, "
                        "posiblemente con errores ortográficos o lenguaje informal. "
                        "Extrae los productos y cantidades. "
                        "Responde SOLO con JSON así: "
                        '[{"qty": 10, "product": "blocks"}, {"qty": 5, "product": "varilla 3/8"}] '
                        "Si no hay productos claros, responde: []"
                    )
                },
                {"role": "user", "content": user_text},
            ],
            temperature=0.1,
            max_tokens=200,
        )
        raw = resp.choices[0].message.content or "[]"
        raw = raw.replace("```json", "").replace("```", "").strip()
        items_gpt = json.loads(raw)

        if items_gpt:
            conn = get_conn()
            try:
                state = get_quote_state(company_id, wa_from) if wa_from else {}
                state = state or {}
                missing = []

                for it in items_gpt:
                    qty = int(it.get("qty") or 0)
                    prod = (it.get("product") or "").strip()
                    if not qty or not prod:
                        continue
                    result = smart_search(conn, company_id, prod, qty)
                    if result["status"] == "found":
                        state = cart_add_item(state, {
                            "sku": result["item"].get("sku"),
                            "name": result["item"].get("name"),
                            "unit": result["item"].get("unit") or "unidad",
                            "price": float(result["item"].get("price") or 0.0),
                            "vat_rate": result["item"].get("vat_rate"),
                            "qty": qty,
                        })
                    else:
                        missing.append({"qty": qty, "raw": prod, "candidates": result.get("candidates") or []})

                if missing:
                    state["pending"] = missing
                else:
                    state.pop("pending", None)

                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)

                return _build_reply_with_pending(state)
            finally:
                conn.close()

    except Exception as e:
        print("GPT FALLBACK ERROR:", repr(e))

    return "¿Me repites eso? No entendí bien tu pedido 🤔"


@app.post("/api/admin/rebuild-embeddings-public")
def rebuild_embeddings_public(company_id: str = "aa743e3f-1496-491d-99eb-02fcc5a839d5"):
    conn = get_conn()
    try:
        result = rebuild_embeddings_for_company(conn, company_id)
        return {"ok": True, **result}
    finally:
        conn.close()

@app.get("/api/_version")
def _version():
    return {"version": "pricebook-v2-2026-03-10", "unaccent": False}

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


@app.post("/api/company/settings")
def company_settings_update(request: Request, body: CompanySettingsBody):
    company_id = require_company_id(request)
    hours = (body.hours_text or "").strip() or None
    addr  = (body.address_text or "").strip() or None
    maps  = (body.google_maps_url or "").strip() or None
    mp_url = (body.mercadopago_url or "").strip() or None
    bank_name = (body.bank_name or "").strip() or None
    bank_acc_name = (body.bank_account_name or "").strip() or None
    bank_clabe = (body.bank_clabe or "").strip().replace(" ", "") or None
    bank_acc_num = (body.bank_account_number or "").strip().replace(" ", "") or None
    owner_phone = (body.owner_phone or "").strip().replace(" ", "") or None

    if bank_clabe and (not bank_clabe.isdigit() or len(bank_clabe) != 18):
        raise HTTPException(status_code=400, detail="CLABE inválida (debe ser 18 dígitos)")

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE companies
            SET hours_text=%s, address_text=%s, google_maps_url=%s, mercadopago_url=%s,
                bank_name=%s, bank_account_name=%s, bank_clabe=%s, bank_account_number=%s,
                owner_phone=%s, updated_at=now()
            WHERE id=%s
            RETURNING id
            """,
            (hours, addr, maps, mp_url, bank_name, bank_acc_name, bank_clabe, bank_acc_num, owner_phone, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        return {"ok": True}
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.get("/api/company/settings")
def company_settings_get(request: Request):
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT hours_text, address_text, google_maps_url,
                   mercadopago_url, bank_name, bank_account_name, bank_clabe, bank_account_number,
                   owner_phone
            FROM companies WHERE id=%s LIMIT 1
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        return {
            "ok": True,
            "settings": {
                "hours_text": row[0], "address_text": row[1], "google_maps_url": row[2],
                "mercadopago_url": row[3], "bank_name": row[4], "bank_account_name": row[5],
                "bank_clabe": row[6], "bank_account_number": row[7], "owner_phone": row[8],
            },
        }
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.get("/api/company/me")
def company_me(request: Request):
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id::text, name, slug, twilio_phone FROM companies WHERE id=%s LIMIT 1", (company_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        return {"ok": True, "company": {"id": row[0], "name": row[1], "slug": row[2], "twilio_phone": row[3]}}
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.post("/api/pricebook/rebuild-embeddings")
def rebuild_embeddings(request: Request):
    company_id = require_company_id(request)
    conn = get_conn()
    try:
        result = rebuild_embeddings_for_company(conn, company_id)
        return {"ok": True, **result}
    finally:
        conn.close()

@app.get("/api/health")
def api_health():
    return {"ok": True}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/webhook/whatsapp")
def whatsapp_verify(
    hub_mode: str = Query(default=None, alias="hub.mode"),
    hub_verify_token: str = Query(default=None, alias="hub.verify_token"),
    hub_challenge: str = Query(default=None, alias="hub.challenge"),
):
    expected = (os.getenv("WA_VERIFY_TOKEN") or "").strip()
    print("WA VERIFY DEBUG:", {
        "mode": hub_mode,
        "got_len": len(hub_verify_token or ""),
        "exp_len": len(expected or ""),
        "match": (hub_verify_token or "") == expected,
        "has_challenge": bool(hub_challenge),
    })
    if hub_mode == "subscribe" and hub_verify_token == expected and hub_challenge:
        return Response(content=str(hub_challenge), media_type="text/plain")
    raise HTTPException(status_code=403, detail="verify failed")

@app.get("/api/db/ping")
def db_ping():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("select 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/db/test")
def db_test():
    conn = None
    try:
        dsn = (os.getenv("DATABASE_URL") or "").strip()
        if not dsn:
            return {"db_ok": False, "error": "DATABASE_URL no está configurada en Render."}
        if "sslmode=" not in dsn:
            dsn = dsn + ("&" if "?" in dsn else "?") + "sslmode=require"
        conn = psycopg2.connect(dsn, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        cur.close()
        return {"db_ok": True, "result": result}
    except Exception as e:
        return {"db_ok": False, "error": str(e)}
    finally:
        if conn: conn.close()


@app.post("/api/auth/register")
def register(body: RegisterBody):
    email = (body.email or "").strip().lower()
    password = (body.password or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")
    if not password:
        raise HTTPException(status_code=400, detail="Password requerido")
    conn = None
    cur = None
    try:
        password_hash = hash_password(password)
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("insert into users (email, password_hash) values (%s, %s) returning id", (email, password_hash))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="No se pudo obtener user_id")
        user_id = row[0]
        return {"ok": True, "user_id": user_id}
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Email ya registrado")
    except HTTPException:
        raise
    except Exception as e:
        print("REGISTER ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error interno")
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.post("/api/whatsapp/provision")
def whatsapp_provision(request: Request):
    _ = get_user_from_session(request)
    raise HTTPException(status_code=501, detail="WhatsApp provisioning aún no disponible")

@app.post("/api/company/whatsapp/provision")
def company_whatsapp_provision(request: Request):
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT twilio_phone FROM companies WHERE id=%s", (company_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        existing = (row[0] or "").strip()
        if existing:
            return {"ok": True, "twilio_phone": existing, "already_assigned": True}
    finally:
        if cur: cur.close()
        if conn: conn.close()

    client = twilio_client()
    available = client.available_phone_numbers("US").local.list(area_code="571", limit=1)
    if not available:
        raise HTTPException(status_code=409, detail="No hay números disponibles (area_code=571)")
    chosen = available[0].phone_number
    purchased = client.incoming_phone_numbers.create(phone_number=chosen)
    twilio_phone = f"whatsapp:{purchased.phone_number}"

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE companies SET twilio_phone=%s, updated_at=now() WHERE id=%s RETURNING id", (twilio_phone, company_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Ese número ya está asignado a otra empresa")
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return {"ok": True, "twilio_phone": twilio_phone, "already_assigned": False}

@app.post("/api/auth/login")
def login(body: LoginBody, response: Response):
    email = (body.email or "").strip().lower()
    password = (body.password or "").strip()
    if not email or not password:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("select id, password_hash from users where email=%s and is_active=true", (email,))
        row = cur.fetchone()
        print("LOGIN email:", repr(email))
        print("LOGIN row found?:", bool(row))
        if not row:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        user_id, password_hash = row
        ok = verify_password(password, password_hash)
        print("LOGIN verify_password:", ok)
        if not ok:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        sid = create_session(conn, int(user_id))
        response.set_cookie(
            key=SESSION_COOKIE_NAME, value=sid, httponly=True, secure=True,
            samesite="none", domain=".cotizaexpress.com", path="/",
            max_age=SESSION_TTL_DAYS * 24 * 3600,
        )
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        print("LOGIN ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error interno")
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.get("/api/auth/me")
def auth_me(request: Request):
    u = get_user_from_session(request)
    return {"ok": True, "user": u}

@app.post("/api/auth/logout")
def auth_logout(request: Request, response: Response):
    sid = request.cookies.get(SESSION_COOKIE_NAME)
    if sid:
        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM sessions WHERE id=%s", (sid,))
        finally:
            if cur: cur.close()
            if conn: conn.close()
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/", domain=".cotizaexpress.com")
    return {"ok": True}

@app.get("/api/web/pricebook/template")
def download_template(request: Request):
    _ = get_user_from_session(request)
    wb = Workbook()
    ws = wb.active
    ws.title = "pricebook"
    ws.append(["nombre", "precio_base", "unidad", "sku", "vat_rate"])
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    filename = "plantilla-cotizaexpress.xlsx"
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

def _rebuild_embeddings_bg(company_id: str):
    try:
        print(f"BG EMBEDDINGS START: company={company_id}")
        conn = get_conn()
        try:
            result = rebuild_embeddings_for_company(conn, company_id)
            print(f"BG EMBEDDINGS DONE: {result}")
        finally:
            conn.close()
    except Exception as e:
        print(f"BG EMBEDDINGS ERROR: {repr(e)}")


@app.post("/api/pricebook/upload")
def pricebook_upload(
    authorization: str = Header(default=""),
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
):
    tenant = get_company_from_bearer(authorization)
    company_id = tenant["company_id"]

    if not file.filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(status_code=400, detail="Solo archivos .xlsx o .xlsm")

    conn = None
    cur = None
    upload_id = None

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pricebook_uploads (company_id, filename, status) VALUES (%s, %s, 'processing') RETURNING id",
            (company_id, file.filename),
        )
        upload_id = cur.fetchone()[0]

        content = file.file.read()
        wb = load_workbook(BytesIO(content))
        ws = wb.active

        header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        headers_raw = [str(h or "") for h in header_row]
        headers_norm = [h.strip().lower() for h in headers_raw]

        alias = {
            "nombre": "name", "producto": "name", "product": "name",
            "precio": "price", "precio_base": "price", "precio unitario": "price",
            "costo": "price", "cost": "price",
            "unidad": "unit", "uom": "unit",
            "vat_rate": "vat_rate", "iva": "vat_rate",
            "sku": "sku",
        }

        headers_mapped = [alias.get(h, h) for h in headers_norm]
        idx = {h: i for i, h in enumerate(headers_mapped)}

        required = {"name", "price"}
        missing_cols = required - set(headers_mapped)
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail={"error": f"Faltan columnas requeridas: {sorted(missing_cols)}", "headers_detectadas": headers_norm, "headers_mapeadas": headers_mapped},
            )

        parsed_rows = []
        for r in ws.iter_rows(min_row=2, values_only=True):
            if r is None or all(v is None or str(v).strip() == "" for v in r):
                continue
            name = str(r[idx["name"]]).strip() if r[idx["name"]] is not None else ""
            price_raw = r[idx["price"]] if idx.get("price") is not None else None
            if not name or price_raw is None:
                continue
            try:
                price = float(str(price_raw).replace("$", "").replace(",", "").strip())
            except Exception:
                continue

            unit = None
            if "unit" in idx and idx["unit"] < len(r) and r[idx["unit"]] is not None:
                unit = str(r[idx["unit"]]).strip() or None

            vat_rate = None
            if "vat_rate" in idx and idx["vat_rate"] < len(r) and r[idx["vat_rate"]] is not None:
                try:
                    vat_rate = float(r[idx["vat_rate"]])
                except Exception:
                    vat_rate = None

            sku = None
            if "sku" in idx and idx["sku"] < len(r) and r[idx["sku"]] is not None:
                sku_val = str(r[idx["sku"]]).strip()
                sku = sku_val if sku_val else None

            parsed_rows.append({"name": name, "price": price, "unit": unit, "vat_rate": vat_rate, "sku": sku})

        synonyms_map = {}

        def _clean_name(n):
            return re.sub(r'[\"\'\\]', '', n)

        def _batch_synonyms(names_batch: list) -> dict:
            if not openai_client:
                return {}
            try:
                numbered = "\n".join(f"{i+1}. {_clean_name(n)}" for i, n in enumerate(names_batch))
                resp = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": (
                            "Eres experto en ferreterías de México. Para cada producto numerado, "
                            "genera hasta 5 sinónimos coloquiales, typos comunes o marcas usadas como nombre genérico. "
                            'Responde SOLO en JSON válido así: {"1": "sin1, sin2", "2": "sin1, sin2"} '
                            "Sin explicación, sin markdown, solo el JSON."
                        )},
                        {"role": "user", "content": numbered}
                    ],
                    temperature=0.3, max_tokens=300,
                )
                raw = (resp.choices[0].message.content or "{}").strip()
                raw = raw.replace("```json", "").replace("```", "").strip()
                parsed = json.loads(raw)
                return {
                    names_batch[int(k)-1]: v
                    for k, v in parsed.items()
                    if k.isdigit() and int(k)-1 < len(names_batch)
                }
            except Exception as e:
                print("BATCH SYNONYMS ERROR:", repr(e))
                return {}

        rows_total = len(parsed_rows)
        rows_upserted = 0
        rows_skipped = 0

        for row in parsed_rows:
            name = row["name"]
            name_norm = norm_name(name)
            auto_syn = synonyms_map.get(name, "")
            try:
                cur.execute(
                    """
                    INSERT INTO pricebook_items
                        (company_id, sku, name, name_norm, unit, price, vat_rate, synonyms, source, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, 'excel', now())
                    ON CONFLICT (company_id, name_norm)
                    DO UPDATE SET
                        sku = EXCLUDED.sku, name = EXCLUDED.name, unit = EXCLUDED.unit,
                        price = EXCLUDED.price, vat_rate = EXCLUDED.vat_rate,
                        synonyms = COALESCE(NULLIF(pricebook_items.synonyms, ''), EXCLUDED.synonyms),
                        source = 'excel', updated_at = now()
                    """,
                    (company_id, row["sku"], name, name_norm, row["unit"], row["price"], row["vat_rate"], auto_syn),
                )
                rows_upserted += 1
            except Exception as e:
                print(f"ROW INSERT ERROR {name}:", repr(e))
                rows_skipped += 1

        cur.execute(
            "UPDATE pricebook_uploads SET status='success', rows_total=%s, rows_upserted=%s, error=NULL, finished_at=now() WHERE id=%s",
            (rows_total, rows_upserted, upload_id),
        )

        if background_tasks:
            background_tasks.add_task(_rebuild_embeddings_bg, company_id)
        else:
            try:
                rebuild_embeddings_for_company(conn, company_id)
            except Exception as e:
                print("EMBEDDINGS REBUILD ERROR:", repr(e))

        return {"ok": True, "company_id": company_id, "upload_id": str(upload_id), "rows_total": rows_total, "rows_upserted": rows_upserted, "rows_skipped": rows_skipped}

    except HTTPException:
        raise
    except Exception as e:
        print("UPLOAD ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/pricebook/items")
def pricebook_items(
    request: Request,
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
):
    conn = None
    cur = None
    try:
        _ = get_user_from_session(request)
        company_id = require_company_id(request)
        if not company_id:
            raise HTTPException(status_code=400, detail="No pude resolver company_id")
        conn = get_conn()
        cur = conn.cursor()
        if q:
            like = f"%{q.strip()}%"
            cur.execute(
                """
                select id, company_id, sku, name, unit, price, vat_rate, source, updated_at, created_at
                from pricebook_items
                where company_id = %s and (sku ilike %s or name ilike %s or name_norm ilike %s)
                order by name asc limit %s
                """,
                (company_id, like, like, like, limit),
            )
        else:
            cur.execute(
                "select id, company_id, sku, name, unit, price, vat_rate, source, updated_at, created_at from pricebook_items where company_id = %s order by name asc limit %s",
                (company_id, limit),
            )
        rows = cur.fetchall()
        items = []
        for r in rows:
            items.append({
                "id": r[0], "company_id": r[1], "sku": r[2],
                "name": r[3], "description": r[3], "unit": r[4],
                "price": float(r[5]) if r[5] is not None else None,
                "vat_rate": float(r[6]) if r[6] is not None else None,
                "source": r[7],
                "updated_at": r[8].isoformat() if r[8] else None,
                "created_at": r[9].isoformat() if r[9] else None,
            })
        return {"ok": True, "items": items}
    except HTTPException:
        raise
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print("PRICEBOOK ITEMS ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"pricebook_items failed: {type(e).__name__}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()


class PricebookItemCreateBody(BaseModel):
    name: str
    sku: Optional[str] = None
    unit: Optional[str] = None
    price: Optional[float] = None
    vat_rate: Optional[float] = 0.16
    source: Optional[str] = "manual"
    synonyms: Optional[str] = None


@app.post("/api/pricebook/items")
def pricebook_item_create(request: Request, body: PricebookItemCreateBody):
    _ = get_user_from_session(request)
    company_id = require_company_id(request)
    if not company_id:
        raise HTTPException(status_code=500, detail="DEFAULT_COMPANY_ID missing en Render")

    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name requerido")

    sku = (body.sku or "").strip() or None
    unit = (body.unit or "").strip() or None
    source = (body.source or "manual").strip() or "manual"

    price = body.price
    if price is not None:
        try: price = float(price)
        except Exception: raise HTTPException(status_code=400, detail="price inválido")
        if price < 0: raise HTTPException(status_code=400, detail="price debe ser >= 0")

    vat_rate = body.vat_rate
    if vat_rate is not None:
        try: vat_rate = float(vat_rate)
        except Exception: raise HTTPException(status_code=400, detail="vat_rate inválido")
        if vat_rate < 0 or vat_rate > 1: raise HTTPException(status_code=400, detail="vat_rate debe estar entre 0 y 1")

    name_norm = norm_name(name)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pricebook_items (company_id, sku, name, name_norm, unit, price, vat_rate, source, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (company_id, name_norm)
            DO UPDATE SET sku=EXCLUDED.sku, name=EXCLUDED.name, unit=EXCLUDED.unit,
                price=EXCLUDED.price, vat_rate=EXCLUDED.vat_rate, source=EXCLUDED.source, updated_at=now()
            RETURNING id
            """,
            (company_id, sku, name, name_norm, unit, price, vat_rate, source),
        )
        new_id = cur.fetchone()[0]
        try:
            upsert_single_embedding(conn, company_id, new_id, name, sku or "", unit or "")
        except Exception as e:
            print("SINGLE EMBEDDING ERROR:", repr(e))
        return {"ok": True, "id": str(new_id)}
    except IntegrityError as e:
        msg = str(e).lower()
        if "duplicate" in msg or "unique" in msg:
            raise HTTPException(status_code=409, detail="Producto ya existe (conflicto)")
        raise HTTPException(status_code=400, detail="Integridad inválida")
    except HTTPException:
        raise
    except Exception as e:
        print("PRICEBOOK CREATE ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="pricebook_item_create failed")
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.delete("/api/pricebook/items/{item_id}")
def pricebook_item_delete(request: Request, item_id: str):
    _ = get_user_from_session(request)
    company_id = require_company_id(request)
    if not company_id:
        raise HTTPException(status_code=500, detail="DEFAULT_COMPANY_ID missing en Render")
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM pricebook_items WHERE company_id = %s AND id = %s RETURNING id", (company_id, item_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        print("PRICEBOOK DELETE ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="pricebook_item_delete failed")
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.patch("/api/pricebook/items/{item_id}")
def pricebook_item_update(request: Request, item_id: str, body: PricebookItemCreateBody):
    _ = get_user_from_session(request)
    company_id = require_company_id(request)
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name requerido")
    sku = (body.sku or "").strip() or None
    unit = (body.unit or "").strip() or None
    price = body.price
    vat_rate = body.vat_rate
    synonyms = (body.synonyms or "").strip() or None
    name_norm = norm_name(name)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE pricebook_items
            SET name=%s, name_norm=%s, sku=%s, unit=%s, price=%s, vat_rate=%s, synonyms=%s, updated_at=now()
            WHERE id=%s AND company_id=%s
            RETURNING id
            """,
            (name, name_norm, sku, unit, price, vat_rate, synonyms, item_id, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        try:
            upsert_single_embedding(conn, company_id, item_id, name, sku or "", unit or "", synonyms or "")
        except Exception as e:
            print("EMBEDDING UPDATE ERROR:", repr(e))
        return {"ok": True}
    finally:
        cur.close()
        conn.close()

@app.get("/api/pricebook/items/{item_id}/synonyms-suggestions")
def synonyms_suggestions(request: Request, item_id: str):
    _ = get_user_from_session(request)
    company_id = require_company_id(request)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name, synonyms FROM pricebook_items WHERE id=%s AND company_id=%s", (item_id, company_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        name = (row[0] or "").strip()
        existing = (row[1] or "")
        suggestions = []
        if openai_client and name:
            try:
                resp = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Eres un experto en ferreterías de México. Dado un producto, devuelve palabras alternativas coloquiales que usarían clientes o ferreteros para pedirlo. Responde SOLO con las palabras separadas por coma, sin explicación, sin puntos, en minúsculas."},
                        {"role": "user", "content": f"Producto: {name}\nDame 4 sinónimos o nombres alternativos coloquiales."}
                    ],
                    temperature=0.3, max_tokens=60,
                )
                raw = resp.choices[0].message.content or ""
                suggestions = [s.strip().lower() for s in raw.split(",") if s.strip()]
            except Exception as e:
                print("SYNONYMS GPT ERROR:", repr(e))
        existing_list = [s.strip().lower() for s in existing.split(",") if s.strip()]
        suggestions = [s for s in suggestions if s not in existing_list]
        return {"ok": True, "suggestions": suggestions, "existing": existing_list}
    finally:
        cur.close()
        conn.close()

@app.post("/api/pricebook/deduplicate")
def pricebook_deduplicate(request: Request):
    company_id = require_company_id(request)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM pricebook_items
            WHERE id NOT IN (
                SELECT MIN(id::text)::uuid FROM pricebook_items WHERE company_id=%s GROUP BY name_norm
            ) AND company_id=%s
            """,
            (company_id, company_id),
        )
        deleted = cur.rowcount
        return {"ok": True, "deleted": deleted}
    finally:
        cur.close()
        conn.close()

@app.post("/api/companies")
def create_company(body: CompanyCreateBody):
    name = (body.name or "").strip()
    slug = (body.slug or "").strip() if body.slug else None
    key_name = (body.key_name or "default").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name requerido")
    token = generate_api_key()
    prefix = api_key_prefix(token)
    key_hash = api_key_hash(token)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO companies (name, slug) VALUES (%s, %s) RETURNING id", (name, slug))
        company_id = cur.fetchone()[0]
        cur.execute("INSERT INTO api_keys (company_id, name, prefix, key_hash) VALUES (%s, %s, %s, %s) RETURNING id", (company_id, key_name, prefix, key_hash))
        api_key_id = cur.fetchone()[0]
        return {"ok": True, "company_id": str(company_id), "api_key_id": str(api_key_id), "api_key": token, "api_key_prefix": prefix}
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Slug ya existe o conflicto")
    except Exception as e:
        print("CREATE COMPANY ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()

class TwilioPhoneBody(BaseModel):
    twilio_phone: str

@app.post("/api/admin/companies/{company_id}/twilio/provision")
def provision_twilio_number(company_id: str, request: Request):
    _ = get_user_from_session(request)
    client = twilio_client()
    available = client.available_phone_numbers("US").local.list(area_code="571", limit=1)
    if not available:
        raise HTTPException(status_code=409, detail="No hay números disponibles en ese area_code")
    chosen = available[0].phone_number
    purchased = client.incoming_phone_numbers.create(phone_number=chosen)
    twilio_phone = f"whatsapp:{purchased.phone_number}"
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE companies SET twilio_phone=%s WHERE id=%s RETURNING id", (twilio_phone, company_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Empresa no encontrada")
    finally:
        cur.close()
        conn.close()
    return {"ok": True, "company_id": company_id, "twilio_phone": twilio_phone}

@app.post("/api/companies/{company_id}/twilio_phone")
def set_company_twilio_phone(company_id: str, body: TwilioPhoneBody, request: Request):
    _ = get_user_from_session(request)
    tw = (body.twilio_phone or "").strip()
    if not tw.startswith("whatsapp:+"):
        raise HTTPException(status_code=400, detail="Formato inválido. Usa whatsapp:+E164")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE companies SET twilio_phone=%s, updated_at=now() WHERE id=%s RETURNING id", (tw, company_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")
        return {"ok": True}
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Ese número ya está asignado a otra empresa")
    finally:
        cur.close()
        conn.close()


def search_pricebook(conn, company_id: str, q: str, limit: int = 8):
    q = (q or "").strip()
    if not q:
        return []
    q_clean = extract_product_query(q)
    qn = norm_name(q_clean)
    specs = extract_specs(q_clean)
    tokens = [t for t in qn.split() if len(t) >= 3]
    if not tokens:
        tokens = [qn]
    where_parts = []
    params = [company_id]
    for tok in tokens[:6]:
        where_parts.append("name_norm LIKE %s")
        params.append(f"%{tok}%")
    where_sql = " AND ".join(where_parts)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT sku, name, unit, price, vat_rate, updated_at
            FROM pricebook_items
            WHERE company_id=%s AND ({where_sql} OR sku ILIKE %s OR name ILIKE %s)
            LIMIT %s
            """,
            (*params, f"%{q_clean}%", f"%{q_clean}%", max(limit, 12)),
        )
        rows = cur.fetchall()
        items = []
        for sku, name, unit, price, vat_rate, updated_at in rows:
            items.append({
                "sku": sku, "name": name, "unit": unit,
                "price": float(price) if price is not None else None,
                "vat_rate": float(vat_rate) if vat_rate is not None else None,
                "updated_at": updated_at.isoformat() if updated_at else None,
            })
        constrained = [it for it in items if passes_constraints(it["name"], specs)]
        pool = constrained if constrained else items
        best, score = rank_best_match(q_clean, pool)
        if not best or score < 72:
            return []
        return [best]
    finally:
        cur.close()

def ner_extract_items(user_text: str):
    if not openai_client:
        return []
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    "Eres asistente de ferretería mexicana. Extrae productos y cantidades "
                    "de mensajes con posibles errores ortográficos o lenguaje informal. "
                    "Responde SOLO JSON sin explicación: "
                    '[{"qty": 10, "product": "cemento"}, {"qty": 5, "product": "varilla 3/8"}] '
                    "Si no hay productos claros, responde: []"
                )},
                {"role": "user", "content": user_text},
            ],
            temperature=0.1, max_tokens=200,
        )
        raw = (resp.choices[0].message.content or "[]").replace("```json", "").replace("```", "").strip()
        parsed = json.loads(raw)
        return [(int(it["qty"]), str(it["product"]).strip()) for it in parsed if it.get("qty") and it.get("product")]
    except Exception as e:
        print("NER ERROR:", repr(e))
        return []

def extract_qty_items_robust(text: str):
    t = (text or "").strip()
    t = re.sub(r"[•;]", "\n", t)
    t = re.sub(r"^\s*(ocupo|necesito|quiero|quisiera|dame|deme|manda|mandame|mandeme|pasame|pásame|paseme|necesitamos|queremos|ocupamos|me puede dar|me pueden dar|me das|me mandas|favor de|necesito cotizar|quiero cotizar)\s+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^\s*(me\s+)?(puede[ns]?|podría[ns]?|podrías)\s+(cotizar|dar|mandar|pasar)\s+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(cotiza|cotización|cotizacion|precio|precios|por favor|porfa|pls)\b", " ", t, flags=re.IGNORECASE)
    # Protege fracciones 1/4, 5/8
    t = re.sub(r"(\d+)\s*/\s*(\d+)", r"\1_\2", t)
    items = []
    lines = [l.strip() for l in re.split(r"[\n\r]+", t) if l.strip()]
    for line in lines:
        parts = [p.strip() for p in line.split(",") if p.strip()]
        for part in parts:
            m = re.match(r"^\s*(\d+)\s+(.+)$", part.strip())
            if m:
                qty = int(m.group(1))
                prod = m.group(2).replace("_", "/").strip()
                prod = re.sub(r"\bde\b", "", prod, flags=re.IGNORECASE).strip()
                prod = re.sub(r"\s+", " ", prod).strip()
                if prod and qty > 0:
                    items.append((qty, prod))
    return items

@app.post("/api/chat")
async def chat(req: ChatRequest, authorization: str = Header(default="")):
    app_id = (getattr(req, "app", None) or "cotizabot").lower().strip()
    user_text = (getattr(req, "message", None) or "").strip()

    if not user_text:
        return {"reply": "Escribe un mensaje para poder ayudarte."}

    if app_id == "cotizabot":
        qty, prod_query = extract_qty_and_product(user_text)
        if qty and prod_query:
            try:
                tenant = get_company_from_bearer(authorization)
                company_id = tenant["company_id"]
                conn = get_conn()
                try:
                    items = search_pricebook(conn, company_id, prod_query, limit=1)
                finally:
                    conn.close()
                if not items:
                    return {"reply": f"No encontré '{prod_query}' en tu catálogo."}
                it = items[0]
                unit = it.get("unit") or "unidad"
                price = float(it.get("price") or 0)
                subtotal = qty * price
                iva = subtotal * 0.16
                total = subtotal + iva
                return {"reply": (
                    "Cotización rápida:\n"
                    f"- {qty} {unit} de {it['name']} x ${price:,.2f} = ${subtotal:,.2f}\n"
                    f"IVA (16%): ${iva:,.2f}\n"
                    f"Total: ${total:,.2f}\n\n"
                    "¿Agregamos otro producto?"
                )}
            except Exception:
                pass

        if looks_like_price_question(user_text):
            try:
                tenant = get_company_from_bearer(authorization)
                company_id = tenant["company_id"]
                conn = get_conn()
                try:
                    items = search_pricebook(conn, company_id, user_text, limit=8)
                finally:
                    conn.close()
                if items:
                    lines = []
                    for it in items[:8]:
                        unit = f" / {it['unit']}" if it.get("unit") else ""
                        price = it["price"]
                        lines.append(f"- {it['name']}: ${price:,.2f}{unit}")
                    reply = (
                        "Encontré estos precios en tu catálogo:\n"
                        + "\n".join(lines)
                        + "\n\nDime cantidades para armar cotización (ej: 10 tablaroca ultralight)."
                    )
                    return {"reply": reply}
            except Exception:
                pass

    if not openai_client:
        return {"reply": "Falta configurar OPENAI_API_KEY en Render."}

    greetings = {"hola", "buenas", "hey", "holi"}
    if norm_name(user_text) in greetings:
        return {"reply": (
            "👋 ¡Hola! Puedo cotizarte materiales.\n\n"
            "Mándame tu pedido así:\n"
            "👉 10 tablaroca ultralight, 5 postes 4.10\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )}

    if app_id == "cotizabot":
        system_prompt = COTIZABOT_SYSTEM_PROMPT
    elif app_id == "dondever":
        system_prompt = DONDEVER_SYSTEM_PROMPT
    elif app_id == "entiendeusa":
        system_prompt = ENTIENDEUSA_SYSTEM_PROMPT
    else:
        system_prompt = "Eres un asistente útil. Responde claro y directo."

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        temperature=0.3,
    )
    reply = response.choices[0].message.content or ""
    return {"reply": reply}

_processed_sids: set = set()
_image_cache: dict = {}


@app.post("/webhook/twilio")
async def twilio_webhook(
    From: str = Form(...),
    To: str = Form(...),
    Body: str = Form(default=""),
    MessageSid: str = Form(default=""),
    NumMedia: str = Form(default="0"),
    MediaUrl0: str = Form(default=""),
    MediaContentType0: str = Form(default=""),
):
    From = normalize_wa(From)
    To = normalize_wa(To)
    Body = (Body or "").strip()

    TWIML_OK = Response(
        content='<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
        media_type="text/xml",
    )

    if MessageSid:
        cache_key = f"msid:{MessageSid}"
        if cache_key in _processed_sids:
            print(f"DUPLICATE WEBHOOK ignored: {MessageSid}")
            return TWIML_OK
        _processed_sids.add(cache_key)
        if len(_processed_sids) > 1000:
            _processed_sids.clear()

    try:
        print("TWILIO IN:", {"from": From, "to": To, "body": Body})

        if not Body and int(NumMedia or 0) > 0 and MediaUrl0:
            if "image" in (MediaContentType0 or ""):
                cached = _image_cache.get(MessageSid)
                if cached:
                    Body = cached
                    print("TWILIO IMAGE FROM CACHE:", Body[:200])
                else:
                    try:
                        twilio_sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
                        twilio_token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
                        img_resp = requests.get(MediaUrl0, auth=(twilio_sid, twilio_token), timeout=15)
                        img_resp.raise_for_status()
                        extracted = extract_text_from_image(img_resp.content)
                        if extracted:
                            Body = extracted
                            _image_cache[MessageSid] = extracted
                            if len(_image_cache) > 200:
                                _image_cache.clear()
                            print("TWILIO IMAGE EXTRACTED:", Body[:200])
                        else:
                            twilio_send_whatsapp(to_user_whatsapp=From, text="📷 Vi tu imagen pero no encontré una lista de productos.\n\nMándame el pedido así:\n10 cemento, 5 varilla 3/8")
                            return TWIML_OK
                    except Exception as e:
                        print("TWILIO IMAGE ERROR:", repr(e))
                        twilio_send_whatsapp(to_user_whatsapp=From, text="No pude leer la imagen 😔 Intenta enviarla más clara o escribe el pedido.")
                        return TWIML_OK

        if not Body:
            twilio_send_whatsapp(to_user_whatsapp=From, text="Solo proceso mensajes de texto por ahora 📝")
            return TWIML_OK

        company = get_company_by_twilio_number(To)
        print("TWILIO company:", company)

        if not company:
            twilio_send_whatsapp(to_user_whatsapp=From, text="Hola 👋 Este número aún no está ligado a una empresa.")
            return TWIML_OK

        reply = build_reply_for_company(company["company_id"], Body, wa_from=From)

        def _sections_to_text(sections: list) -> str:
            lines = []
            for section in (sections or []):
                title = (section.get("title") or "").strip()
                if title:
                    lines.append(f"\n{title}")
                for row in (section.get("rows") or []):
                    row_id = (row.get("id") or "").upper().replace("PICK_", "")
                    row_title = (row.get("title") or "").strip()
                    row_desc = (row.get("description") or "").strip()
                    lines.append(f"  {row_id}) {row_title} — {row_desc}")
            return "\n".join(lines)

        if isinstance(reply, dict):
            reply_type = reply.get("type", "")
            if reply_type == "text_then_list_sections":
                cart_text = (reply.get("text") or "").strip()
                if cart_text:
                    twilio_send_whatsapp(to_user_whatsapp=From, text=cart_text)
                opciones = (reply.get("body") or "") + _sections_to_text(reply.get("sections") or [])
                opciones += "\n\n✅ Responde con el código, ej: A1"
                twilio_send_whatsapp(to_user_whatsapp=From, text=opciones.strip())
            elif reply_type in ("list_sections", "list"):
                lines = [(reply.get("body") or "")]
                lines.append(_sections_to_text(reply.get("sections") or reply.get("options") or []))
                lines.append("\n✅ Responde con el número, ej: A1")
                reply_text = "\n".join(lines).strip()
                twilio_send_whatsapp(to_user_whatsapp=From, text=reply_text)
            else:
                reply_text = (reply.get("body") or "").strip() or "¿Me repites eso?"
                twilio_send_whatsapp(to_user_whatsapp=From, text=reply_text)
        else:
            reply_text = (reply or "").strip() or "¿Me repites eso?"
            twilio_send_whatsapp(to_user_whatsapp=From, text=reply_text)

        print("WHATSAPP ENVIADO OK")
        return TWIML_OK

    except Exception as e:
        print("TWILIO WEBHOOK ERROR:", repr(e))
        traceback.print_exc()
        try:
            twilio_send_whatsapp(to_user_whatsapp=From, text="Error interno. Intenta de nuevo en 1 minuto.")
        except Exception:
            pass
        return TWIML_OK
