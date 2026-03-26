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
from generate_quote_pdf import build_quote_pdf, generate_folio

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
from pydantic import BaseModel, validator


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

# Middleware (CORS)

# -------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://cotizaexpress.com",
        "https://www.cotizaexpress.com",
        "https://app.cotizaexpress.com",
        "https://whatsapp-quotes-test.preview.emergentagent.com",
        "https://whatsapp-quotes-test.preview.static.emergentagent.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------

# Basic endpoints

# -------------------------

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
    has_number = bool(re.search(r"\d", t))
    if not tokens and not has_number:
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

def normalize_display_name(name: str) -> str:
    """Primera letra mayúscula, resto minúsculas. Ej: 'CLAVO 1 1/2' → 'Clavo 1 1/2'"""
    n = (name or "").strip()
    return (n[0].upper() + n[1:].lower()) if n else n

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
    # Solo quitar "es" si la base termina en consonante (canales→canal, postes→poste)
    if t.endswith("es"):
        base = t[:-2]
        if len(base) >= 3 and base[-1] not in "aeiouáéíóú":
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
        "donde se ubican", "donde estan", "dónde están", "donde quedan",
        "como llego", "cómo llego", "donde queda", "dónde queda",
        "donde se encuentran", "donde los encuentro", "donde es", "dónde es",
        "donde están ubicados", "como los encuentro",
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

def _get_company_discount(company_id: str) -> tuple:
    """Devuelve (threshold, percent) o (None, None) si no hay descuento configurado."""
    if not company_id:
        return None, None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT discount_threshold, discount_percent FROM companies WHERE id=%s LIMIT 1",
            (company_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0] and row[1]:
            return float(row[0]), float(row[1])
    except Exception:
        pass
    return None, None


def cart_render_quote(state: dict, company_id: str = "", client_phone: str = "") -> str:
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
        lines.append(f"• {qty} x {name} — ${subtotal:,.0f}")

    # ── Descuento por volumen ─────────────────────────────────────────────────
    descuento_txt = ""
    total_final = total
    if company_id:
        threshold, percent = _get_company_discount(company_id)
        if threshold and percent and total >= threshold:
            descuento = round(total * percent / 100)
            total_final = total - descuento
            descuento_txt = (
                f"\n🏷️ Descuento {percent:.0f}% por volumen: -${descuento:,.0f}"
            )

    folio_txt = ""
    if company_id and client_phone:
        try:
            existing_folio = state.get("folio")
            folio = save_quote(company_id, client_phone, cart, existing_folio=existing_folio)
            if not existing_folio:
                state["folio"] = folio
            folio_txt = f"\n📋 Folio: *{folio}*"
        except Exception as e:
            print("CART RENDER SAVE QUOTE ERROR:", repr(e))

    return (
        "Cotización:\n"
        + "\n".join(lines)
        + descuento_txt
        + f"\n\n*Total: ${total_final:,.0f}* (IVA incluido)"
        + folio_txt
        + "\n\n💳 Escribe *pagar* y te mandamos datos bancarios o link para pago con tarjeta."
    )

def api_key_prefix(token: str) -> str:
    return token[:API_KEY_PREFIX_LEN]

def api_key_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# -------------------------
# DB
# -------------------------
def save_search_miss(company_id: str, term: str):
    """Guarda términos que el bot no encontró para aprendizaje continuo."""
    if not company_id or not term or len(term.strip()) < 3:
        return
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO search_misses (company_id, term, created_at)
            VALUES (%s, %s, now())
            ON CONFLICT DO NOTHING
            """,
            (company_id, term.strip().lower()),
        )
        cur.close()
        conn.close()
    except Exception as e:
        print("SAVE_MISS ERROR:", repr(e))

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
        print("SAVE QUOTE ERROR:", repr(e))
    finally:
        cur.close()
        conn.close()

    return folio

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
    owner_phone_clean = (owner_phone or "").replace("+", "").replace("whatsapp:", "").strip()
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
    send_whatsapp_text(wa_api_key, phone_number_id, owner_phone_clean, msg)

def notify_owner_comprobante(wa_api_key: str, phone_number_id: str, owner_phone: str,
                              client_phone: str, state: dict):
    owner_phone_clean = (owner_phone or "").replace("+", "").replace("whatsapp:", "").strip()
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
    send_whatsapp_text(wa_api_key, phone_number_id, owner_phone_clean, msg)

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
    print(f"BEARER DEBUG: prefix={repr(prefix)} hash={repr(key_hash)}")
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

class PricebookItemCreateBody(BaseModel):
    name: str
    sku: Optional[str] = None
    unit: Optional[str] = None
    price: Optional[float] = None
    vat_rate: Optional[float] = 0.16
    source: Optional[str] = "manual"
    synonyms: Optional[str] = None

# ── NUEVO: schema para PATCH (todos los campos opcionales) ────────────────────
class PricebookItemUpdateBody(BaseModel):
    name: Optional[str] = None
    sku: Optional[str] = None
    unit: Optional[str] = None
    price: Optional[float] = None
    vat_rate: Optional[float] = None
    synonyms: Optional[str] = None
# ─────────────────────────────────────────────────────────────────────────────


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

        _st_check = get_quote_state(company["company_id"], from_phone) or {}
        if _st_check.get("awaiting_comprobante"):
            try:
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
            _st_check.pop("awaiting_comprobante", None)
            upsert_quote_state(company["company_id"], from_phone, _st_check)
            _bot_reply_comprobante = "✅ ¡Comprobante recibido! Le avisamos a la empresa y en breve te confirman tu pedido. 🙏"
            log_message(company["company_id"], from_phone, "user", "📎 [Imagen de comprobante de pago]")
            log_message(company["company_id"], from_phone, "bot", _bot_reply_comprobante, {
                "cart":  _st_check.get("cart") or [],
                "folio": _st_check.get("folio") or None,
            })
            send_whatsapp_text(
                wa_api_key=company["wa_api_key"],
                phone_number_id=company["wa_phone_number_id"],
                to=from_phone,
                text=_bot_reply_comprobante,
            )
            return {"ok": True}

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

    if text:
        log_message(company["company_id"], from_phone, "user", text)

    reply = build_reply_for_company(
        company["company_id"], text,
        wa_from=from_phone,
        is_interactive=(msg_type == "interactive"),
    )

    if not reply:
        return {"ok": True}

    def _reply_text_for_log(r) -> str:
        if isinstance(r, dict):
            rtype = r.get("type", "")
            if rtype == "text_then_list_sections":
                body = (r.get("text") or "") + "\n" + (r.get("body") or "")
            else:
                body = r.get("body") or ""
            for section in (r.get("sections") or []):
                for row in (section.get("rows") or []):
                    body += "\n  " + row.get("id","") + " " + row.get("title","") + " " + row.get("description","")
            for opt in (r.get("options") or []):
                body += "\n  • " + str(opt)
            return body.strip()
        return (r or "").strip()

    try:
        _state_for_log = get_quote_state(company["company_id"], from_phone) or {}
        _log_extra = {
            "cart":  _state_for_log.get("cart") or [],
            "folio": _state_for_log.get("folio") or None,
        }
    except Exception:
        _log_extra = {}
    log_message(company["company_id"], from_phone, "bot", _reply_text_for_log(reply), _log_extra)

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

# ── Conversation logging ──────────────────────────────────────────────────────

def log_message(company_id: str, client_phone: str, role: str, message: str, extra: dict = None):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            """
            INSERT INTO conversation_messages
                (company_id, client_phone, role, message, extra, created_at)
            VALUES (%s, %s, %s, %s, %s::jsonb, now())
            """,
            (company_id, client_phone, role, (message or "")[:4000],
             json.dumps(extra or {})),
        )
        cur.close()
        conn.close()
    except Exception as e:
        print("LOG_MESSAGE ERROR:", repr(e))


@app.get("/api/conversations")
def list_conversations(
    request: Request,
    authorization: str = Header(default=""),
    limit: int = Query(default=30, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    if authorization and authorization.lower().startswith("bearer "):
        company_id = get_company_from_bearer(authorization)["company_id"]
    else:
        company_id = require_company_id(request)

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                client_phone,
                MAX(created_at)                                          AS last_at,
                (array_agg(message   ORDER BY created_at DESC))[1]       AS last_message,
                (array_agg(role      ORDER BY created_at DESC))[1]       AS last_role,
                (array_agg(extra     ORDER BY created_at DESC))[1]       AS last_extra,
                COUNT(*)                                                  AS total_msgs
            FROM conversation_messages
            WHERE company_id = %s::uuid
            GROUP BY client_phone
            ORDER BY last_at DESC
            LIMIT %s OFFSET %s
            """,
            (company_id, limit, offset),
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT COUNT(DISTINCT client_phone) FROM conversation_messages WHERE company_id = %s::uuid",
            (company_id,),
        )
        total = cur.fetchone()[0]
        convs = []
        for r in rows:
            extra = r[4] or {}
            convs.append({
                "client_phone": r[0],
                "last_at":      r[1].isoformat() if r[1] else None,
                "last_message": r[2],
                "last_role":    r[3],
                "cart":         extra.get("cart") or [],
                "folio":        extra.get("folio") or None,
                "total_msgs":   r[5],
            })
        return {"ok": True, "conversations": convs, "total": total}
    finally:
        cur.close()
        conn.close()


@app.get("/api/conversations/{client_phone}")
def get_conversation(
    request: Request,
    client_phone: str,
    authorization: str = Header(default=""),
    limit: int = Query(default=100, ge=1, le=500),
):
    if authorization and authorization.lower().startswith("bearer "):
        company_id = get_company_from_bearer(authorization)["company_id"]
    else:
        company_id = require_company_id(request)

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(
            """
            SELECT role, message, extra, created_at
            FROM conversation_messages
            WHERE company_id = %s::uuid AND client_phone = %s
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (company_id, client_phone, limit),
        )
        rows = cur.fetchall()
        messages = [
            {
                "role":       r[0],
                "message":    r[1],
                "cart":       (r[2] or {}).get("cart") or [],
                "folio":      (r[2] or {}).get("folio") or None,
                "created_at": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ]
        return {"ok": True, "client_phone": client_phone, "messages": messages}
    finally:
        cur.close()
        conn.close()

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

# ── Módulo Construcción Ligera ────────────────────────────────────────────────


# ── Módulo Construcción Ligera ────────────────────────────────────────────────

import math

CONSTRUCCION_TIPOS = {
    "muro tablaroca": {
        "label": "Muro Tablaroca",
        "inputs": ["alto_muro", "largo_muro"],
        "preguntas": {
            "alto_muro": "📐 ¿Cuántos metros de *alto* tiene el muro? (ej: 2.44)",
            "largo_muro": "📏 ¿Cuántos metros de *largo* tiene el muro? (ej: 10)",
        },
    },
    "muro durock": {
        "label": "Muro Durock",
        "inputs": ["alto_muro", "largo_muro"],
        "preguntas": {
            "alto_muro": "📐 ¿Cuántos metros de *alto* tiene el muro? (ej: 2.44)",
            "largo_muro": "📏 ¿Cuántos metros de *largo* tiene el muro? (ej: 10)",
        },
    },
    "plafon tablaroca": {
        "label": "Plafón Tablaroca",
        "inputs": ["largo", "ancho"],
        "preguntas": {
            "largo": "📏 ¿Cuántos metros de *largo* tiene el plafón? (ej: 12)",
            "ancho": "📐 ¿Cuántos metros de *ancho* tiene el plafón? (ej: 8)",
        },
    },
    "plafon reticulado": {
        "label": "Plafón Reticulado",
        "inputs": ["largo", "ancho"],
        "preguntas": {
            "largo": "📏 ¿Cuántos metros de *largo* tiene el plafón? (ej: 12)",
            "ancho": "📐 ¿Cuántos metros de *ancho* tiene el plafón? (ej: 5)",
        },
    },
}

CONSTRUCCION_PRODUCTOS = {
    "muro tablaroca": [
        "Tablaroca ultralight usg",
        "Poste 6.35 x 3.05 cal 26",
        "Canal 6.35 x 3.05 cal 26",
        "Redimix 21.8 kg usg",
        "Pija 6 x 1",
        "Pija framer",
        "Perfacinta",
    ],
    "muro durock": [
        "Durock usg",
        "Poste 6.35 x 3.05 cal 20",
        "Canal 6.35 x 3.05 cal 22",
        "Basecoat usg",
        "Pija para durock",
        "Pija framer",
        "Cinta fibra de vidrio",
    ],
    "plafon tablaroca": [
        "Tablaroca ultralight usg",
        "Canal listón cal 26",
        "Ángulo de amarre cal 26",
        "Canaleta de carga cal 24",
        "Redimix 21.8 kg usg",
        "Pija 6 x 1",
        "Pija framer",
        "Perfacinta",
        "Alambre galvanizado liso cal 12.5",
    ],
    "plafon reticulado": [
        "Plafón radar 61 x 61",
        "Tee principal",
        "Tee 1.22",
        "Tee 61",
        "Ángulo perimetral",
        "Alambre galvanizado liso cal 12.5",
    ],
}


def _buscar_precio_exacto(conn, company_id: str, nombre: str):
    nombre_norm = norm_name(nombre)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT sku, name, unit, price, vat_rate
            FROM pricebook_items
            WHERE company_id = %s AND name_norm = %s
            LIMIT 1
            """,
            (company_id, nombre_norm),
        )
        row = cur.fetchone()
        if row:
            return {"sku": row[0], "name": row[1], "unit": row[2],
                    "price": float(row[3]) if row[3] else None,
                    "vat_rate": float(row[4]) if row[4] else None}
        return None
    finally:
        cur.close()


def _calc_muro_tablaroca(alto: float, largo: float) -> list:
    m2 = alto * largo
    tablaroca = math.ceil(math.ceil(m2 / (1.22 * 2.44) * 2 * 1.03) * 1.03)
    pijas = math.ceil(tablaroca * 30)
    return [
        ("Tablaroca ultralight usg",          tablaroca),
        ("Canal 6.35 x 3.05 cal 26",          math.ceil((largo / 3) * 2)),
        ("Poste 6.35 x 3.05 cal 26",          math.ceil(alto / 0.61) * math.ceil(largo / 3.05)),
        ("Pija 6 x 1",                        pijas),
        ("Pija framer",                       math.ceil(pijas / 2)),
        ("Perfacinta",                        math.ceil((m2 / 2.44) / 20)),
        ("Redimix 21.8 kg usg",               math.ceil(m2 / 14)),
    ]


def _calc_muro_durock(alto: float, largo: float) -> list:
    m2 = alto * largo
    durock = math.ceil(math.ceil(m2 / (1.22 * 2.44) * 2 * 1.03) * 1.03)
    pijas = math.ceil(durock * 30)
    return [
        ("Durock usg",                        durock),
        ("Canal 6.35 x 3.05 cal 22",          math.ceil((largo / 3) * 2)),
        ("Poste 6.35 x 3.05 cal 20",          math.ceil(alto / 0.61) * math.ceil(largo / 3.05)),
        ("Pija para durock",                  pijas),
        ("Pija framer",                       math.ceil(pijas / 2)),
        ("Cinta fibra de vidrio",             math.ceil((m2 / 2.44) / 20)),
        ("Basecoat usg",                      math.ceil(m2 / 4)),
    ]


def _calc_plafon_tablaroca(largo: float, ancho: float) -> list:
    m2 = largo * ancho
    tablaroca = math.ceil(m2 / 2.9768 * 1.07)
    pijas = math.ceil(tablaroca * 30)
    return [
        ("Tablaroca ultralight usg",          tablaroca),
        ("Canal listón cal 26",               math.ceil(((m2 / 0.61) * 1.05) / 3.05) + 2),
        ("Canaleta de carga cal 24",          math.ceil(((m2 / 1.22) * 1.05) / 3.05)),
        ("Ángulo de amarre cal 26",           math.ceil(((largo * 2) + (ancho * 2)) / 3.05)),
        ("Pija 6 x 1",                        pijas),
        ("Pija framer",                       math.ceil(pijas / 2)),
        ("Perfacinta",                        math.ceil((m2 * 0.8 * 1.05) / 75)),
        ("Redimix 21.8 kg usg",               math.ceil((m2 * 0.65 * 1.05) / 21.8)),
        ("Alambre galvanizado liso cal 12.5", math.ceil(m2 / 20)),
    ]


def _calc_plafon_reticulado(largo: float, ancho: float) -> list:
    m2 = largo * ancho
    return [
        ("Plafón radar 61 x 61",              math.ceil(m2 / 0.36 * 1.03)),
        ("Tee principal",                     math.ceil(m2 * 0.29)),
        ("Tee 1.22",                          math.ceil(m2 * 1.4)),
        ("Tee 61",                            math.ceil(m2 * 1.4)),
        ("Ángulo perimetral",                 math.ceil(((largo * 2) + (ancho * 2)) / 3.05)),
        ("Alambre galvanizado liso cal 12.5", math.ceil(m2 / 20)),
    ]


def _is_construccion_trigger(text: str) -> bool:
    t = norm_name(text)
    triggers = [
        "calcula", "calcular",
        "construccion", "construcción", "construcion",
        "calcular material", "calcular materiales",
        "cuantos materiales", "cuántos materiales",
        "material para", "materiales para",
        "construccion ligera", "construcción ligera",
        "drywall", "tablaroca construccion",
        "muro tablaroca", "muro durock",
        "plafon tablaroca", "plafon reticulado",
        "cuanto material", "cuánto material",
        "cuanto necesito", "cuánto necesito",
        "m2 muro", "m2 plafon", "m2 tablaroca", "m2 durock",
        "metros muro", "metros plafon",
        "m2 de muro", "m2 de plafon",
        "metros de muro", "metros de plafon",
        "metros cuadrados",
    ]
    if any(tr in t for tr in triggers):
        return True
    if re.search(r"\d+\s*m2", t):
        if any(w in t for w in ["muro", "plafon", "tablaroca", "durock", "pared", "techo"]):
            return True
    return False


def _handle_construccion(company_id: str, user_text: str, wa_from: str):
    state = get_quote_state(company_id, wa_from) or {}
    cs = state.get("construccion_state") or {}
    t = norm_name(user_text)

    # ── Paso 0: elegir tipo ───────────────────────────────────────────────────
    if not cs.get("tipo"):
        tipo_detectado = None
        for key in CONSTRUCCION_TIPOS:
            if key in t:
                tipo_detectado = key
                break
        if not tipo_detectado:
            mapeo = {
                "plafón tablaroca": "plafon tablaroca",
                "plafón reticulado": "plafon reticulado",
                "plafon tablaroca": "plafon tablaroca",
                "plafon reticulado": "plafon reticulado",
            }
            for k, v in mapeo.items():
                if k in t:
                    tipo_detectado = v
                    break

        if not tipo_detectado:
            state["construccion_state"] = {"step": "eligiendo_tipo"}
            upsert_quote_state(company_id, wa_from, state)
            return {
                "type": "list",
                "body": "🏗️ *Calculadora de Construcción Ligera*\n\n¿Qué tipo de construcción vas a hacer?",
                "options": ["Muro tablaroca", "Muro durock", "Plafón tablaroca", "Plafón reticulado"],
                "button_label": "Elegir tipo",
            }
        else:
            cs["tipo"] = tipo_detectado
            cs["datos"] = {}

    # ── Paso 1: si estaban eligiendo tipo ────────────────────────────────────
    if cs.get("step") == "eligiendo_tipo":
        tipo_encontrado = None
        for key in CONSTRUCCION_TIPOS:
            label = CONSTRUCCION_TIPOS[key]["label"].lower()
            if key in t or label in t:
                tipo_encontrado = key
                break
        if not tipo_encontrado:
            return {
                "type": "list",
                "body": "Por favor elige una opción 👇",
                "options": ["Muro tablaroca", "Muro durock", "Plafón tablaroca", "Plafón reticulado"],
                "button_label": "Elegir tipo",
            }
        cs["tipo"] = tipo_encontrado
        cs["step"] = None
        cs["datos"] = {}

    tipo_key = cs.get("tipo")
    tipo_cfg = CONSTRUCCION_TIPOS.get(tipo_key)
    if not tipo_cfg:
        state.pop("construccion_state", None)
        upsert_quote_state(company_id, wa_from, state)
        return "No reconocí el tipo. Escribe *construccion* para intentar de nuevo."

    datos = cs.get("datos") or {}
    inputs = tipo_cfg["inputs"]

    # ── Detectar m2 directo ───────────────────────────────────────────────────
    m2_match = re.search(r"(\d+(?:[.,]\d+)?)\s*m2", t)
    if m2_match and not datos:
        m2_val = float(m2_match.group(1).replace(",", "."))
        if tipo_key in ("muro tablaroca", "muro durock"):
            datos["alto_muro"] = 2.44
            datos["largo_muro"] = round(m2_val / 2.44, 2)
        else:
            lado = round(m2_val ** 0.5, 2)
            datos["largo"] = lado
            datos["ancho"] = lado
        cs["datos"] = datos

    # ── Leer número si están esperando input ─────────────────────────────────
    if cs.get("esperando_input") and not m2_match:
        campo = cs["esperando_input"]
        nums = re.findall(r"\d+(?:[.,]\d+)?", user_text.replace(",", "."))
        if nums:
            try:
                val = float(nums[0].replace(",", "."))
                datos[campo] = val
                cs["datos"] = datos
                cs.pop("esperando_input", None)
            except Exception:
                pass

    # ── Verificar qué inputs faltan ───────────────────────────────────────────
    faltantes = [inp for inp in inputs if inp not in datos]

    if faltantes:
        siguiente = faltantes[0]
        cs["esperando_input"] = siguiente
        state["construccion_state"] = cs
        upsert_quote_state(company_id, wa_from, state)
        pregunta = tipo_cfg["preguntas"][siguiente]
        label = tipo_cfg["label"]
        return f"🏗️ *{label}*\n\n{pregunta}"

    # ── Todos los datos listos → calcular ─────────────────────────────────────
    try:
        if tipo_key == "muro tablaroca":
            materiales = _calc_muro_tablaroca(datos["alto_muro"], datos["largo_muro"])
            m2 = datos["alto_muro"] * datos["largo_muro"]
            dim_txt = f"Alto: {datos['alto_muro']}m × Largo: {datos['largo_muro']}m = *{m2:.1f} m²*"
        elif tipo_key == "muro durock":
            materiales = _calc_muro_durock(datos["alto_muro"], datos["largo_muro"])
            m2 = datos["alto_muro"] * datos["largo_muro"]
            dim_txt = f"Alto: {datos['alto_muro']}m × Largo: {datos['largo_muro']}m = *{m2:.1f} m²*"
        elif tipo_key == "plafon tablaroca":
            materiales = _calc_plafon_tablaroca(datos["largo"], datos["ancho"])
            m2 = datos["largo"] * datos["ancho"]
            dim_txt = f"Largo: {datos['largo']}m × Ancho: {datos['ancho']}m = *{m2:.1f} m²*"
        elif tipo_key == "plafon reticulado":
            materiales = _calc_plafon_reticulado(datos["largo"], datos["ancho"])
            m2 = datos["largo"] * datos["ancho"]
            dim_txt = f"Largo: {datos['largo']}m × Ancho: {datos['ancho']}m = *{m2:.1f} m²*"
        else:
            raise ValueError(f"tipo desconocido: {tipo_key}")
    except Exception as e:
        print("CALC ERROR:", repr(e))
        state.pop("construccion_state", None)
        upsert_quote_state(company_id, wa_from, state)
        return "Error calculando materiales. Escribe *construccion* para intentar de nuevo."

    label = tipo_cfg["label"]
    lines = [
        f"🏗️ *{label}*",
        dim_txt,
        "",
        "📦 *Materiales necesarios:*",
    ]

    total_estimado = 0.0
    materiales_con_precio = []
    nombres_exactos = CONSTRUCCION_PRODUCTOS.get(tipo_key, [])
    nombre_map = {norm_name(n): n for n in nombres_exactos}

    conn_precio = get_conn()
    try:
        for nombre, cantidad in materiales:
            nombre_n = norm_name(nombre)
            nombre_catalogo = nombre_map.get(nombre_n)
            if not nombre_catalogo:
                for key in nombre_map:
                    if key in nombre_n or nombre_n in key:
                        nombre_catalogo = nombre_map[key]
                        break
            item = _buscar_precio_exacto(conn_precio, company_id, nombre_catalogo) if nombre_catalogo else None
            if item and item.get("price"):
                precio = float(item["price"])
                subtotal = precio * cantidad
                total_estimado += subtotal
                unit = item.get("unit") or "pza"
                lines.append(f"• {cantidad} × {item['name']} — ${precio:,.2f}/{unit} = *${subtotal:,.0f}*")
                materiales_con_precio.append((item["name"], cantidad))
            else:
                lines.append(f"• {cantidad} × {nombre} — (sin precio en catálogo)")
                materiales_con_precio.append((nombre, cantidad))
    finally:
        conn_precio.close()

    if total_estimado > 0:
        lines.append("")
        total_final = total_estimado
        threshold, percent = _get_company_discount(company_id)
        if threshold and percent and total_estimado >= threshold:
            descuento = round(total_estimado * percent / 100)
            total_final = total_estimado - descuento
            lines.append(f"🏷️ Descuento {percent:.0f}% por volumen: -*${descuento:,.0f}*")
        lines.append(f"💰 *Total estimado: ${total_final:,.0f}* (IVA incluido)")

    lines.append("")
    lines.append("¿Quieres cotizar estos materiales?\nEscribe *si* para agregar al carrito, o *pagar* para datos de pago directo.")

    cs["resultado"] = materiales_con_precio
    cs["step"] = "esperando_cotizar"
    state["construccion_state"] = cs
    upsert_quote_state(company_id, wa_from, state)
    return "\n".join(lines)
                                     

   
def _build_cart_context(st: dict) -> str:
    cart = (st or {}).get("cart") or []
    if not cart:
        return ""
    return ", ".join(it["name"] for it in cart if it.get("name"))


def build_reply_for_company(company_id: str, user_text: str, wa_from: str = "", is_interactive: bool = False) -> str:
    if wa_from:
        _bot_state = get_quote_state(company_id, wa_from) or {}
        if _bot_state.get("bot_active") is False:
            return ""

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
        return [(m[0], int(m[1])) for m in re.findall(r"([A-Z])(\d+)", t)]

    def _is_greeting_like(tnorm: str) -> bool:
        t = (tnorm or "").strip()
        if not t:
            return False
        if t in {"hola", "buenas", "hey", "holi", "menu", "menú", "ayuda", "inicio",
                 "buen dia", "buen día", "buenos dias", "buenos días",
                 "buenas tardes", "buenas noches"}:
            return True
        if t.startswith("hola"):
            return True
        if t.startswith("buenos") or t.startswith("buenas") or t.startswith("buen"):
            return True
        return False

    def _build_reply_with_pending(state: dict, company_id: str = "", wa_from: str = ""):
        pending = state.get("pending") or []

        if pending:
            letters = "ABCDEFGHIJKLMNOP"
            cart = state.get("cart") or []
            cart_count = len(cart)
            pending_count = len(pending)

            con_opciones = [(i, p) for i, p in enumerate(pending) if p.get("candidates")]
            sin_opciones = [(i, p) for i, p in enumerate(pending) if not p.get("candidates")]

            section_rows = []
            for idx, (i, p) in enumerate(con_opciones[:10]):
                tag = letters[idx]
                qty = int(p.get("qty") or 0)
                raw = (p.get("raw") or "").strip()
                cands = p.get("candidates") or []
                for j, it in enumerate(cands[:5], start=1):
                    price = float(it.get("price") or 0.0)
                    section_rows.append({
                        "id": f"pick_{tag}{j}",
                        "title": f"{tag}{j}) {it['name']}"[:24],
                        "description": f"${price:,.0f} / {it.get('unit') or 'unidad'}",
                    })

            if wa_from and company_id:
                upsert_quote_state(company_id, wa_from, state)

            if section_rows:
                resumen_lines = []
                if cart_count > 0:
                    resumen_lines.append(f"✅ Cotizamos *{cart_count} producto(s)* automáticamente.")
                resumen_lines.append(f"❓ Necesito confirmar *{pending_count}* — elige una opción por letra:")
                resumen_txt = "\n".join(resumen_lines)

                list_body_lines = []
                for idx, (i, p) in enumerate(con_opciones[:10]):
                    tag = letters[idx]
                    qty = int(p.get("qty") or 0)
                    raw = (p.get("raw") or "").strip()
                    list_body_lines.append(f"*{tag})* {qty}x {raw}")
                for i, p in sin_opciones:
                    qty = int(p.get("qty") or 0)
                    raw = (p.get("raw") or "").strip()
                    list_body_lines.append(f"❌ {qty}x {raw} — no encontrado")
                list_body = "\n".join(list_body_lines) or "Elige una opción:"

                return {
                    "type": "text_then_list_sections",
                    "text": resumen_txt,
                    "body": list_body[:1024],
                    "sections": [{"title": "Elige por letra", "rows": section_rows[:10]}],
                    "button_label": "Ver opciones",
                }
            else:
                lines = []
                if cart_count > 0:
                    lines.append(f"✅ Cotizamos *{cart_count} producto(s)* automáticamente.\n")
                for i, p in sin_opciones:
                    qty = int(p.get("qty") or 0)
                    raw = (p.get("raw") or "").strip()
                    lines.append(f"❌ *{qty}x {raw}* — no encontrado, escríbelo diferente")
                return "\n".join(lines)

        msg = cart_render_quote(state, company_id=company_id, client_phone=wa_from) if (state.get("cart") or []) else ""

        if wa_from and company_id:
            upsert_quote_state(company_id, wa_from, state)

        msg += (
            "\n\n¿Agregamos algo más?\n"
            "🧭 Comandos:\n"
            "• 'quitar [producto]' → ej: quitar cemento\n"
            "• 'cambiar [cantidad] [producto]' → ej: cambiar 10 varilla\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )
        return msg

    tnorm = norm_name(user_text).replace("cotización", "cotizacion")

    _edit_state = get_quote_state(company_id, wa_from) if wa_from else {}
    _edit_state = _edit_state or {}
    _cart = _edit_state.get("cart") or []

    ver_triggers = {"ver carrito", "mi carrito", "que llevo", "qué llevo", "ver pedido", "mi pedido"}
    if tnorm in ver_triggers:
        if not _cart:
            return "Tu carrito está vacío. Mándame tu pedido, ej: 10 cemento, 5 varilla 3/8"
        return cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from)

    _quitar_match = re.match(r"^(quitar|eliminar|borrar|sacar)\s+(.+)$", tnorm)
    if _quitar_match:
        _prod_query = _quitar_match.group(2).strip()
        if not _cart:
            return "Tu carrito está vacío."
        _matches = [it for it in _cart if _prod_query in norm_name(it.get("name", "")).lower()]
        if not _matches:
            _matches = [it for it in _cart if any(tok in norm_name(it.get("name", "")).lower() for tok in _prod_query.split() if len(tok) >= 3)]
        if len(_matches) == 1:
            _cart = [it for it in _cart if it != _matches[0]]
            _edit_state["cart"] = _cart
            if wa_from:
                upsert_quote_state(company_id, wa_from, _edit_state)
            if not _cart:
                return f"✅ Eliminé *{_matches[0]['name']}*. Tu carrito quedó vacío."
            return cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from) + "\n\n¿Agregamos o quitamos algo más?"
        elif len(_matches) > 1:
            lines = "\n".join([f"• {it['name']}" for it in _matches])
            return f"Encontré varios con '{_prod_query}':\n{lines}\n\nEscribe el nombre más completo."
        else:
            return f"No encontré '{_prod_query}' en tu carrito.\n\nEscribe *ver carrito* para ver lo que llevas."

    _cambiar_match = re.match(r"^(cambiar|cambia|modificar|modifica|actualizar)\s+(\d+)\s+(.+)$", tnorm)
    if _cambiar_match:
        _nueva_qty = int(_cambiar_match.group(2))
        _prod_query = _cambiar_match.group(3).strip()
        if not _cart:
            return "Tu carrito está vacío."
        _matches = [it for it in _cart if _prod_query in norm_name(it.get("name", "")).lower()]
        if not _matches:
            _matches = [it for it in _cart if any(tok in norm_name(it.get("name", "")).lower() for tok in _prod_query.split() if len(tok) >= 3)]
        if len(_matches) == 1:
            for it in _cart:
                if it == _matches[0]:
                    it["qty"] = _nueva_qty
            _edit_state["cart"] = _cart
            if wa_from:
                upsert_quote_state(company_id, wa_from, _edit_state)
            return cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from) + "\n\n¿Agregamos o quitamos algo más?"
        elif len(_matches) > 1:
            lines = "\n".join([f"• {it['name']}" for it in _matches])
            return f"Encontré varios con '{_prod_query}':\n{lines}\n\nEscribe el nombre más completo."
        else:
            return f"No encontré '{_prod_query}' en tu carrito.\n\nEscribe *ver carrito* para ver lo que llevas."

    pagar_triggers = {"pagar", "pago", "como pago", "cómo pago", "quiero pagar", "datos de pago", "datos bancarios", "transferencia"}
    if any(pt == tnorm or pt in tnorm for pt in pagar_triggers):
        _plan = get_company_plan_code(company_id)
        if _plan not in ("cotizabot", "pro", "enterprise", "owner"):
            return (
                "Para procesar tu pago, contáctanos directamente:\n\n"
                "📞 Llama o escribe *asesor* y un representante te atenderá. 🙏"
            )
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
            return "Para recibir los datos de pago, escribe *asesor* y un representante te los enviará. 🙏"

    reset_triggers = {
        "salir", "cancelar", "cancel", "reset", "reiniciar",
        "nueva cotizacion", "nuevo", "empezar de nuevo",
        "borrar", "borrar carrito", "vaciar carrito",
        "limpiar", "limpiar carrito",
    }
    if any(rt == tnorm or rt in tnorm for rt in reset_triggers):
        if wa_from:
            clear_quote_state(company_id, wa_from)
        try:
            conn_co = get_conn()
            cur_co = conn_co.cursor()
            cur_co.execute("SELECT name FROM companies WHERE id=%s LIMIT 1", (company_id,))
            row_co = cur_co.fetchone()
            cur_co.close()
            conn_co.close()
            company_name = row_co[0] if row_co else "tu ferretería"
        except Exception:
            company_name = "tu ferretería"
        return (
            f"👋 ¡Hola! Soy el Cotizabot de *{company_name}*\n\n"
            "¿En qué te puedo ayudar?\n"
            "🔨 Cotizar materiales → mándame tu pedido\n"
            "🏗️ Calcular materiales m2 de muros o plafones → escribe *Calcula*\n"
            "🕐 Horarios y ubicación → escribe *horario* o *ubicación*\n"
            "👤 Hablar con alguien → escribe *asesor*\n\n"
            "Ejemplo de cotización:\n"
            "👉 10 cemento, 5 varilla 3/8"
        )

    thanks_triggers = {"gracias", "muchas gracias", "mil gracias", "thx", "thanks"}
    if tnorm in thanks_triggers:
        return (
            "¡Con gusto! 🙌\n"
            "Si quieres otra cotización, mándame: 10 cemento, 5 varilla 3/8\n\n"
            "🧭 Comandos:\n"
            "• 'quitar [producto]' → ej: quitar cemento\n"
            "• 'cambiar [cantidad] [producto]' → ej: cambiar 10 varilla\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

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

    if _is_greeting_like(tnorm):
        try:
            conn_co = get_conn()
            cur_co = conn_co.cursor()
            cur_co.execute("SELECT name FROM companies WHERE id=%s LIMIT 1", (company_id,))
            row_co = cur_co.fetchone()
            cur_co.close()
            conn_co.close()
            company_name = row_co[0] if row_co else "tu ferretería"
        except Exception:
            company_name = "tu ferretería"

        if wa_from:
            st = get_quote_state(company_id, wa_from) or {}
            if (st.get("cart") or []) or (st.get("pending") or []):
                clear_quote_state(company_id, wa_from)
                return (
                    f"👋 ¡Hola! Soy el Cotizabot de *{company_name}*\n\n"
                    "¿En qué te puedo ayudar?\n"
                    "🔨 Cotizar materiales → mándame tu pedido\n"
                    "🏗️ Calcular m2 de muros o plafones → escribe *calcula*\n"
                    "🕐 Horarios y ubicación → escribe *horario* o *ubicación*\n"
                    "👤 Hablar con alguien → escribe *asesor*\n\n"
                    "Ejemplo de cotización:\n"
                    "👉 10 cemento, 5 varilla 3/8"
                )
        return (
            f"👋 ¡Hola! Soy el Cotizabot de *{company_name}*\n\n"
            "¿En qué te puedo ayudar?\n"
            "🔨 Cotizar materiales → mándame tu pedido\n"
            "🏗️ Calcular m2 de muros o plafones → escribe *calcula*\n"
            "🕐 Horarios y ubicación → escribe *horario* o *ubicación*\n"
            "👤 Hablar con alguien → escribe *asesor*\n\n"
            "Ejemplo de cotización:\n"
            "👉 10 cemento, 5 varilla 3/8, 2 martillos"
        )

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
                    result = smart_search(conn, company_id, full_query, current["qty"],
                                          cart_context=_build_cart_context(_state_specs))
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

                return _build_reply_with_pending(_state_specs, company_id=company_id, wa_from=wa_from)

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

    # ── Construcción Ligera ───────────────────────────────────────────────────
    try:
        conn_cl = get_conn()
        cur_cl = conn_cl.cursor()
        cur_cl.execute("SELECT construccion_ligera_enabled FROM companies WHERE id=%s", (company_id,))
        row_cl = cur_cl.fetchone()
        cur_cl.close()
        conn_cl.close()
        _cl_enabled = bool(row_cl[0]) if row_cl else False
    except Exception:
        _cl_enabled = False

    if _cl_enabled:
        _cs_state = get_quote_state(company_id, wa_from) if wa_from else {}
        _cs_state = _cs_state or {}

        if _cs_state.get("construccion_state"):
            cs = _cs_state["construccion_state"]
            if cs.get("step") == "esperando_cotizar":
                if tnorm in {"si", "sí", "yes", "s", "dale", "va", "ok", "listo", "cotiza", "cotizar"}:
                    materiales = cs.get("resultado") or []
                    state = _cs_state
                    conn = get_conn()
                    try:
                        for nombre, cantidad in materiales:
                            result = smart_search(conn, company_id, nombre, cantidad,
                                                  cart_context=_build_cart_context(state))
                            if result["status"] == "found":
                                state = cart_add_item(state, {
                                    "sku": result["item"].get("sku"),
                                    "name": result["item"].get("name"),
                                    "unit": result["item"].get("unit") or "unidad",
                                    "price": float(result["item"].get("price") or 0.0),
                                    "vat_rate": result["item"].get("vat_rate"),
                                    "qty": cantidad,
                                })
                    finally:
                        conn.close()
                    state.pop("construccion_state", None)
                    upsert_quote_state(company_id, wa_from, state)
                    return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
                else:
                    _cs_state.pop("construccion_state", None)
                    upsert_quote_state(company_id, wa_from, _cs_state)
                    return "Entendido, cancelado. ¿En qué más te ayudo?"
            return _handle_construccion(company_id, user_text, wa_from)

        if _is_construccion_trigger(tnorm):
            return _handle_construccion(company_id, user_text, wa_from)

    # ── Picks múltiples A1 B2 C3 ─────────────────────────────────────────────
    _quick_picks = _parse_pending_picks(user_text)
    _state_picks = get_quote_state(company_id, wa_from) if wa_from else {}
    _state_picks = _state_picks or {}

    if _quick_picks and _state_picks.get("pending"):
        state = _state_picks
        pend = state.get("pending") or []
        letters = "ABCDEFGHIJKLMNOP"

        letter_to_idx = {letters[i]: i for i in range(len(pend))}
        picks_map = {}
        for letter, opt in _quick_picks:
            if letter in letter_to_idx:
                picks_map[letter] = opt

        still_pending = []
        for i, p in enumerate(pend):
            tag = letters[i] if i < len(letters) else None
            if tag and tag in picks_map:
                opt = picks_map[tag]
                cands = p.get("candidates") or []
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
                else:
                    still_pending.append(p)
            else:
                still_pending.append(p)

        if still_pending:
            state["pending"] = still_pending
        else:
            state.pop("pending", None)

        if wa_from:
            upsert_quote_state(company_id, wa_from, state)

        return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

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
            _pedido_raw = ", ".join(p for _, p in multi if p.strip() != "???")
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
                    result = smart_search(conn, company_id, prod_raw, qty,
                                          cart_context=_pedido_raw)  
                except Exception as e:
                    print("SMART SEARCH ERROR:", repr(e))
                    result = {"status": "not_found", "item": None, "candidates": []}
                    save_search_miss(company_id, prod_raw)
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
                    prefix = cart_render_quote(state, company_id=company_id, client_phone=wa_from) + "\n\n"
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
            return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
        finally:
            conn.close()

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
            _single_state = get_quote_state(company_id, wa_from) if wa_from else {}
            _single_state = _single_state or {}
            try:
                result = smart_search(conn, company_id, prod_query, qty,
                                      cart_context=_build_cart_context(_single_state))
            except Exception as e:
                print("SMART SEARCH ERROR:", repr(e))
                result = {"status": "not_found", "item": None, "candidates": []}
                save_search_miss(company_id, prod_query)


            if result["status"] == "found":
                state = _single_state
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
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

            elif result["status"] == "ambiguous":
                pending = [{"qty": qty, "raw": prod_query, "candidates": result["candidates"]}]
                state = _single_state
                state["pending"] = pending
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

        finally:
            conn.close()

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

    state = get_quote_state(company_id, wa_from) if wa_from else None
    if state and state.get("pending"):
        pend = state.get("pending") or []

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
                        result = smart_search(conn, company_id, prod_raw, qty,
                                              cart_context=_build_cart_context(state))
                    except Exception as e:
                        print("SMART SEARCH ERROR:", repr(e))
                        result = {"status": "not_found", "item": None, "candidates": []}
                        save_search_miss(company_id, prod_raw)
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
                        still.append({
                            "qty": qty, "raw": prod_raw,
                            "candidates": result["candidates"],
                        })
                if still:
                    state["pending"] = still
                else:
                    state.pop("pending", None)
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
            finally:
                conn.close()

        if pend:
            return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

    if re.search(r"\b\d+\b", user_text):
        qty, prod_query = extract_qty_and_product(user_text)
        if qty and prod_query:
            _fallback_state = get_quote_state(company_id, wa_from) if wa_from else {}
            _fallback_state = _fallback_state or {}
            conn = get_conn()
            try:
                result = smart_search(conn, company_id, prod_query, qty,
                                      cart_context=_build_cart_context(_fallback_state))
            finally:
                conn.close()

            if result["status"] == "found":
                state = _fallback_state
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
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

            elif result["status"] == "ambiguous":
                state = _fallback_state
                state["pending"] = [{"qty": qty, "raw": prod_query, "candidates": result["candidates"]}]
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

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

    if looks_like_product_phrase(user_text) and not re.search(r"\b\d+\b", user_text):
        productos_detectados = []
        for sep in [" y ", " e ", ",", "/"]:
            if sep in user_text.lower():
                partes = [p.strip() for p in user_text.lower().split(sep) if p.strip()]
                if len(partes) > 1:
                    productos_detectados = partes
                    break

        if productos_detectados:
            ejemplos = ", ".join([f"10 {p}" for p in productos_detectados[:3]])
            return (
                f"Vi que mencionas varios productos. ¿Cuántas piezas de cada uno necesitas?\n\n"
                f"Mándamelo así:\n"
                f"👉 {ejemplos}\n\n"
                "🧭 Comandos:\n"
                "• 'nueva cotizacion' → empezar de cero\n"
                "• 'salir' → cancelar"
            )

        return (
            "¿Cuántas piezas necesitas?\n"
            "Ejemplo: '10 sacos cemento' o '5 varilla 3/8'\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

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
                    result = smart_search(conn, company_id, prod, qty,
                                          cart_context=_build_cart_context(state))
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
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
            finally:
                conn.close()

    except Exception as e:
        print("GPT FALLBACK ERROR:", repr(e))

    return "¿Me repites eso? No entendí bien tu pedido 🤔"


@app.post("/api/admin/rebuild-synonyms-public")
def rebuild_synonyms_public(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, name, synonyms FROM pricebook_items WHERE company_id=%s", (company_id,))
        rows = cur.fetchall()
        updated = 0
        for item_id, name, synonyms in rows:
            existing = (synonyms or "").strip()
            auto_vars = _auto_plural_singular(name)
            if auto_vars:
                existing_set = {s.strip().lower() for s in existing.split(",") if s.strip()}
                new_vars = [v for v in auto_vars if v not in existing_set]
                if new_vars:
                    new_synonyms = (existing + ", " + ", ".join(new_vars)).strip(", ")
                    cur.execute(
                        "UPDATE pricebook_items SET synonyms=%s, updated_at=now() WHERE id=%s",
                        (new_synonyms, item_id)
                    )
                    updated += 1
        return {"ok": True, "total": len(rows), "updated": updated}
    finally:
        cur.close()
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
    email: Optional[str] = None
    rfc: Optional[str] = None
    brand_color: Optional[str] = None
    discount_threshold: Optional[float] = None
    discount_percent: Optional[float] = None
    @validator('discount_threshold', 'discount_percent', pre=True)
    def coerce_empty_to_none(cls, v):
        if v == '' or v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
        

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

    email       = (body.email or "").strip() or None
    rfc         = (body.rfc or "").strip().upper() or None
    brand_color = (body.brand_color or "").strip() or None
    discount_threshold = float(body.discount_threshold) if body.discount_threshold is not None and body.discount_threshold > 0 else None
    discount_percent   = float(body.discount_percent)   if body.discount_percent   is not None and 0 < body.discount_percent <= 100 else None

    
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
                owner_phone=%s, email=%s, rfc=%s, brand_color=%s,
                discount_threshold=%s, discount_percent=%s, updated_at=now()
            WHERE id=%s
            RETURNING id
            """,
            (hours, addr, maps, mp_url, bank_name, bank_acc_name, bank_clabe, bank_acc_num,
             owner_phone, email, rfc, brand_color, discount_threshold, discount_percent, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        conn.commit()
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
                   owner_phone, email, rfc, brand_color, logo_url,
                   discount_threshold, discount_percent
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
                "email": row[9], "rfc": row[10], "brand_color": row[11], "logo_url": row[12],
                "discount_threshold": float(row[13]) if row[13] else None,
                "discount_percent":   float(row[14]) if row[14] else None,
            },
        }
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.post("/api/company/logo")
async def upload_company_logo(
    request: Request,
    file: UploadFile = File(...),
):
    company_id = require_company_id(request)

    content = await file.read()
    if len(content) > 2 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Imagen demasiado grande (máx 2 MB)")

    ext = (file.filename or "logo.png").rsplit(".", 1)[-1].lower()
    if ext not in ("png", "jpg", "jpeg", "webp"):
        raise HTTPException(status_code=400, detail="Formato no soportado (usa PNG, JPG o WEBP)")

    import base64
    mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
    b64  = base64.b64encode(content).decode()
    data_url = f"data:{mime};base64,{b64}"

    conn = None
    cur  = None
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
        return {"ok": True, "logo_url": data_url}
    finally:
        if cur:  cur.close()
        if conn: conn.close()


@app.delete("/api/company/logo")
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


# ══════════════════════════════════════════════════════════════════════════════
# STRIPE
# ══════════════════════════════════════════════════════════════════════════════

import stripe as _stripe

_STRIPE_SECRET_KEY     = (os.getenv("STRIPE_SECRET_KEY") or "").strip()
_STRIPE_WEBHOOK_SECRET = (os.getenv("STRIPE_WEBHOOK_SECRET") or "").strip()

_STRIPE_PRICES = {
    "cotizabot": "price_1TCSlDF3nSPXsrl4Q05iH98d",
    "pro":        "price_1TCSlcF3nSPXsrl4mDPdBvN3",
    "enterprise": "price_1TCSlvF3nSPXsrl41CLP8xv7",
}

_PRICE_TO_PLAN = {v: k for k, v in _STRIPE_PRICES.items()}


class CheckoutBody(BaseModel):
    plan: str
    success_url: str
    cancel_url: str


@app.post("/api/pagos/crear-checkout")
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
        )
        return {"ok": True, "checkout_url": session.url, "session_id": session.id}
    except Exception as e:
        print("STRIPE CHECKOUT ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"Error creando checkout: {str(e)}")


@app.get("/api/pagos/estado")
def pago_estado(request: Request, session_id: str = Query(...)):
    if not _STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="STRIPE_SECRET_KEY no configurada")

    _stripe.api_key = _STRIPE_SECRET_KEY
    try:
        session = _stripe.checkout.Session.retrieve(session_id)
        paid = session.payment_status == "paid"
        plan = session.metadata.get("plan") if session.metadata else None
        return {"ok": True, "paid": paid, "plan": plan, "status": session.payment_status}
    except Exception as e:
        print("STRIPE ESTADO ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pagos/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not _STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="STRIPE_WEBHOOK_SECRET no configurada")

    _stripe.api_key = _STRIPE_SECRET_KEY
    try:
        event = _stripe.Webhook.construct_event(payload, sig_header, _STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        print("STRIPE WEBHOOK SIGNATURE ERROR:", repr(e))
        raise HTTPException(status_code=400, detail="Invalid signature")

    event_type = event.get("type")
    print(f"STRIPE EVENT: {event_type}")

    if event_type == "checkout.session.completed":
        session = event["data"]["object"]
        company_id = (session.get("metadata") or {}).get("company_id")
        plan       = (session.get("metadata") or {}).get("plan")
        stripe_customer_id = session.get("customer")

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
                cur.close()
                conn.close()
                print(f"STRIPE PLAN ACTIVADO: company={company_id} plan={plan}")
            except Exception as e:
                print("STRIPE WEBHOOK DB ERROR:", repr(e))

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
                cur.close()
                conn.close()
                print(f"STRIPE SUSCRIPCION CANCELADA: customer={stripe_customer_id}")
            except Exception as e:
                print("STRIPE CANCEL DB ERROR:", repr(e))

    return {"ok": True}

# ── Quotes ────────────────────────────────────────────────────────────────────

@app.get("/api/quotes")
def list_quotes(
    request: Request,
    authorization: str = Header(default=""),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    if authorization and authorization.lower().startswith("bearer "):
        company_id = get_company_from_bearer(authorization)["company_id"]
    else:
        company_id = require_company_id(request)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, folio, client_phone, total, created_at
            FROM quotes
            WHERE company_id = %s::uuid
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (company_id, limit, offset),
        )
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM quotes WHERE company_id = %s::uuid", (company_id,))
        total_count = cur.fetchone()[0]
        quotes = [
            {
                "id":           str(r[0]),
                "folio":        r[1],
                "client_phone": r[2],
                "total":        float(r[3]),
                "created_at":   r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ]
        return {"ok": True, "quotes": quotes, "total": total_count}
    finally:
        cur.close()
        conn.close()

@app.get("/api/quotes/{folio}/pdf")
def download_quote_pdf(
    request: Request,
    folio: str,
    authorization: str = Header(default=""),
):
    if authorization and authorization.lower().startswith("bearer "):
        company_id = get_company_from_bearer(authorization)["company_id"]
    else:
        company_id = require_company_id(request)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT q.folio, q.client_phone, q.items, q.total, q.created_at,
                   c.name, c.address_text, c.rfc,
                   c.owner_phone, c.email, c.logo_url, c.brand_color
            FROM quotes q
            JOIN companies c ON c.id = q.company_id
            WHERE q.company_id = %s::uuid AND q.folio = %s
            LIMIT 1
            """,
            (company_id, folio.upper()),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Cotización no encontrada")
        (
            q_folio, client_phone, items_json, total,
            created_at, company_name, address, rfc,
            owner_phone, company_email, logo_url, brand_color
        ) = row
        items = items_json if isinstance(items_json, list) else json.loads(items_json or "[]")
        company_dict = {
            "name":        company_name or "CotizaExpress",
            "address":     address or "",
            "rfc":         rfc or "",
            "phone":       owner_phone or "",
            "email":       company_email or "",
            "logo_url":    logo_url or "",
            "brand_color": brand_color or "",
        }
    finally:
        cur.close()
        conn.close()
    pdf_bytes = build_quote_pdf(
        company=company_dict,
        items=items,
        client_phone=client_phone or "",
        folio=q_folio,
    )
    filename = f"cotizacion_{q_folio}.pdf"
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.get("/api/company/me")
def company_me(request: Request):
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id::text, name, slug, twilio_phone, plan_code FROM companies WHERE id=%s LIMIT 1", (company_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        return {"ok": True, "company": {"id": row[0], "name": row[1], "slug": row[2], "twilio_phone": row[3], "plan_code": row[4]}}
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.post("/api/pricebook/rebuild-synonyms")
def rebuild_synonyms(request: Request):
    company_id = require_company_id(request)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, synonyms FROM pricebook_items WHERE company_id=%s",
            (company_id,)
        )
        rows = cur.fetchall()
        updated = 0
        for item_id, name, synonyms in rows:
            existing = (synonyms or "").strip()
            auto_vars = _auto_plural_singular(name)
            if auto_vars:
                existing_set = {s.strip().lower() for s in existing.split(",") if s.strip()}
                new_vars = [v for v in auto_vars if v not in existing_set]
                if new_vars:
                    new_synonyms = (existing + ", " + ", ".join(new_vars)).strip(", ")
                    cur.execute(
                        "UPDATE pricebook_items SET synonyms=%s, updated_at=now() WHERE id=%s",
                        (new_synonyms, item_id)
                    )
                    updated += 1
        return {"ok": True, "company_id": company_id, "total": len(rows), "updated": updated}
    finally:
        cur.close()
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

        slug = re.sub(r'[^a-z0-9]+', '-', email.split("@")[0].lower()).strip('-')
        cur.execute("INSERT INTO companies (name, slug) VALUES (%s, %s) RETURNING id", (email, slug or None))
        company_id = cur.fetchone()[0]

        token = generate_api_key()
        cur.execute(
            "INSERT INTO api_keys (company_id, name, prefix, key_hash) VALUES (%s, %s, %s, %s)",
            (company_id, "default", api_key_prefix(token), api_key_hash(token))
        )

        cur.execute(
            "INSERT INTO users (email, password_hash, company_id) VALUES (%s, %s, %s) RETURNING id",
            (email, password_hash, company_id)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="No se pudo obtener user_id")
        user_id = row[0]
        return {"ok": True, "user_id": user_id, "company_id": str(company_id), "api_key": token}
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
    request: Request,
    authorization: str = Header(default=""),
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
):
    if authorization and authorization.lower().startswith("bearer "):
        tenant = get_company_from_bearer(authorization)
        company_id = tenant["company_id"]
    else:
        company_id = require_company_id(request)

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
            name = normalize_display_name(str(r[idx["name"]])) if r[idx["name"]] is not None else ""
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

        synonyms_map = {}
        names_list = [r["name"] for r in parsed_rows]
        for i in range(0, len(names_list), 20):
            batch = names_list[i:i+20]
            synonyms_map.update(_batch_synonyms(batch))

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
    authorization: str = Header(default=""),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=1000),
):
    conn = None
    cur = None
    try:
        if authorization and authorization.lower().startswith("bearer "):
            company_id = get_company_from_bearer(authorization)["company_id"]
        else:
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

def _auto_plural_singular(name: str) -> list:
    """Genera variantes plural/singular para los tokens del nombre."""
    extras = set()
    tokens = norm_name(name).split()
    for tok in tokens:
        if len(tok) < 4 or re.search(r"\d", tok):
            continue
        if tok[-1] not in "aeiouáéíóús":
            extras.add(tok + "es")
        elif tok[-1] in "aeiouáéíóú":
            extras.add(tok + "s")
        elif tok.endswith("es") and len(tok) > 4 and tok[-3] not in "aeiouáéíóú":
            extras.add(tok[:-2])
        elif tok.endswith("s") and len(tok) > 3:
            extras.add(tok[:-1])
    return list(extras)
    
@app.post("/api/pricebook/items")
def pricebook_item_create(request: Request, body: PricebookItemCreateBody):
    _ = get_user_from_session(request)
    company_id = require_company_id(request)
    if not company_id:
        raise HTTPException(status_code=500, detail="DEFAULT_COMPANY_ID missing en Render")

    name = normalize_display_name(body.name or "")
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

    # Auto-generar plurales/singulares como sinónimos
    synonyms = (body.synonyms or "").strip()
    _auto_vars = _auto_plural_singular(name)
    if _auto_vars:
        existing_set = {s.strip().lower() for s in synonyms.split(",") if s.strip()}
        new_vars = [v for v in _auto_vars if v not in existing_set]
        if new_vars:
            synonyms = (synonyms + ", " + ", ".join(new_vars)).strip(", ")

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pricebook_items (company_id, sku, name, name_norm, unit, price, vat_rate, synonyms, source, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (company_id, name_norm)
            DO UPDATE SET sku=EXCLUDED.sku, name=EXCLUDED.name, unit=EXCLUDED.unit,
                price=EXCLUDED.price, vat_rate=EXCLUDED.vat_rate,
                synonyms=COALESCE(NULLIF(pricebook_items.synonyms,''), EXCLUDED.synonyms),
                source=EXCLUDED.source, updated_at=now()
            RETURNING id
            """,
            (company_id, sku, name, name_norm, unit, price, vat_rate, synonyms, source),
        )
        new_id = cur.fetchone()[0]
        try:
            upsert_single_embedding(conn, company_id, new_id, name, sku or "", unit or "", synonyms or "")
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


# ── PATCH actualizado: usa PricebookItemUpdateBody (todos los campos opcionales) ──

@app.patch("/api/pricebook/items/{item_id}")
def pricebook_item_update(request: Request, item_id: str, body: PricebookItemUpdateBody):
    _ = get_user_from_session(request)
    company_id = require_company_id(request)

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT name, sku, unit, price, vat_rate, synonyms FROM pricebook_items WHERE id=%s AND company_id=%s",
            (item_id, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        name     = (body.name.strip() if body.name is not None else None) or row[0]
        sku      = (body.sku.strip()  if body.sku  is not None else None) or row[1]
        unit     = (body.unit.strip() if body.unit is not None else None) or row[2]
        price    = body.price    if body.price    is not None else row[3]
        vat_rate = body.vat_rate if body.vat_rate is not None else row[4]
        synonyms = (body.synonyms.strip() if body.synonyms is not None else None) or row[5] or ""

        # Auto-generar plurales/singulares como sinónimos
        _auto_vars = _auto_plural_singular(name)
        if _auto_vars:
            existing_set = {s.strip().lower() for s in synonyms.split(",") if s.strip()}
            new_vars = [v for v in _auto_vars if v not in existing_set]
            if new_vars:
                synonyms = (synonyms + ", " + ", ".join(new_vars)).strip(", ")

        name_norm = norm_name(name)

        cur.execute(
            """
            UPDATE pricebook_items
            SET name=%s, name_norm=%s, sku=%s, unit=%s, price=%s, vat_rate=%s, synonyms=%s, updated_at=now()
            WHERE id=%s AND company_id=%s
            RETURNING id
            """,
            (name, name_norm, sku, unit, price, vat_rate, synonyms, item_id, company_id),
        )
        if not cur.fetchone():
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
    t = re.sub(r"(\d+)\s*/\s*(\d+)", r"\1_\2", t)
    t = re.sub(r"\s+y\s+(?=\d)", "\n", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+e\s+(?=\d)", "\n", t, flags=re.IGNORECASE)
    items = []
    lines = [l.strip() for l in re.split(r"[\n\r]+", t) if l.strip()]
    for line in lines:
        parts = [p.strip() for p in line.split(",") if p.strip()]
        for part in parts:
            m = re.match(r"^\s*(\d+)\s+(.+)$", part.strip())
            if m:
                qty = int(m.group(1))
                prod = m.group(2).replace("_", "/").strip()
                prod = re.sub(r"\b(de|hojas|hoja|piezas|pieza|rollos|rollo|bultos|bulto|sacos|saco|atados|atado|paquetes|paquete)\b", "", prod, flags=re.IGNORECASE).strip()
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

        if not reply:
            return TWIML_OK

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

# ── Agent Console: enviar mensaje manual + toggle bot ─────────────────────────

class AgentMessageBody(BaseModel):
    message: str

@app.post("/api/conversations/{client_phone}/mensaje")
def agent_send_message(
    request: Request,
    client_phone: str,
    body: AgentMessageBody,
    authorization: str = Header(default=""),
):
    if authorization and authorization.lower().startswith("bearer "):
        company_id = get_company_from_bearer(authorization)["company_id"]
    else:
        company_id = require_company_id(request)

    text = (body.message or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="message requerido")

    # Detectar si el tenant usa Meta API o Twilio
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT c.wa_api_key, c.wa_phone_number_id, ch.provider
            FROM companies c
            LEFT JOIN channels ch ON ch.company_id = c.id AND ch.is_active = true
            WHERE c.id = %s::uuid
            LIMIT 1
            """,
            (company_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Company no encontrada")

    wa_api_key, wa_phone_number_id, provider = row

    # Normalizar teléfono destino
    to_phone = client_phone.replace("whatsapp:", "").strip()

    try:
        if provider == "twilio":
            twilio_send_whatsapp(to_user_whatsapp=to_phone, text=text)
        else:
            send_whatsapp_text(
                wa_api_key=wa_api_key,
                phone_number_id=wa_phone_number_id,
                to=to_phone,
                text=text,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error enviando mensaje: {str(e)}")

    # Loggear como 'agent'
    log_message(company_id, client_phone, "agent", text)

    return {"ok": True}


class BotToggleBody(BaseModel):
    bot_active: bool

@app.put("/api/conversations/{client_phone}/bot")
def toggle_bot(
    request: Request,
    client_phone: str,
    body: BotToggleBody,
    authorization: str = Header(default=""),
):
    if authorization and authorization.lower().startswith("bearer "):
        company_id = get_company_from_bearer(authorization)["company_id"]
    else:
        company_id = require_company_id(request)

    # Guardamos bot_active en wa_quote_state como campo extra
    state = get_quote_state(company_id, client_phone) or {}
    state["bot_active"] = body.bot_active
    upsert_quote_state(company_id, client_phone, state)

    return {"ok": True, "bot_active": body.bot_active}
