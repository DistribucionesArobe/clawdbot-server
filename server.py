from prompts_cotizabot import COTIZABOT_SYSTEM_PROMPT
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

    # Normaliza variantes comunes
    t = t.replace("cotización", "cotizacion")
    t = re.sub(r"[^a-z0-9áéíóúñü\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Frases completas que NO son producto (comandos / conversaciones)
    stop_phrases = {
        "hola",
        "buenas",
        "buenos dias",
        "buenas tardes",
        "buenas noches",
        "gracias",
        "muchas gracias",
        "ok",
        "va",
        "sale",
        "listo",
        "perfecto",
        "dale",
        "ayuda",
        "menu",
        "inicio",
        "salir",
        "cancelar",
        "cancel",
        "reiniciar",
        "reset",
        "nueva cotizacion",
        "nueva cotizacion por favor",
        "nueva cotizacion porfa",
        "empezar de nuevo",
        "borrar carrito",
        "vaciar carrito",
        "limpiar carrito",
    }

    # Si el texto completo es un comando/saludo, NO es producto
    if t in stop_phrases:
        return False

    # Si contiene un comando fuerte como frase dentro del mensaje, NO lo uses como producto
    strong_cmds = {
        "salir",
        "cancelar",
        "cancel",
        "reiniciar",
        "reset",
        "nueva cotizacion",
        "empezar de nuevo",
        "borrar carrito",
        "vaciar carrito",
        "limpiar carrito",
    }
    if any(cmd in t for cmd in strong_cmds):
        return False

    # Tokens útiles
    tokens = [w for w in t.split() if len(w) >= 3]
    if not tokens:
        return False

    # Palabras sueltas típicas de chat (si SOLO trae esto, no es producto)
    blacklist_tokens = {
        "hola", "buenas", "gracias", "ok", "sale",
        "perfecto", "listo", "vale", "va", "dale",
        "porfa", "favor", "por", "si", "no",
        "quiero", "necesito", "dame", "manda", "pasame",
        "cotiza", "cotizar", "cotizacion", "precio", "precios",
        "salir", "cancelar", "cancel", "reiniciar", "reset",
        "nueva", "inicio", "menu", "ayuda",
    }

    # Si TODOS los tokens son "chat/comando", NO es producto
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
# Normalización universal de texto de producto
# (plural->singular conservador + limpieza + abreviaciones)
# -------------------------

SPANISH_STOPWORDS = {
    "de", "del", "la", "el", "un", "una", "unos", "unas",
    "por", "para", "con", "sin", "y", "o", "me", "dime", "oye",
    "precio", "precios", "cuanto", "cuánto", "cuesta", "vale", "costo", "cost",
    "cotiza", "cotizacion", "cotización", "presupuesto", "lista", "saber",
    "quiero", "necesito", "dame", "manda", "pasame", "pásame",
}

# Abreviaciones/sinónimos típicos de chat. Amplíalo con el tiempo.
SYNONYMS = {
    "pste": "poste",
    "psts": "postes",
    "ptr": "poste",
    "tblrc": "tablaroca",
    "tablarok": "tablaroca",
    "durok": "durock",
    "perf": "perfacinta",
    "perfa": "perfacinta",
    # puedes agregar marcas/variantes comunes
}

def singularize_token(tok: str) -> str:
    """
    Singularizador conservador en español:
    - Solo aplica a tokens >= 5 caracteres
    - No toca tokens con números (6.35, 2.44, cal26)
    - Evita romper palabras cortas (usg, cal, kg)
    """
    t = (tok or "").strip()
    if len(t) < 5:
        return t
    if re.search(r"\d", t):  # contiene números -> no tocar
        return t

    # plurales simples:
    # tornillos -> tornillo
    # postes -> poste
    # anclas -> ancla
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
    """
    Normaliza texto para búsqueda:
    - lower
    - reemplaza sinónimos por palabra canónica
    - quita símbolos raros (pero deja '.' para medidas 6.35)
    - tokeniza
    - remueve stopwords
    - singulariza tokens (conservador)
    """
    t = (text or "").lower().strip()

    # Reemplazos por sinónimos (palabra completa)
    for k, v in SYNONYMS.items():
        t = re.sub(rf"\b{re.escape(k)}\b", v, t)

    # Limpieza: conserva números y punto (para 6.35), letras y espacios
    t = re.sub(r"[^a-z0-9áéíóúñü\.\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    out_tokens = []
    for w in t.split():
        if not w:
            continue
        if w in SPANISH_STOPWORDS:
            continue

        # No singularizar números/medidas tipo 6.35
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

    client.messages.create(
        messaging_service_sid=msid,
        to=to_user_whatsapp,
        body=text,
    )

def extract_product_query(text: str) -> str:
    """
    Antes limpiabas manualmente y quitabas palabras.
    Ahora usamos normalización universal (plural/singular + stopwords + sinónimos).
    """
    t = normalize_product_text(text)

    # Si queda vacío, regresa el original limpio mínimo
    if not t:
        raw = (text or "").lower().strip()
        raw = re.sub(r"[^a-z0-9áéíóúñü\.\s]", " ", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw

    return t


def extract_qty_and_product(text: str):
    """
    Extrae qty + producto solo si qty es ENTERO.
    Fix universal: NO interpretar decimales tipo 6.35 como qty=6.
    """
    t = (text or "").strip().lower()

    # qty entero al inicio, pero NO permitir decimales: (?!\.) evita "6.35"
    m = re.match(r"^\s*(\d+)(?!\.)\s+(.+?)\s*$", t)
    if not m:
        return None, None

    qty = int(m.group(1))
    product = m.group(2).strip()
    return qty, product


def is_specs_only(text: str) -> bool:
    """
    Detecta cuando el usuario manda solo especificaciones
    (ej: '6.35 calibre 26') sin nombre claro de producto.
    """
    t = norm_name(text)
    if not t:
        return False

    has_measure = bool(re.search(r"\b\d+(?:\.\d+)?\b", t))
    has_spec_word = any(
        w in t for w in [
            "cal", "calibre", "mm", "cm", "mts", "m",
            "pulg", "pulgada", "x"
        ]
    )

    looks_product = looks_like_product_phrase(t)

    return (has_measure or has_spec_word) and not looks_product

def split_clarifications(text: str):
    t = (text or "").strip()
    # ✅ FIX: separa por líneas primero
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
# API keys (bearer legacy for upload/chat)
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

    # merge por sku si existe, si no por name
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
        cart.append({
            "sku": sku,
            "name": name,
            "unit": unit,
            "price": price,
            "vat_rate": vat_rate,
            "qty": qty,
        })

    state["cart"] = cart
    return state

def extract_specs(text: str):
    t = norm_name(text)
    # medida como 6.35, 4.10, 2.44, etc.
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
    # Si el usuario pidió medida, EXIGE que esté en el nombre del producto
    if medida and medida not in n:
        return False
    # Si pidió cal, también
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
        # score por tokens (mejor que LIKE)
        s1 = fuzz.token_set_ratio(qn, sn)
        s2 = fuzz.token_set_ratio(qn, sku) if sku else 0
        score = max(s1, s2 + 5)  # leve boost a SKU
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
            items.append({
                "sku": sku,
                "name": name,
                "unit": unit,
                "price": float(price) if price is not None else None,
                "vat_rate": float(vat_rate) if vat_rate is not None else None,
            })

        if not items:
            return None

        # 1) Si hay specs, filtra por restricciones duras (evita 6.35->4.10)
        constrained = [it for it in items if passes_constraints(it["name"], specs)]
        pool = constrained if constrained else items

        # 2) Rank real por similitud
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
    # OR para recall alto
    for tok in tokens[:6]:
        where_parts.append("name_norm LIKE %s")
        params.append(f"%{tok}%")
    where_sql = " OR ".join(where_parts)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT sku, name, unit, price, vat_rate
            FROM pricebook_items
            WHERE company_id=%s
              AND ({where_sql} OR sku ILIKE %s OR name ILIKE %s)
            LIMIT 30
            """,
            (*params, f"%{q_clean}%", f"%{q_clean}%"),
        )
        rows = cur.fetchall()

        items = []
        for sku, name, unit, price, vat_rate in rows:
            it = {
                "sku": sku,
                "name": name,
                "unit": unit,
                "price": float(price) if price is not None else None,
                "vat_rate": float(vat_rate) if vat_rate is not None else None,
            }
            sn = norm_name(name or "")
            sku_n = norm_name(sku or "")
            it["_score"] = max(
                fuzz.token_set_ratio(qn, sn),
                fuzz.token_set_ratio(qn, sku_n) if sku else 0
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
    """
    Renderiza pendientes con formato A1, A2... B1...
    """
    if not pending:
        return ""

    letters = string.ascii_uppercase
    lines = ["No pude identificar algunos productos. Elige del catálogo:"]

    for i, p in enumerate(pending[:6]):
        tag = letters[i]
        qty = int(p.get("qty") or 0)
        raw = (p.get("raw") or "").strip()
        cands = p.get("candidates") or []

        lines.append(f"\n❓ ({tag}) {qty} x {raw}")

        if not cands:
            lines.append("   (sin sugerencias) Mándalo más exacto o con SKU.")
            continue

        for j, it in enumerate(cands[:5], start=1):
            unit = it.get("unit") or "unidad"
            price = float(it.get("price") or 0.0)
            sku = (it.get("sku") or "").strip()
            sku_txt = f" (SKU {sku})" if sku else ""
            lines.append(f"   {tag}{j}) {it['name']}{sku_txt} — ${price:,.2f} / {unit}")

    lines.append("\n✅ Responde con: A1, B2, C1 (ejemplo).")
    return "\n".join(lines)


def parse_pending_picks(text: str):
    """
    Parse A1, B2, C3... (no importa si vienen con comas/espacios)
    """
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
        + f"\n\n*Total: ${total:,.2f}* (IVA incluido)"
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
            """
            SELECT state
            FROM wa_quote_state
            WHERE company_id=%s AND wa_from=%s
            LIMIT 1
            """,
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
    return 0  # free/sin WA

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
    """
    Cuenta 1 conversación si la última conversación con ese wa_from fue hace > 24h.
    Devuelve: {counted: bool, usage: int, limit: int, plan_code: str, year_month: str}
    """
    wa_from = (wa_from or "").strip()
    if not wa_from:
        return {"counted": False, "usage": 0, "limit": 0, "plan_code": "free", "year_month": _year_month_utc()}

    plan_code = get_company_plan_code(company_id)
    limit = get_plan_limit(plan_code)
    ym = _year_month_utc()

    # Si no hay WA en el plan, no cuentes (y luego puedes bloquear)
    if limit <= 0:
        return {"counted": False, "usage": get_monthly_usage(company_id, ym), "limit": limit, "plan_code": plan_code, "year_month": ym}

    conn = get_conn()
    cur = conn.cursor()
    try:
        # Lee ventana actual
        cur.execute(
            """
            SELECT last_started_at
            FROM wa_conversation_windows
            WHERE company_id=%s AND wa_from=%s
            LIMIT 1
            """,
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
            # upsert ventana a "ahora"
            cur.execute(
                """
                INSERT INTO wa_conversation_windows(company_id, wa_from, last_started_at, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (company_id, wa_from)
                DO UPDATE SET last_started_at=EXCLUDED.last_started_at, updated_at=now()
                """,
                (company_id, wa_from, now_utc),
            )

        # OJO: increment mensual en conexión separada (o misma) — aquí lo hago directo
        # Si prefieres, puedes hacerlo en esta misma transacción.
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
            SELECT id, wa_api_key, wa_phone_number_id
            FROM companies
            WHERE wa_phone_number_id=%s
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

# -------------------------
# WhatsApp send
# -------------------------
def send_whatsapp_text(wa_api_key: str, phone_number_id: str, to: str, text: str):
    url = f"https://graph.facebook.com/v19.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {wa_api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": text},
    }
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"WhatsApp send failed {r.status_code}: {r.text[:400]}")

# -------------------------
# Sessions
# -------------------------
def create_session(conn, user_id: int) -> str:
    sid = secrets.token_urlsafe(32)
    exp = datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO sessions (id, user_id, expires_at) VALUES (%s, %s, %s)",
            (sid, user_id, exp),
        )
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
            # Esto es importante: sin company_id no hay tenant
            raise HTTPException(status_code=400, detail="User sin company_id asignado")

        return {"id": int(user_id), "email": email, "company_id": company_id}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

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
            SELECT id, company_id
            FROM api_keys
            WHERE prefix = %s
              AND key_hash = %s
              AND revoked_at IS NULL
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
        if cur:
            cur.close()
        if conn:
            conn.close()


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

    # 1) extraer phone_number_id
    try:
        phone_number_id = payload["entry"][0]["changes"][0]["value"]["metadata"]["phone_number_id"]
    except Exception:
        return {"ok": True}

    company = get_company_by_phone_number_id(phone_number_id)
    if not company:
        print("No company mapped for phone_number_id:", phone_number_id)
        return {"ok": True}

    value = payload["entry"][0]["changes"][0]["value"]
    messages = value.get("messages") or []
    if not messages:
        return {"ok": True}

    msg = messages[0]
    from_phone = msg.get("from")
    text = (msg.get("text") or {}).get("body") or ""

    print("WA IN:", {"company_id": company["company_id"], "from": from_phone, "text": text})

    reply = build_reply_for_company(company["company_id"], text, wa_from=from_phone)
    
    send_whatsapp_text(
        wa_api_key=company["wa_api_key"],
        phone_number_id=company["wa_phone_number_id"],
        to=from_phone,
        text=reply,
    )

    return {"ok": True}

@app.get("/api/_version")
def _version():
    return {"version": "pricebook-v2-2026-02-12"}

class CompanySettingsBody(BaseModel):
    hours_text: Optional[str] = None
    address_text: Optional[str] = None
    google_maps_url: Optional[str] = None

@app.post("/api/company/settings")
def company_settings_update(request: Request, body: CompanySettingsBody):
    company_id = require_company_id(request)

    hours = (body.hours_text or "").strip() or None
    addr  = (body.address_text or "").strip() or None
    maps  = (body.google_maps_url or "").strip() or None

    # ✅ cobros
    mp_url = (body.mercadopago_url or "").strip() or None
    bank_name = (body.bank_name or "").strip() or None
    bank_acc_name = (body.bank_account_name or "").strip() or None
    bank_clabe = (body.bank_clabe or "").strip().replace(" ", "") or None
    bank_acc_num = (body.bank_account_number or "").strip().replace(" ", "") or None

    # ✅ validación suave CLABE
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
            SET hours_text=%s,
                address_text=%s,
                google_maps_url=%s,
                mercadopago_url=%s,
                bank_name=%s,
                bank_account_name=%s,
                bank_clabe=%s,
                bank_account_number=%s,
                updated_at=now()
            WHERE id=%s
            RETURNING id
            """,
            (hours, addr, maps, mp_url, bank_name, bank_acc_name, bank_clabe, bank_acc_num, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        return {"ok": True}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

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
                   mercadopago_url, bank_name, bank_account_name, bank_clabe, bank_account_number
            FROM companies
            WHERE id=%s
            LIMIT 1
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")

        return {
            "ok": True,
            "settings": {
                "hours_text": row[0],
                "address_text": row[1],
                "google_maps_url": row[2],
                "mercadopago_url": row[3],
                "bank_name": row[4],
                "bank_account_name": row[5],
                "bank_clabe": row[6],
                "bank_account_number": row[7],
            },
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@app.get("/api/company/me")
def company_me(request: Request):
    company_id = require_company_id(request)

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id::text, name, slug, twilio_phone FROM companies WHERE id=%s LIMIT 1",
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")

        return {
            "ok": True,
            "company": {
                "id": row[0],
                "name": row[1],
                "slug": row[2],
                "twilio_phone": row[3],
            },
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

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

# -------------------------
# WhatsApp webhook verify
# -------------------------

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
        if conn:
            conn.close()


# -------------------------
# Auth (cookie session)
# -------------------------
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
        cur.execute(
            "insert into users (email, password_hash) values (%s, %s) returning id",
            (email, password_hash),
        )
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
        if cur:
            cur.close()
        if conn:
            conn.close()

@app.post("/api/whatsapp/provision")
def whatsapp_provision(request: Request):
    _ = get_user_from_session(request)
    # stub: evita 404
    raise HTTPException(status_code=501, detail="WhatsApp provisioning aún no disponible")

@app.post("/api/company/whatsapp/provision")
def company_whatsapp_provision(request: Request):
    company_id = require_company_id(request)

    # 1) Si ya hay número, regresa el existente (idempotente)
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
        if cur:
            cur.close()
        if conn:
            conn.close()

    # 2) Comprar número Twilio (correcto: buscar disponible y luego comprar)
    client = twilio_client()

    # Puedes parametrizar país/area_code después. Por ahora US 571 como traías.
    available = client.available_phone_numbers("US").local.list(area_code="571", limit=1)
    if not available:
        raise HTTPException(status_code=409, detail="No hay números disponibles (area_code=571)")

    chosen = available[0].phone_number  # ej +1571...
    purchased = client.incoming_phone_numbers.create(phone_number=chosen)

    twilio_phone = f"whatsapp:{purchased.phone_number}"  # ej whatsapp:+1571...

    # 3) Guardar en DB (y proteger contra duplicados con unique index)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE companies SET twilio_phone=%s, updated_at=now() WHERE id=%s RETURNING id",
            (twilio_phone, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")

    except IntegrityError:
        # Si por alguna razón el mismo twilio_phone ya existe en otra company
        raise HTTPException(status_code=409, detail="Ese número ya está asignado a otra empresa")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

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

        cur.execute(
            "select id, password_hash from users where email=%s and is_active=true",
            (email,),
        )
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
            key=SESSION_COOKIE_NAME,
            value=sid,
            httponly=True,
            secure=True,
            samesite="none",
            domain=".cotizaexpress.com",
            path="/",
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
        if cur:
            cur.close()
        if conn:
            conn.close()


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
            if cur:
                cur.close()
            if conn:
                conn.close()

    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        domain=".cotizaexpress.com",
    )
    return {"ok": True}


# -------------------------
# Pricebook template (cookie auth)
# -------------------------
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


# -------------------------
# Pricebook upload (bearer token)
# -------------------------
@app.post("/api/pricebook/upload")
def pricebook_upload(
    authorization: str = Header(default=""),
    file: UploadFile = File(...),
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
            """
            INSERT INTO pricebook_uploads (company_id, filename, status)
            VALUES (%s, %s, 'processing')
            RETURNING id
            """,
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
            "nombre": "name",
            "producto": "name",
            "product": "name",
            "precio": "price",
            "precio_base": "price",
            "precio unitario": "price",
            "costo": "price",
            "cost": "price",
            "unidad": "unit",
            "uom": "unit",
            "vat_rate": "vat_rate",
            "iva": "vat_rate",
            "sku": "sku",
        }

        headers_mapped = [alias.get(h, h) for h in headers_norm]
        idx = {h: i for i, h in enumerate(headers_mapped)}

        required = {"name", "price"}
        missing = required - set(headers_mapped)
        if missing:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": f"Faltan columnas requeridas: {sorted(missing)}",
                    "headers_detectadas": headers_norm,
                    "headers_mapeadas": headers_mapped,
                },
            )

        rows_total = 0
        rows_upserted = 0
        rows_skipped = 0

        for r in ws.iter_rows(min_row=2, values_only=True):
            if r is None or all(v is None or str(v).strip() == "" for v in r):
                continue

            rows_total += 1

            name = str(r[idx["name"]]).strip() if r[idx["name"]] is not None else ""
            price_raw = r[idx["price"]] if idx.get("price") is not None else None

            if not name or price_raw is None:
                rows_skipped += 1
                continue

            try:
                price_str = str(price_raw).replace("$", "").replace(",", "").strip()
                price = float(price_str)
            except Exception:
                rows_skipped += 1
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

            name_norm = norm_name(name)

            cur.execute(
                """
                INSERT INTO pricebook_items
                    (company_id, sku, name, name_norm, unit, price, vat_rate, source, updated_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, 'excel', now())
                ON CONFLICT (company_id, name_norm)
                DO UPDATE SET
                    sku = EXCLUDED.sku,
                    name = EXCLUDED.name,
                    unit = EXCLUDED.unit,
                    price = EXCLUDED.price,
                    vat_rate = EXCLUDED.vat_rate,
                    source = 'excel',
                    updated_at = now()
                """,
                (company_id, sku, name, name_norm, unit, price, vat_rate),
            )

            rows_upserted += 1

        cur.execute(
            """
            UPDATE pricebook_uploads
            SET status='success',
                rows_total=%s,
                rows_upserted=%s,
                error=NULL,
                finished_at=now()
            WHERE id=%s
            """,
            (rows_total, rows_upserted, upload_id),
        )

        
        try:
            rebuild_embeddings_for_company(conn, company_id)
        except Exception as e:
            print("EMBEDDINGS REBUILD ERROR:", repr(e))
        
        
        return {
            "ok": True,
            "company_id": company_id,
            "upload_id": str(upload_id),
            "rows_total": rows_total,
            "rows_upserted": rows_upserted,
            "rows_skipped": rows_skipped,
        }

    except HTTPException:
        raise

    except Exception as e:
        print("UPLOAD ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# -------------------------
# Pricebook items (cookie session, DEFAULT_COMPANY_ID)
# -------------------------
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
                where company_id = %s
                  and (sku ilike %s or name ilike %s or name_norm ilike %s)
                order by name asc
                limit %s
                """,
                (company_id, like, like, like, limit),
            )
        else:
            cur.execute(
                """
                select id, company_id, sku, name, unit, price, vat_rate, source, updated_at, created_at
                from pricebook_items
                where company_id = %s
                order by name asc
                limit %s
                """,
                (company_id, limit),
            )

        rows = cur.fetchall()

        items = []
        for r in rows:
            items.append(
                {
                    "id": r[0],
                    "company_id": r[1],
                    "sku": r[2],
                    # compat con frontend:
                    "name": r[3],
                    "description": r[3],
                    "unit": r[4],
                    "price": float(r[5]) if r[5] is not None else None,
                    "vat_rate": float(r[6]) if r[6] is not None else None,
                    "source": r[7],
                    "updated_at": r[8].isoformat() if r[8] else None,
                    "created_at": r[9].isoformat() if r[9] else None,
                }
            )

        return {"ok": True, "items": items}

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print("PRICEBOOK ITEMS ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"pricebook_items failed: {type(e).__name__}: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# -------------------------
# Pricebook create item (cookie session, DEFAULT_COMPANY_ID)
# -------------------------
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
        try:
            price = float(price)
        except Exception:
            raise HTTPException(status_code=400, detail="price inválido")
        if price < 0:
            raise HTTPException(status_code=400, detail="price debe ser >= 0")

    vat_rate = body.vat_rate
    if vat_rate is not None:
        try:
            vat_rate = float(vat_rate)
        except Exception:
            raise HTTPException(status_code=400, detail="vat_rate inválido")
        if vat_rate < 0 or vat_rate > 1:
            raise HTTPException(status_code=400, detail="vat_rate debe estar entre 0 y 1")

    name_norm = norm_name(name)

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # UPSERT recomendado (si existe por name_norm, lo actualiza)
        cur.execute(
            """
            INSERT INTO pricebook_items
                (company_id, sku, name, name_norm, unit, price, vat_rate, source, updated_at, created_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (company_id, name_norm)
            DO UPDATE SET
                sku = EXCLUDED.sku,
                name = EXCLUDED.name,
                unit = EXCLUDED.unit,
                price = EXCLUDED.price,
                vat_rate = EXCLUDED.vat_rate,
                source = EXCLUDED.source,
                updated_at = now()
            RETURNING id
            """,
            (company_id, sku, name, name_norm, unit, price, vat_rate, source),
        )

        new_id = cur.fetchone()[0]

        # frontend espera {ok:true}. Dejo id extra por debug, si no lo quieres quítalo.
        
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
        if cur:
            cur.close()
        if conn:
            conn.close()


# -------------------------
# Companies create (bearer)
# -------------------------

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

        # Borra SOLO si pertenece a la company
        cur.execute(
            """
            DELETE FROM pricebook_items
            WHERE company_id = %s AND id = %s
            RETURNING id
            """,
            (company_id, item_id),
        )
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
        if cur:
            cur.close()
        if conn:
            conn.close()

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
            SET name=%s, name_norm=%s, sku=%s, unit=%s, price=%s, vat_rate=%s,
                synonyms=%s, updated_at=now()
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
                SELECT MIN(id::text)::uuid
                FROM pricebook_items
                WHERE company_id=%s
                GROUP BY name_norm
            )
            AND company_id=%s
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

        cur.execute(
            """
            INSERT INTO companies (name, slug)
            VALUES (%s, %s)
            RETURNING id
            """,
            (name, slug),
        )
        company_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO api_keys (company_id, name, prefix, key_hash)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (company_id, key_name, prefix, key_hash),
        )
        api_key_id = cur.fetchone()[0]

        return {
            "ok": True,
            "company_id": str(company_id),
            "api_key_id": str(api_key_id),
            "api_key": token,
            "api_key_prefix": prefix,
        }

    except IntegrityError:
        raise HTTPException(status_code=409, detail="Slug ya existe o conflicto")
    except Exception as e:
        print("CREATE COMPANY ERROR:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

class TwilioPhoneBody(BaseModel):
    twilio_phone: str  # "whatsapp:+15715463202"


@app.post("/api/admin/companies/{company_id}/twilio/provision")
def provision_twilio_number(company_id: str, request: Request):
    _ = get_user_from_session(request)
    client = twilio_client()

    # 1) Buscar un número disponible (USA ejemplo)
    available = client.available_phone_numbers("US").local.list(area_code="571", limit=1)
    if not available:
        raise HTTPException(status_code=409, detail="No hay números disponibles en ese area_code")

    chosen = available[0].phone_number  # ej +1571XXXXXXX

    # 2) Comprar el número
    purchased = client.incoming_phone_numbers.create(phone_number=chosen)

    # 3) Guardar en formato que tu webhook usa
    twilio_phone = f"whatsapp:{purchased.phone_number}"  # ej whatsapp:+1571...

    # 4) Guardar en DB
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE companies SET twilio_phone=%s WHERE id=%s RETURNING id",
            (twilio_phone, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Empresa no encontrada")
    finally:
        cur.close()
        conn.close()

    return {"ok": True, "company_id": company_id, "twilio_phone": twilio_phone}

@app.post("/api/companies/{company_id}/twilio_phone")
def set_company_twilio_phone(company_id: str, body: TwilioPhoneBody, request: Request):
    _ = get_user_from_session(request)  # protege con tu login web

    tw = (body.twilio_phone or "").strip()
    if not tw.startswith("whatsapp:+"):
        raise HTTPException(status_code=400, detail="Formato inválido. Usa whatsapp:+E164")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE companies SET twilio_phone=%s, updated_at=now() WHERE id=%s RETURNING id",
            (tw, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")
        return {"ok": True}
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Ese número ya está asignado a otra empresa")
    finally:
        cur.close()
        conn.close()


# -------------------------
# Search pricebook (bearer)
# -------------------------
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
            WHERE company_id=%s
              AND ({where_sql} OR sku ILIKE %s OR name ILIKE %s)
            LIMIT %s
            """,
            (*params, f"%{q_clean}%", f"%{q_clean}%", max(limit, 12)),
        )
        rows = cur.fetchall()

        items = []
        for sku, name, unit, price, vat_rate, updated_at in rows:
            items.append(
                {
                    "sku": sku,
                    "name": name,
                    "unit": unit,
                    "price": float(price) if price is not None else None,
                    "vat_rate": float(vat_rate) if vat_rate is not None else None,
                    "updated_at": updated_at.isoformat() if updated_at else None,
                }
            )

        # 1) si hay specs, filtra por restricciones duras
        constrained = [it for it in items if passes_constraints(it["name"], specs)]
        pool = constrained if constrained else items

        # 2) rankea por similitud real
        best, score = rank_best_match(q_clean, pool)

        # score bajo => mejor pedir confirmación / SKU
        if not best or score < 72:
            return []

        return [best]
    finally:
        cur.close()


def extract_qty_items_robust(text: str):
    t = (text or "").lower()
    t = t.replace("+", " ")
    t = re.sub(r"[•;]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # quita palabras ruido
    t = re.sub(r"\b(cotiza|cotización|cotizacion|precio|precios|por favor|porfa|pls)\b", " ", t)
    # protege fracciones como 1/4, 1/2, 5/8
    t = re.sub(r"(\d+)\s*/\s*(\d+)", r"\1_\2", t)
    t = re.sub(r"\s+", " ", t).strip()

    # separa por comas primero
    parts = [p.strip() for p in t.split(",") if p.strip()]
    items = []

    for part in parts:
        # dentro de cada parte, puede venir " ... y 10 ..."
        subparts = [s.strip() for s in re.split(r"\s+y\s+", part) if s.strip()]

        for s in subparts:
            pattern = r"(\d+)\s+(.+?)(?=\s+\d+\s+|$)"
            matches = re.findall(pattern, s)
            for qty_s, prod in matches:
                prod = prod.replace("_", "/").strip()
                if not prod:
                    continue
                items.append((int(qty_s), prod))

    return items
    
def build_reply_for_company(company_id: str, user_text: str, wa_from: str = "") -> str:
    user_text = (user_text or "").strip().replace('"', '').replace('"', '').replace('"', '')
    wa_from = (wa_from or "").strip()

    try:
        usage_info = track_conversation_if_new(company_id, wa_from)
        if usage_info.get("limit", 0) > 0 and usage_info.get("usage", 0) > usage_info.get("limit", 0):
            print("WA LIMIT EXCEEDED:", usage_info)
    except Exception as e:
        print("WA TRACK ERROR:", repr(e))

    import string as _string

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
                SELECT sku, name, unit, price, vat_rate
                FROM pricebook_items
                WHERE company_id=%s
                  AND ({where_sql} OR sku ILIKE %s OR name ILIKE %s)
                LIMIT 30
                """,
                (*params, f"%{q_clean}%", f"%{q_clean}%"),
            )
            rows = cur.fetchall()
            items = []
            for sku, name, unit, price, vat_rate in rows:
                it = {
                    "sku": sku,
                    "name": name,
                    "unit": unit,
                    "price": float(price) if price is not None else None,
                    "vat_rate": float(vat_rate) if vat_rate is not None else None,
                }
                sn = norm_name(name or "")
                sku_n = norm_name(sku or "")
                it["_score"] = max(
                    fuzz.token_set_ratio(qn, sn),
                    fuzz.token_set_ratio(qn, sku_n) if sku else 0,
                )
                items.append(it)
            items.sort(key=lambda x: x.get("_score", 0), reverse=True)
            out = []
            for it in items[: max(1, int(limit or 5))]:
                it.pop("_score", None)
                out.append(it)
            return out
        finally:
            cur.close()

    def _render_pending_suggestions(pending: list) -> str:
        if not pending:
            return ""
        letters = _string.ascii_uppercase
        lines = ["No pude identificar algunos productos. Elige del catálogo:"]
        for i, p in enumerate(pending[:6]):
            tag = letters[i]
            qty = int(p.get("qty") or 0)
            raw = (p.get("raw") or "").strip()
            cands = p.get("candidates") or []
            lines.append(f"\n❓ ({tag}) {qty} x {raw}")
            if not cands:
                lines.append("   (sin sugerencias) Mándalo más exacto o con SKU.")
                continue
            for j, it in enumerate(cands[:5], start=1):
                unit = it.get("unit") or "unidad"
                price = float(it.get("price") or 0.0)
                sku = (it.get("sku") or "").strip()
                sku_txt = f" (SKU {sku})" if sku else ""
                lines.append(f"   {tag}{j}) {it['name']}{sku_txt} — ${price:,.2f} / {unit}")
        lines.append("\n✅ Responde con: A1, B2, C1 (ejemplo).")
        return "\n".join(lines)

    def _parse_pending_picks(text: str):
        t = (text or "").upper().replace(" ", "")
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
    # 0) COMANDOS (reset / salir)
    # =========================================================
    tnorm = norm_name(user_text).replace("cotización", "cotizacion")

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
            "Ej: 10 tablaroca ultralight, 5 postes 4.10\n\n"
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
            "Si quieres otra cotización, mándame: 10 tablaroca ultralight, 5 postes 4.10\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
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
                    "👉 10 tablaroca ultralight, 5 postes 4.10\n\n"
                    "🧭 Comandos:\n"
                    "• 'nueva cotizacion' → empezar de cero\n"
                    "• 'salir' → cancelar"
                )
        return (
            "👋 ¡Hola! Puedo cotizarte materiales.\n\n"
            "Mándame tu pedido así:\n"
            "👉 10 tablaroca ultralight, 5 postes 4.10\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

    # =========================================================
    # 1) MULTI-ITEMS + CARRITO
    # =========================================================
    multi = extract_qty_items_robust(user_text)
    if multi:
        conn = get_conn()
        try:
            state = get_quote_state(company_id, wa_from) if wa_from else None
            if not state:
                state = {}

            missing = []

            for qty, prod_raw in multi:
                if not looks_like_product_phrase(prod_raw):
                    continue

                try:
                    result = smart_search(conn, company_id, prod_raw, qty)
                except Exception as e:
                    print("SMART SEARCH ERROR:", repr(e))
                    result = {"status": "not_found", "item": None, "candidates": []}

                if result["status"] == "found":
                    best = result["item"]
                    state = cart_add_item(
                        state,
                        {
                            "sku": best.get("sku"),
                            "name": best.get("name"),
                            "unit": best.get("unit") or "unidad",
                            "price": float(best.get("price") or 0.0),
                            "vat_rate": best.get("vat_rate"),
                            "qty": qty,
                        },
                    )
                else:
                    missing.append({
                        "qty": qty,
                        "raw": prod_raw,
                        "candidates": result["candidates"],
                    })

            if missing:
                state["pending"] = missing
            else:
                state.pop("pending", None)

            if wa_from:
                upsert_quote_state(company_id, wa_from, state)

            msg = (
                cart_render_quote(state)
                if (state.get("cart") or [])
                else "No encontré esos productos en el catálogo."
            )

            if missing:
                msg += "\n\n" + _render_pending_suggestions(missing)

            msg += (
                "\n\n¿Agregamos algo más?\n"
                "🧭 Comandos:\n"
                "• 'nueva cotizacion' → empezar de cero\n"
                "• 'salir' → cancelar"
            )
            return msg
        finally:
            conn.close()

    # =========================================================
    # 2) SINGLE ITEM + CARRITO
    # =========================================================
    qty, prod_query = extract_qty_and_product(user_text)
    if qty and prod_query:
        conn = get_conn()
        try:
            try:
                result = smart_search(conn, company_id, prod_query, qty)
            except Exception as e:
                print("SMART SEARCH ERROR:", repr(e))
                result = {"status": "not_found", "item": None, "candidates": []}

            if result["status"] == "found":
                best = result["item"]
                state = get_quote_state(company_id, wa_from) if wa_from else None
                if not state:
                    state = {}
                state = cart_add_item(
                    state,
                    {
                        "sku": best.get("sku"),
                        "name": best.get("name"),
                        "unit": best.get("unit") or "unidad",
                        "price": float(best.get("price") or 0.0),
                        "vat_rate": best.get("vat_rate"),
                        "qty": qty,
                    },
                )
                state.pop("pending", None)
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return (
                    cart_render_quote(state)
                    + "\n\n¿Agregamos algo más?\n"
                    "🧭 Comandos:\n"
                    "• 'nueva cotizacion' → empezar de cero\n"
                    "• 'salir' → cancelar"
                )
            elif result["status"] == "ambiguous":
                pending = [{"qty": qty, "raw": prod_query, "candidates": result["candidates"]}]
                state = get_quote_state(company_id, wa_from) if wa_from else {}
                if not state:
                    state = {}
                state["pending"] = pending
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return (
                    _render_pending_suggestions(pending)
                    + "\n\n🧭 Comandos:\n"
                    "• 'nueva cotizacion' → empezar de cero\n"
                    "• 'salir' → cancelar"
                )
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
                + "\n\nDime cantidades para cotizar (ej: 10 tablaroca ultralight).\n\n"
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

        # (A) Picks tipo A1, B2
        picks = _parse_pending_picks(user_text)
        if picks:
            letters = _string.ascii_uppercase
            letter_to_idx = {ch: i for i, ch in enumerate(letters)}
            remove_idxs = set()

            for letter, opt in picks:
                pi = letter_to_idx.get(letter)
                if pi is None or pi < 0 or pi >= len(pend):
                    continue
                p = pend[pi]
                cands = p.get("candidates") or []
                if not cands or opt < 1 or opt > len(cands):
                    continue
                chosen = cands[opt - 1]
                qty = int(p.get("qty") or 0)
                state = cart_add_item(
                    state,
                    {
                        "sku": chosen.get("sku"),
                        "name": chosen.get("name"),
                        "unit": chosen.get("unit") or "unidad",
                        "price": float(chosen.get("price") or 0.0),
                        "vat_rate": chosen.get("vat_rate"),
                        "qty": qty,
                    },
                )
                remove_idxs.add(pi)

            new_pending = [p for i, p in enumerate(pend) if i not in remove_idxs]
            if new_pending:
                state["pending"] = new_pending
            else:
                state.pop("pending", None)

            try:
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
            except Exception as e:
                print("UPSERT STATE ERROR:", repr(e))
                return "Error guardando cotización. Intenta de nuevo."

            msg = cart_render_quote(state) if (state.get("cart") or []) else "✅ Listo."
            if state.get("pending"):
                msg += "\n\n" + _render_pending_suggestions(state["pending"])
            msg += (
                "\n\n¿Agregamos algo más?\n"
                "🧭 Comandos:\n"
                "• 'nueva cotizacion' → empezar de cero\n"
                "• 'salir' → cancelar"
            )
            return msg

        # (B) Aclaraciones de texto
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
                        best = result["item"]
                        state = cart_add_item(
                            state,
                            {
                                "sku": best.get("sku"),
                                "name": best.get("name"),
                                "unit": best.get("unit") or "unidad",
                                "price": float(best.get("price") or 0.0),
                                "vat_rate": best.get("vat_rate"),
                                "qty": qty,
                            },
                        )
                    else:
                        still.append({
                            "qty": qty,
                            "raw": prod_raw,
                            "candidates": result["candidates"],
                        })

                if still:
                    state["pending"] = still
                else:
                    state.pop("pending", None)

                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)

                msg = cart_render_quote(state) if (state.get("cart") or []) else "✅ Listo."
                if state.get("pending"):
                    msg += "\n\n" + _render_pending_suggestions(state["pending"])
                msg += (
                    "\n\n¿Agregamos algo más?\n"
                    "🧭 Comandos:\n"
                    "• 'nueva cotizacion' → empezar de cero\n"
                    "• 'salir' → cancelar"
                )
                return msg
            finally:
                conn.close()

        # (C) Re-muestra sugerencias
        if pend and pend[0].get("candidates"):
            return (
                _render_pending_suggestions(pend)
                + "\n\n🧭 Comandos:\n"
                "• 'nueva cotizacion' → empezar de cero\n"
                "• 'salir' → cancelar"
            )

        # (D) Lista pendientes sin candidatos
        pendientes_txt = "\n".join(
            [f"- {int(p.get('qty') or 0)} x {(p.get('raw') or '').strip()}" for p in pend[:12]]
        )
        return (
            "👍 Mándame el nombre exacto o SKU de estos productos:\n\n"
            f"{pendientes_txt}\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

    # =========================================================
    # 4) GUARD — mensaje con números pero sin producto
    # =========================================================
    if re.search(r"\b\d+\b", user_text):
        return (
            "Veo cantidades pero no encontré esos productos.\n\n"
            "👉 Escríbelos más exacto o con SKU.\n"
            "Ejemplo: '10 tablaroca ultralight usg' o '5 postes 4.10 cal26'.\n\n"
            "🧭 Comandos:\n"
            "• 'nueva cotizacion' → empezar de cero\n"
            "• 'salir' → cancelar"
        )

    # =========================================================
    # 4.5) HORARIOS / UBICACIÓN
    # =========================================================
    if looks_like_hours_question(user_text):
        return (
            "📍 Para horarios y ubicación, dime tu sucursal o ciudad.\n\n"
            "Si quieres cotizar: mándame ej: 10 tablaroca ultralight, 5 postes 4.10"
        )

    # =========================================================
    # 5) OPENAI FALLBACK
    # =========================================================
    if not openai_client:
        return "Estoy en mantenimiento. Intenta más tarde."

    resp = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": COTIZABOT_SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
        temperature=0.3,
    )
    return resp.choices[0].message.content or "¿Me repites eso?"

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

                return {
                    "reply": (
                        "Cotización rápida:\n"
                        f"- {qty} {unit} de {it['name']} x ${price:,.2f} = ${subtotal:,.2f}\n"
                        f"IVA (16%): ${iva:,.2f}\n"
                        f"Total: ${total:,.2f}\n\n"
                        "¿Agregamos otro producto?"
                    )
                }
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
                        + "\n\nDime cantidades para armar cotización "
                          "(ej: 10 tablaroca ultralight)."
                    )
                    return {"reply": reply}
            except Exception:
                pass

    if not openai_client:
        return {"reply": "Falta configurar OPENAI_API_KEY en Render."}

    greetings = {"hola", "buenas", "hey", "holi"}

    if norm_name(user_text) in greetings:
        return {
            "reply": (
                "👋 ¡Hola! Puedo cotizarte materiales.\n\n"
                "Mándame tu pedido así:\n"
                "👉 10 tablaroca ultralight, 5 postes 4.10\n\n"
                "🧭 Comandos:\n"
                "• 'nueva cotizacion' → empezar de cero\n"
                "• 'salir' → cancelar"
            )
        }

    
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

@app.post("/webhook/twilio")
async def twilio_webhook(
    From: str = Form(...),
    To: str = Form(...),
    Body: str = Form(default=""),
    MessageSid: str = Form(default=""),
):
    From = normalize_wa(From)
    To = normalize_wa(To)
    Body = (Body or "").strip()

    TWIML_OK = Response(
        content='<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
        media_type="text/xml",
    )

    # Deduplicación — ignora si ya procesamos este MessageSid
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

        if not Body:
            twilio_send_whatsapp(
                to_user_whatsapp=From,
                text="Solo proceso mensajes de texto por ahora 📝",
            )
            return TWIML_OK

        company = get_company_by_twilio_number(To)
        print("TWILIO company:", company)

        if not company:
            twilio_send_whatsapp(
                to_user_whatsapp=From,
                text="Hola 👋 Este número aún no está ligado a una empresa.",
            )
            return TWIML_OK

        reply_text = build_reply_for_company(company["company_id"], Body, wa_from=From)
        reply_text = (reply_text or "").strip() or "¿Me repites eso?"
        print("REPLY TEXT:", repr(reply_text))

        twilio_send_whatsapp(to_user_whatsapp=From, text=reply_text)
        print("WHATSAPP ENVIADO OK")
        return TWIML_OK

    except Exception as e:
        print("TWILIO WEBHOOK ERROR:", repr(e))
        traceback.print_exc()
        try:
            twilio_send_whatsapp(
                to_user_whatsapp=From,
                text="Error interno. Intenta de nuevo en 1 minuto.",
            )
        except Exception:
            pass
        return TWIML_OK

