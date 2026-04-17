from prompts_cotizabot import COTIZABOT_SYSTEM_PROMPT
from fastapi.background import BackgroundTasks
import asyncio
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
from semantic_search import smart_search, rebuild_embeddings_for_company, upsert_single_embedding, seed_jerga_global, auto_generate_context_groups
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

# ── Silence detector: escalamiento proactivo por inactividad ──────────────
# Corre cada 5 min. Si un cliente tiene pending/awaiting state sin actividad
# por >10 min, notifica al owner y marca el estado como escalado por silencio.
SILENCE_CHECK_INTERVAL_SEC = 300  # 5 min
SILENCE_THRESHOLD_MIN = 10
CONVERSATION_DEATH_MIN = 60  # 1 hora sin respuesta → limpiar estado

async def _silence_escalation_loop():
    while True:
        try:
            await asyncio.sleep(SILENCE_CHECK_INTERVAL_SEC)
            _run_silence_escalation_once()
            _run_conversation_death_once()
        except Exception as _e:
            print("SILENCE LOOP ERROR:", repr(_e))

def _run_conversation_death_once():
    """Mata (limpia) el estado de conversaciones sin actividad por >1 hora.
    Esto evita que el buffer de mensajes, carritos viejos, o flags como
    awaiting_removal queden colgados indefinidamente y afecten interacciones futuras.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM wa_quote_state
            WHERE updated_at < now() - interval '%s minutes'
            RETURNING company_id::text, wa_from
            """ % CONVERSATION_DEATH_MIN
        )
        killed = cur.fetchall() or []
        conn.commit()
        cur.close()
        conn.close()
        for company_id, wa_from in killed:
            print(f"CONVERSATION DEATH: company={company_id} client={wa_from} idle>{CONVERSATION_DEATH_MIN}min — state cleared")
    except Exception as _e:
        print("CONVERSATION DEATH ERROR:", repr(_e))

def _run_silence_escalation_once():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT wqs.company_id::text, wqs.wa_from, wqs.state, wqs.updated_at,
                   c.owner_phone, c.wa_api_key, c.wa_phone_number_id, c.telefono_atencion, c.name
            FROM wa_quote_state wqs
            JOIN companies c ON c.id = wqs.company_id
            WHERE wqs.updated_at < now() - interval '%s minutes'
              AND wqs.updated_at > now() - interval '2 hours'
            """ % SILENCE_THRESHOLD_MIN
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        for row in rows:
            company_id, wa_from, state, updated_at, owner_phone, wa_api_key, phone_number_id, telefono_atencion, company_name = row
            state = state or {}
            if state.get("escalated_silence"):
                continue
            _has_pending = bool(
                state.get("pending")
                or state.get("awaiting")
                or state.get("awaiting_removal")
                or state.get("pending_ambiguous")
                or state.get("pending_specs")
            )
            if not _has_pending:
                continue
            _notify_phone = (telefono_atencion or owner_phone or "").strip()
            if not (_notify_phone and wa_api_key and phone_number_id):
                continue
            try:
                notify_owner_escalation(
                    wa_api_key=wa_api_key, phone_number_id=phone_number_id,
                    owner_phone=_notify_phone, client_phone=wa_from,
                    reason=f"Cliente sin responder >{SILENCE_THRESHOLD_MIN} min con pregunta pendiente",
                    state=state,
                )
                state["escalated_silence"] = True
                upsert_quote_state(company_id, wa_from, state)
                print(f"SILENCE ESCALATION: company={company_id} client={wa_from} silent>{SILENCE_THRESHOLD_MIN}min")
            except Exception as _ne:
                print("SILENCE NOTIFY ERROR:", repr(_ne))
    except Exception as _e:
        print("SILENCE ESCALATION RUN ERROR:", repr(_e))

@app.on_event("startup")
async def _start_silence_loop():
    asyncio.create_task(_silence_escalation_loop())
    print(f"Silence escalation loop started (check every {SILENCE_CHECK_INTERVAL_SEC}s, threshold {SILENCE_THRESHOLD_MIN} min)")

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


_BUNDLE_WORDS = {"atado", "atados", "paquete", "paquetes", "bulto", "bultos", "caja", "cajas"}

def _resolve_bundle_qty(qty: int, is_bundle: bool, item: dict) -> int:
    """If customer ordered in bundles and product has bundle_size, multiply qty."""
    if is_bundle and item and item.get("bundle_size"):
        return qty * int(item["bundle_size"])
    return qty

def extract_qty_and_product(text: str):
    """Extract quantity and product from text like '10 tubos' or '2 atados de poste'.
    Returns (qty, product, is_bundle) where is_bundle=True if customer used bundle words."""
    t = (text or "").strip().lower()
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s+(.+?)\s*$", t)
    if not m:
        return None, None, False
    qty = int(float(m.group(1)))  # "175.00" → 175
    product = m.group(2).strip()
    # Detect bundle: "2 atados de poste" → qty=2, product="poste", is_bundle=True
    bundle_match = re.match(r"^(atados?|paquetes?|bultos?|cajas?)\s+(?:de\s+)?(.+)$", product)
    if bundle_match:
        return qty, bundle_match.group(2).strip(), True
    return qty, product, False


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


def _run_pricebook_migrations(conn):
    """Idempotent DB migrations."""
    cur = conn.cursor()
    try:
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='pricebook_items' AND column_name='bundle_size'
                ) THEN
                    ALTER TABLE pricebook_items ADD COLUMN bundle_size INTEGER;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='companies' AND column_name='context_groups'
                ) THEN
                    ALTER TABLE companies ADD COLUMN context_groups JSONB;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='pricebook_items' AND column_name='is_default'
                ) THEN
                    ALTER TABLE pricebook_items ADD COLUMN is_default BOOLEAN DEFAULT FALSE;
                END IF;
            END $$;
        """)
        conn.commit()
        print("PRICEBOOK MIGRATIONS: OK (bundle_size, context_groups, is_default)")
    except Exception as e:
        print("PRICEBOOK MIGRATION ERROR:", repr(e))
        conn.rollback()
    finally:
        cur.close()


def _run_promo_codes_migration(conn):
    """Create promo_codes tables + trial_end column (idempotent)."""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                code        TEXT NOT NULL UNIQUE,
                discount_type TEXT NOT NULL DEFAULT 'trial_days',
                discount_value NUMERIC NOT NULL DEFAULT 10,
                max_uses    INT DEFAULT NULL,
                times_used  INT NOT NULL DEFAULT 0,
                one_per_customer BOOLEAN NOT NULL DEFAULT TRUE,
                active      BOOLEAN NOT NULL DEFAULT TRUE,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at  TIMESTAMPTZ DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS promo_code_uses (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                promo_code_id UUID NOT NULL REFERENCES promo_codes(id),
                company_id  UUID NOT NULL,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_promo_code_uses_company
                ON promo_code_uses(company_id);
            CREATE INDEX IF NOT EXISTS idx_promo_code_uses_code
                ON promo_code_uses(promo_code_id);
        """)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'companies' AND column_name = 'trial_end'
                ) THEN
                    ALTER TABLE companies ADD COLUMN trial_end TIMESTAMPTZ DEFAULT NULL;
                END IF;
            END $$;
        """)
        print("PROMO_CODES MIGRATION: OK")
    except Exception as e:
        print(f"PROMO_CODES MIGRATION ERROR: {repr(e)}")
    finally:
        cur.close()

# Run pricebook migrations at startup (idempotent)
try:
    _mig_conn = get_conn()
    _run_pricebook_migrations(_mig_conn)
    _run_promo_codes_migration(_mig_conn)
    _mig_conn.close()
except Exception as e:
    print(f"PRICEBOOK MIGRATION STARTUP ERROR: {repr(e)}")

# Seed jerga_global con términos críticos al iniciar
try:
    _seed_conn = get_conn()
    seed_jerga_global(_seed_conn)
    _seed_conn.close()
except Exception as e:
    print(f"SEED STARTUP ERROR: {repr(e)}")

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
        cur.execute("SELECT plan_code, trial_end FROM companies WHERE id=%s LIMIT 1", (company_id,))
        row = cur.fetchone()
        if not row:
            return "free"
        plan_code = (row[0] or "free").strip()
        trial_end = row[1]
        # Auto-expire trial: if trial_end is set and has passed, revert to free
        if trial_end and plan_code in ("pro", "complete"):
            if datetime.now(timezone.utc) > trial_end:
                try:
                    cur.execute(
                        "UPDATE companies SET plan_code='free', trial_end=NULL, updated_at=now() WHERE id=%s",
                        (company_id,),
                    )
                    print(f"TRIAL EXPIRADO: company={company_id} plan={plan_code} -> free")
                except Exception as e:
                    print(f"TRIAL EXPIRE ERROR: {repr(e)}")
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
    rows = [{"id": f"spec_{i}", "title": opt[:24]} for i, opt in enumerate(options[:10])]
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
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "high"}},
                    {"type": "text", "text": (
                        "Eres asistente de ferretería mexicana. Esta imagen contiene una lista "
                        "de materiales. Puede ser una nota manuscrita, una tabla digital, una "
                        "captura de pantalla, o una foto de un pedido. "
                        "Extrae TODOS los productos con sus cantidades, sin omitir ninguno. "
                        "Formato estricto: CANTIDAD PRODUCTO, un item por línea. "
                        "La cantidad siempre debe ser un número entero (sin decimales). "
                        "Si ves '175.00', escribe '175'. Si ves '20.00', escribe '20'. "
                        "Si la imagen es una tabla con columnas (ej: Conceptos | Cantidad), "
                        "lee CADA fila y pon la cantidad antes del nombre del producto. "
                        "Productos comunes: poste, tablaroca, cemento, varilla, block, "
                        "malla, perfacinta, redimix, canal, tornillo, clavo, tubo, pija, "
                        "durock, basecoat, ángulo, canaleta, reborde, taquete. "
                        "Conserva medidas y especificaciones tal cual aparecen (ej: 'Cal 26', '1/2\"', '6 x 1', '10 x 1 1/2'). "
                        "Si una palabra parece un producto con error ortográfico, corrígela. "
                        "Ignora encabezados, totales, fechas, logos y textos que no sean productos. "
                        "Si un renglón existe pero no puedes leerlo, escribe: 1 ???. "
                        "NO agregues productos que no estén en la imagen. "
                        "Ejemplo de salida:\n20 tablaroca ultralight USG\n50 ángulo amarre cal 26\n1200 pija 6 x 1\n"
                        "Si no hay lista de productos en absoluto, responde exactamente: NO_LIST"
                    )}
                ]
            }],
            max_tokens=800, temperature=0.1,
        )
        result = (resp.choices[0].message.content or "").strip()
        return None if result == "NO_LIST" else result
    except Exception as e:
        print("VISION ERROR:", repr(e))
        return None

def _normalize_mx_phone(phone: str) -> str:
    """Normalize a Mexican phone number to include country code 52.
    Handles: 8341891017 → 528341891017, +528341891017 → 528341891017, etc."""
    p = (phone or "").replace("+", "").replace(" ", "").replace("-", "").replace("whatsapp:", "").strip()
    # If it's exactly 10 digits (local Mexican number), prepend 52
    if len(p) == 10 and p.isdigit():
        p = "52" + p
    return p

def notify_owner_escalation(wa_api_key: str, phone_number_id: str, owner_phone: str,
                             client_phone: str, reason: str, state: dict):
    owner_phone_clean = _normalize_mx_phone(owner_phone)
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
    owner_phone_clean = _normalize_mx_phone(owner_phone)
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
    promo_code: Optional[str] = None

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
    bundle_size: Optional[int] = None

# ── NUEVO: schema para PATCH (todos los campos opcionales) ────────────────────
class PricebookItemUpdateBody(BaseModel):
    name: Optional[str] = None
    sku: Optional[str] = None
    unit: Optional[str] = None
    price: Optional[float] = None
    vat_rate: Optional[float] = None
    synonyms: Optional[str] = None
    bundle_size: Optional[int] = None
    is_default: Optional[bool] = None
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/")
def root():
    return {"ok": True, "service": "clawdbot-server"}

# -------------------------
# WhatsApp webhook receive
# -------------------------

_processed_msg_ids: dict = {}  # message_id → timestamp, simple dedup cache

def _dedup_cleanup():
    """Remove entries older than 5 minutes."""
    import time
    now = time.time()
    stale = [k for k, v in _processed_msg_ids.items() if now - v > 300]
    for k in stale:
        del _processed_msg_ids[k]

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

    # ── Deduplication: Meta retries webhooks if response is slow ─────────
    import time
    wa_msg_id = msg.get("id", "")
    if wa_msg_id:
        if wa_msg_id in _processed_msg_ids:
            print(f"DEDUP: skipping already-processed message {wa_msg_id}")
            return {"ok": True}
        _processed_msg_ids[wa_msg_id] = time.time()
        if len(_processed_msg_ids) > 500:
            _dedup_cleanup()

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
        print(f"INTERACTIVE MSG: type={itype} text={text!r} from={from_phone}")

    if text:
        log_message(company["company_id"], from_phone, "user", text)

    print(f"WEBHOOK DISPATCH: msg_type={msg_type} text={text!r} is_interactive={msg_type == 'interactive'}")
    reply = build_reply_for_company(
        company["company_id"], text,
        wa_from=from_phone,
        is_interactive=(msg_type == "interactive"),
    )
    print(f"WEBHOOK REPLY: type={type(reply).__name__} empty={not reply} preview={str(reply)[:120] if reply else 'NONE'}")

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

    elif isinstance(reply, dict) and reply.get("type") == "text_then_buttons":
        send_whatsapp_text(
            wa_api_key=company["wa_api_key"],
            phone_number_id=company["wa_phone_number_id"],
            to=from_phone,
            text=(reply.get("text") or "")[:4096],
        )
        btn_payload = {
            "messaging_product": "whatsapp",
            "to": from_phone,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {"text": reply.get("body") or "¿Qué deseas hacer?"},
                "action": {
                    "buttons": [
                        {"type": "reply", "reply": {"id": f"btn_{i}", "title": btn[:20]}}
                        for i, btn in enumerate(reply.get("buttons") or [])
                    ]
                }
            }
        }
        requests.post(
            f"https://graph.facebook.com/v19.0/{company['wa_phone_number_id']}/messages",
            headers={"Authorization": f"Bearer {company['wa_api_key']}", "Content-Type": "application/json"},
            json=btn_payload,
            timeout=20,
        )

    elif isinstance(reply, dict) and reply.get("type") == "text_then_list_sections":
        _pre_text = (reply.get("text") or "").strip()
        if _pre_text:
            send_whatsapp_text(
                wa_api_key=company["wa_api_key"],
                phone_number_id=company["wa_phone_number_id"],
                to=from_phone,
                text=_pre_text,
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


# ============================================================
# Parser shadow mode: corre LLM en paralelo y loguea para comparar
# ============================================================
_PARSER_SHADOW_ENABLED = os.environ.get("PARSER_SHADOW", "0") in ("1", "true", "True")
_PARSER_LLM_FIRST = os.environ.get("PARSER_LLM_FIRST", "0") in ("1", "true", "True")

def _load_catalog_for_shadow(company_id: str) -> list[dict]:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
            "WHERE company_id = %s ORDER BY name",
            (company_id,),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return [
            {"sku": r[0], "name": r[1], "unit": r[2],
             "price": float(r[3]) if r[3] is not None else None,
             "vat_rate": float(r[4]) if r[4] is not None else None}
            for r in rows
        ]
    except Exception as e:
        print("SHADOW: catalog load error:", repr(e))
        return []

def _try_llm_parse(company_id: str, user_text: str) -> dict | None:
    """Intenta parsear con LLM. Devuelve resultado o None si falla."""
    try:
        from llm_parser import llm_parse_order, norm_key
        catalog = _load_catalog_for_shadow(company_id)
        if not catalog:
            return None
        import time as _t
        t0 = _t.time()
        result = llm_parse_order(user_text, catalog)
        ms = int((_t.time() - t0) * 1000)
        print(f"LLM PARSE: {ms}ms, items={len(result.get('items', []))}, "
              f"non_order={result.get('non_order')}, error={result.get('error')}")
        if result.get("error"):
            print(f"LLM PARSE FAILED: {result['error']} — falling back to regex")
            return None
        # Attach catalog lookup dict for downstream use
        cat_by_key = {}
        for ci in catalog:
            k = norm_key(ci.get("name") or "")
            cat_by_key[k] = ci
        result["_catalog"] = catalog
        result["_cat_by_key"] = cat_by_key
        return result
    except Exception as e:
        print(f"LLM PARSE EXCEPTION: {repr(e)} — falling back to regex")
        return None

def log_parser_shadow(company_id, client_phone, user_text, regex_items):
    if not _PARSER_SHADOW_ENABLED:
        return
    if not user_text or len(user_text.strip()) < 2:
        return
    def _runner():
        try:
            from llm_parser import llm_parse_order
            catalog = _load_catalog_for_shadow(company_id)
            if not catalog:
                return
            import time as _t
            t0 = _t.time()
            result = llm_parse_order(user_text, catalog)
            latency_ms = int((_t.time() - t0) * 1000)
            regex_json = [{"qty": mi[0], "prod": mi[1]} for mi in (regex_items or []) if mi]
            llm_json = result.get("items") or []
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO parser_shadow_log
                    (company_id, client_phone, user_text, regex_items, llm_items,
                     llm_non_order, llm_error, llm_latency_ms, llm_model, created_at)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, now())
                """,
                (
                    company_id,
                    client_phone,
                    (user_text or "")[:4000],
                    json.dumps(regex_json),
                    json.dumps(llm_json),
                    bool(result.get("non_order")),
                    result.get("error"),
                    latency_ms,
                    result.get("model"),
                ),
            )
            cur.close()
            conn.close()
        except Exception as e:
            print("SHADOW: log error:", repr(e))
    try:
        import threading
        threading.Thread(target=_runner, daemon=True).start()
    except Exception as e:
        print("SHADOW: thread spawn error:", repr(e))


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

def _ceil_hundreds(n: int) -> int:
    return math.ceil(n / 100) * 100

def _calc_muro_tablaroca(alto: float, largo: float) -> list:
    m2 = alto * largo
    tablaroca = math.ceil(math.ceil(m2 / (1.22 * 2.44) * 2 * 1.03) * 1.03)
    pijas = _ceil_hundreds(math.ceil(tablaroca * 30))
    return [
        ("Tablaroca ultralight usg",          tablaroca),
        ("Canal 6.35 x 3.05 cal 26",          math.ceil((largo / 3) * 2)),
        ("Poste 6.35 x 3.05 cal 26",          (math.ceil(largo / 0.61) + 1) * (math.ceil(alto / 3.05) + 1)),
        ("Pija 6 x 1",                        pijas),
        ("Pija framer",                       _ceil_hundreds(math.ceil(pijas / 2))),
        ("Perfacinta",                        math.ceil((m2 / 2.44) / 20)),
        ("Redimix 21.8 kg usg",               math.ceil(m2 / 14)),
    ]


def _calc_muro_durock(alto: float, largo: float) -> list:
    m2 = alto * largo
    durock = math.ceil(math.ceil(m2 / (1.22 * 2.44) * 2 * 1.03) * 1.03)
    pijas = _ceil_hundreds(math.ceil(durock * 30))
    return [
        ("Durock usg",                        durock),
        ("Canal 6.35 x 3.05 cal 22",          math.ceil((largo / 3) * 2)),
        ("Poste 6.35 x 3.05 cal 20",          (math.ceil(largo / 0.406) + 1) * (math.ceil(alto / 3.05) + 1)),
        ("Pija para durock",                  pijas),
        ("Pija framer",                       _ceil_hundreds(math.ceil(pijas / 2))),
        ("Cinta fibra de vidrio",             math.ceil((m2 / 2.44) / 20)),
        ("Basecoat usg",                      math.ceil(m2 / 4)),
    ]


def _calc_plafon_tablaroca(largo: float, ancho: float) -> list:
    m2 = largo * ancho
    tablaroca = math.ceil(m2 / 2.9768 * 1.07)
    pijas = _ceil_hundreds(math.ceil(tablaroca * 30))
    return [
        ("Tablaroca ultralight usg",          tablaroca),
        ("Canal listón cal 26",               math.ceil(((m2 / 0.61) * 1.05) / 3.05) + 2),
        ("Canaleta de carga cal 24",          math.ceil(((m2 / 1.22) * 1.05) / 3.05)),
        ("Ángulo de amarre cal 26",           math.ceil(((largo * 2) + (ancho * 2)) / 3.05)),
        ("Pija 6 x 1",                        pijas),
        ("Pija framer",                       _ceil_hundreds(math.ceil(pijas / 2))),
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
    # Exclusiones explícitas: menú principal y otros módulos de calculadora
    if t in {"cotizar materiales", "🔨 cotizar materiales"}:
        return False
    _other_calc_keywords = ["rejacero", "reja", "pintura", "imper", "impermeabilizante", "calculadoras", "📐"]
    if any(k in t for k in _other_calc_keywords):
        return False
    triggers = [
        "calcula", "calcular",
        "construccion", "construcción", "construcion",
        "calcular material", "calcular materiales", "calcular m2", "calcular m", "🏗️ calcular m2",
        "muros y plafones", "🏗️ muros y plafones",
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
    
    detalle = "\n".join(lines)
    return {
        "type": "text_then_buttons",
        "text": detalle,
        "body": "¿Qué deseas hacer?",
        "buttons": ["🛒 Agregar productos", "💳 Pagar"],
    }
   
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

    # ── Check for button clicks BEFORE buffer flush ─────────────────
    # The message buffer could prepend stale text like "salor" to the
    # current "🗑️ Quitar producto" click, making the early trigger miss.
    # If the CURRENT message alone is a button click, skip buffer prepend.
    _raw_current = (user_text or "").strip().lower()
    _raw_current_stripped = re.sub(r"[^\w\s]", "", _raw_current).strip()
    _raw_current_stripped = re.sub(r"\s+", " ", _raw_current_stripped)
    # Strip accents for comparison (ubicación→ubicacion, más→mas, etc.)
    import unicodedata as _ud
    _raw_current_stripped = _ud.normalize("NFD", _raw_current_stripped)
    _raw_current_stripped = "".join(c for c in _raw_current_stripped if _ud.category(c) != "Mn")
    _button_click_triggers = {
        "quitar producto", "quitar productos", "quitar",
        "eliminar producto", "eliminar", "borrar producto", "remover", "remover producto",
        "pagar", "agregar mas", "agregar mas productos",
        "cotizar materiales", "salir", "nueva cotizacion",
        "hablar con alguien", "horarios y ubicacion",
    }
    _is_button_click = is_interactive and _raw_current_stripped in _button_click_triggers
    if is_interactive:
        print(f"BTN_DEBUG: raw_stripped={_raw_current_stripped!r} is_button_click={_is_button_click} in_triggers={_raw_current_stripped in _button_click_triggers}")

    # Flush stale message buffer (>15s old) — prepend to current message
    # SKIP if current message is a button click (prevents buffer from mangling it)
    if wa_from and not _is_button_click:
        import time as _tflush
        _flush_state = get_quote_state(company_id, wa_from) or {}
        _flush_buf = _flush_state.get("_msg_buffer")
        if _flush_buf:
            _flush_age = _tflush.time() - (_flush_buf.get("ts") or 0)
            if _flush_age > 15:
                _old = _flush_buf.get("texts") or []
                if _old:
                    user_text = " ".join(_old) + " " + user_text
                _flush_state.pop("_msg_buffer", None)
                upsert_quote_state(company_id, wa_from, _flush_state)
    elif wa_from and _is_button_click:
        # Clear the buffer silently — the button click should take priority
        _flush_state = get_quote_state(company_id, wa_from) or {}
        if _flush_state.get("_msg_buffer"):
            _flush_state.pop("_msg_buffer", None)
            upsert_quote_state(company_id, wa_from, _flush_state)
            print(f"BUTTON CLICK: cleared stale buffer for {wa_from}")

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

    def _classify_intent(text: str) -> str:
        """
        Clasifica si un mensaje es intención de cotizar productos o conversación general.
        Retorna 'product' o 'other'. Usa GPT-4o-mini para clasificación rápida.
        """
        t = (text or "").strip()
        if not t or len(t) < 3:
            return "other"
        try:
            from openai import OpenAI
            _oai_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
            if not _oai_key:
                return "product"  # fallback: asumir producto si no hay API key
            _oai = OpenAI(api_key=_oai_key)
            resp = _oai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": (
                        "Eres un clasificador de mensajes para un bot de cotización de materiales de construcción "
                        "(ferretería, acero, tablaroca, cemento, etc.) en México.\n\n"
                        "Clasifica si el mensaje del cliente es:\n"
                        "- PRODUCT: quiere cotizar, preguntar por un producto, material, herramienta o precio\n"
                        "- OTHER: conversación casual, preguntas personales, temas administrativos, saludos extendidos, "
                        "quejas, pagos, facturas, entregas, o cualquier cosa que NO sea pedir/cotizar un producto\n\n"
                        "Responde SOLO con: PRODUCT o OTHER"
                    )},
                    {"role": "user", "content": t},
                ],
                temperature=0.0,
                max_tokens=5,
            )
            result = (resp.choices[0].message.content or "").strip().upper()
            print(f"INTENT CLASSIFY: '{t[:50]}' → {result}")
            return "product" if "PRODUCT" in result else "other"
        except Exception as e:
            print(f"INTENT CLASSIFY ERROR: {repr(e)}")
            return "product"  # fallback seguro: asumir producto

    def _escalate_non_quote(company_id_esc: str, wa_from_esc: str, text_esc: str) -> str:
        """Escala un mensaje no-cotización al dueño y responde al cliente."""
        try:
            conn_esc = get_conn()
            cur_esc = conn_esc.cursor()
            cur_esc.execute(
                "SELECT owner_phone, wa_api_key, wa_phone_number_id, name FROM companies WHERE id=%s",
                (company_id_esc,),
            )
            row_esc = cur_esc.fetchone()
            cur_esc.close()
            conn_esc.close()
            company_name_esc = "la empresa"
            if row_esc and row_esc[0]:
                state_esc = get_quote_state(company_id_esc, wa_from_esc) or {}
                notify_owner_escalation(
                    wa_api_key=row_esc[1], phone_number_id=row_esc[2], owner_phone=row_esc[0],
                    client_phone=wa_from_esc,
                    reason=f"Mensaje no relacionado a cotización: \"{(text_esc or '')[:100]}\"",
                    state=state_esc,
                )
                company_name_esc = row_esc[3] or "la empresa"
        except Exception as e:
            print(f"ESCALATE NON-QUOTE ERROR: {repr(e)}")
            company_name_esc = "la empresa"
        return (
            f"Ese tema lo maneja directamente el equipo de *{company_name_esc}* 🙋\n\n"
            "Ya les avisé y te contactarán pronto.\n\n"
            "Si quieres cotizar materiales, mándame tu lista con cantidades 📋\n"
            "Ej: 10 cemento, 5 varilla 3/8"
        )

    def _is_greeting_like(tnorm: str) -> bool:
        t = (tnorm or "").strip()
        if not t:
            return False
        # Exact match: pure greetings only
        if t in {"hola", "buenas", "hey", "holi", "menu", "menú", "ayuda", "inicio",
                 "buen dia", "buen día", "buenos dias", "buenos días",
                 "buenas tardes", "buenas noches"}:
            return True
        # If the message contains product/order signals, it's NOT a pure greeting
        # e.g. "hola buenas tardes me podras cotizar 3 cajas de redimix"
        _order_signals = re.search(
            r"\b(cotiz|precio|cuanto|cuánto|mand|necesito|ocupo|quiero|dame|lista|material|"
            r"\d+\s+[a-záéíóúñ]{3,})\b",
            t, re.IGNORECASE
        )
        if _order_signals:
            return False
        if t.startswith("hola"):
            return True
        if t.startswith("buenos") or t.startswith("buenas") or t.startswith("buen"):
            return True
        return False

    def _build_reply_with_pending(state: dict, company_id: str = "", wa_from: str = ""):
        pending = state.get("pending") or []

        if pending:
            cart = state.get("cart") or []
            cart_count = len(cart)
            pending_count = len(pending)

            con_opciones = [p for p in pending if p.get("candidates")]
            sin_opciones = [p for p in pending if not p.get("candidates")]

            # ── Show ONE pending item at a time ──────────────────────
            if con_opciones:
                current = con_opciones[0]
                qty = int(current.get("qty") or 0)
                raw = (current.get("raw") or "").strip()
                cands = current.get("candidates") or []

                # Sort by price (cheapest first) as tiebreaker, then limit to 3
                # IMPORTANT: save sorted+truncated list back to state so pick handler
                # uses the same order as what was displayed to the user
                cands.sort(key=lambda x: float(x.get("price") or 999999))
                cands = cands[:3]
                current["candidates"] = cands  # sync state with displayed order

                # Persist AFTER candidates are sorted+truncated so pick handler
                # reads the same order that was displayed to the user
                if wa_from and company_id:
                    upsert_quote_state(company_id, wa_from, state)

                section_rows = []
                for j, it in enumerate(cands, start=1):
                    price = float(it.get("price") or 0.0)
                    unit = it.get("unit") or "unidad"
                    full_name = it["name"]
                    section_rows.append({
                        "id": f"pick_A{j}",
                        "title": f"{j}) ${price:,.0f}/{unit}"[:24],
                        "description": full_name[:72],
                    })

                # Add "Ninguno" skip option
                section_rows.append({
                    "id": "pick_A0",
                    "title": "❌ Ninguno",
                    "description": "No es ninguno, saltar este producto",
                })

                # Build contextual header
                resumen_lines = []
                if cart_count > 0:
                    resumen_lines.append(f"✅ *{cart_count}* producto(s) cotizados.")
                remaining = len(con_opciones) - 1
                if remaining > 0:
                    resumen_lines.append(f"📋 Quedan *{remaining}* más por confirmar después de este.")
                # Not-found items
                for p in sin_opciones:
                    nq = int(p.get("qty") or 0)
                    nr = (p.get("raw") or "").strip()
                    resumen_lines.append(f"❌ {nq}x {nr} — no encontrado")
                resumen_txt = "\n".join(resumen_lines) if resumen_lines else ""

                body_txt = f"*{qty}x {raw}*\n¿Cuál de estas opciones?"

                return {
                    "type": "text_then_list_sections",
                    "text": resumen_txt,
                    "body": body_txt[:1024],
                    "sections": [{"title": f"{raw}"[:24], "rows": section_rows[:10]}],
                    "button_label": "Ver opciones",
                }
            else:
                # Only not-found items remain — save state
                if wa_from and company_id:
                    upsert_quote_state(company_id, wa_from, state)
                lines = []
                if cart_count > 0:
                    lines.append(f"✅ Cotizamos *{cart_count} producto(s)* automáticamente.\n")
                for p in sin_opciones:
                    qty = int(p.get("qty") or 0)
                    raw = (p.get("raw") or "").strip()
                    lines.append(f"❌ *{qty}x {raw}* — no encontrado, escríbelo diferente")
                not_found_msg = "\n".join(lines)
                # If cart has items, show quote + action buttons
                if cart_count > 0:
                    quote_msg = cart_render_quote(state, company_id=company_id, client_phone=wa_from)
                    return {
                        "type": "text_then_buttons",
                        "text": not_found_msg + "\n\n" + quote_msg,
                        "body": "¿Qué deseas hacer?",
                        "buttons": ["💳 Pagar", "➕ Agregar más", "🗑️ Quitar producto"],
                    }
                return not_found_msg

        msg = cart_render_quote(state, company_id=company_id, client_phone=wa_from) if (state.get("cart") or []) else ""

        if wa_from and company_id:
            upsert_quote_state(company_id, wa_from, state)

        if state.get("cart"):
            return {
                "type": "text_then_buttons",
                "text": msg,
                "body": "¿Qué deseas hacer?",
                "buttons": ["💳 Pagar", "➕ Agregar más", "🗑️ Quitar producto"],
            }
    

        return {
            "type": "text_then_buttons",
            "text": msg if msg else "No encontré ninguno de los productos. Intenta con otros nombres.",
            "body": "¿Qué deseas hacer?",
            "buttons": ["➕ Agregar más", "🔄 Nueva cotización", "🚪 Salir"],
        }
    

    tnorm = norm_name(user_text).replace("cotización", "cotizacion")

    _edit_state = get_quote_state(company_id, wa_from) if wa_from else {}
    _edit_state = _edit_state or {}
    _cart = _edit_state.get("cart") or []

    # ── HIGH-PRIORITY: "Quitar producto" button handler ─────────────
    # This fires FIRST, before any state-dependent logic, to guarantee
    # the button always shows the removal list regardless of pending flags.
    _early_stripped = re.sub(r"[^\w\s]", "", tnorm).strip()
    _early_stripped = re.sub(r"\s+", " ", _early_stripped)
    _early_remove_triggers = {
        "quitar producto", "quitar productos", "quitar", "eliminar producto",
        "eliminar", "borrar producto", "remover", "remover producto",
        "quitar un producto", "quitar algo",
    }
    if _early_stripped in _early_remove_triggers:
        print(f"EARLY REMOVE TRIGGER: raw={user_text!r} tnorm={tnorm!r} stripped={_early_stripped!r}")
        if not _cart:
            return "Tu carrito está vacío."
        # Clear any stale removal-related flags so we re-render the list fresh
        _edit_state.pop("awaiting_removal_qty", None)
        _edit_state["awaiting_removal"] = True
        if wa_from:
            upsert_quote_state(company_id, wa_from, _edit_state)
        _removal_rows = []
        for _ri, _item in enumerate(_cart[:9]):
            _rname = (_item.get("name") or "Producto")
            _rqty = int(_item.get("qty") or 0)
            _removal_rows.append({
                "id": f"remove_{_ri}",
                "title": _rname[:24],
                "description": f"{_rqty}x — quitar este"[:72],
            })
        _removal_rows.append({
            "id": "remove_cancel",
            "title": "❌ Cancelar",
            "description": "No quitar nada",
        })
        _body_msg = "¿Cuál producto quieres quitar?"
        if len(_cart) > 9:
            _body_msg += f"\n\n(Mostrando 9 de {len(_cart)}. O escribe el nombre directo.)"
        return {
            "type": "list",
            "body": _body_msg,
            "sections": [{"title": "Tu carrito", "rows": _removal_rows}],
            "button_label": "Ver productos",
        }

    ver_triggers = {"ver carrito", "mi carrito", "que llevo", "qué llevo", "ver pedido", "mi pedido"}
    if tnorm in ver_triggers:
        if not _cart:
            return "Tu carrito está vacío. Mándame tu pedido, ej: 10 cemento, 5 varilla 3/8"
        return cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from)

    # Si estamos esperando la cantidad a quitar
    if _edit_state.get("awaiting_removal_qty"):
        _removal_name = _edit_state.pop("awaiting_removal_qty")
        _item_match = next((it for it in _cart if (it.get("name") or "").lower() == _removal_name.lower()), None)
        if _item_match:
            _current_qty = int(_item_match.get("qty") or 0)
            if tnorm in {"todas", "todo", "todos", "all"}:
                _cart = [it for it in _cart if it != _item_match]
                _edit_state["cart"] = _cart
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _edit_state)
                if not _cart:
                    return f"✅ Eliminé *{_item_match['name']}*. Tu carrito quedó vacío."
                return cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from) + "\n\n¿Agregamos o quitamos algo más?"
            _qty_num = re.match(r"^(\d+)", tnorm.strip())
            if _qty_num:
                _remove_n = int(_qty_num.group(1))
                if _remove_n >= _current_qty:
                    _cart = [it for it in _cart if it != _item_match]
                    _edit_state["cart"] = _cart
                else:
                    _item_match["qty"] = _current_qty - _remove_n
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _edit_state)
                if not _edit_state.get("cart"):
                    return f"✅ Eliminé *{_item_match['name']}*. Tu carrito quedó vacío."
                remaining = _item_match.get("qty", 0) if _remove_n < _current_qty else 0
                msg = cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from)
                if remaining:
                    return msg + f"\n\n✅ Quité {_remove_n} de *{_item_match['name']}* (quedan {remaining})"
                return msg + "\n\n¿Agregamos o quitamos algo más?"
        if wa_from:
            upsert_quote_state(company_id, wa_from, _edit_state)

    # Pre-check: si el usuario vuelve a hacer click en "Quitar producto" (con o sin emoji),
    # tratamos como un nuevo inicio del flujo aunque awaiting_removal ya esté activo.
    # Así evitamos caer en la rama de "buscar '🗑️ quitar producto' en el carrito" que no matchea.
    _tnorm_stripped_pre = re.sub(r"[^\w\s]", "", tnorm).strip()
    _tnorm_stripped_pre = re.sub(r"\s+", " ", _tnorm_stripped_pre)
    _remove_triggers_pre = {
        "quitar producto", "quitar productos", "quitar", "eliminar producto",
        "eliminar", "remover", "remover producto",
        "quitar un producto", "quitar algo",
    }
    if _tnorm_stripped_pre in _remove_triggers_pre:
        # Limpia estado viejo y deja que el flujo normal más abajo muestre la lista
        _edit_state.pop("awaiting_removal", None)
        _edit_state.pop("awaiting_removal_qty", None)
        if wa_from:
            upsert_quote_state(company_id, wa_from, _edit_state)

    # Si estamos esperando que el usuario seleccione qué quitar
    if _edit_state.get("awaiting_removal"):
        _edit_state.pop("awaiting_removal", None)
        # Cancel removal if user picks the cancel list option or says cancel/salir/no
        if tnorm.strip() == "remove_cancel" or tnorm.strip() in {"cancelar", "cancel", "no", "salir", "nada", "ninguno", "ninguna", "ya no", "dejalo", "déjalo", "❌ cancelar"}:
            if wa_from:
                upsert_quote_state(company_id, wa_from, _edit_state)
            _quote = cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from)
            return {
                "type": "text_then_buttons",
                "text": _quote + "\n\n👌 Listo, no quité nada.",
                "body": "¿Qué deseas hacer?",
                "buttons": ["💳 Pagar", "➕ Agregar más", "🗑️ Quitar producto"],
            }
        # Handle interactive list selection (remove_0, remove_1, etc.)
        _rm_match = re.match(r"^remove_(\d+)$", tnorm.strip())
        if _rm_match:
            _rm_idx = int(_rm_match.group(1))
            if 0 <= _rm_idx < len(_cart):
                _removed = _cart.pop(_rm_idx)
                _edit_state["cart"] = _cart
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _edit_state)
                if not _cart:
                    return {
                        "type": "text_then_buttons",
                        "text": f"✅ Eliminé *{_removed['name']}*. Tu carrito quedó vacío.",
                        "body": "¿Qué deseas hacer?",
                        "buttons": ["🔨 Cotizar materiales", "🚪 Salir"],
                    }
                _quote = cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from)
                return {
                    "type": "text_then_buttons",
                    "text": _quote + f"\n\n✅ Quité *{_removed['name']}*",
                    "body": "¿Qué deseas hacer?",
                    "buttons": ["💳 Pagar", "➕ Agregar más", "🗑️ Quitar producto"],
                }
        if wa_from:
            upsert_quote_state(company_id, wa_from, _edit_state)
        # Si no escribió "quitar X", agregamos el prefijo para que lo procese la regex
        if not re.match(r"^(quitar|eliminar|borrar|sacar)\s+", tnorm):
            tnorm = f"quitar {tnorm}"

    _quitar_match = re.match(r"^(quitar|eliminar|borrar|sacar)\s+(.+)$", tnorm)
    if _quitar_match:
        _prod_raw = _quitar_match.group(2).strip()
        # Detectar cantidad: "quitar 3 durock" → qty=3, prod="durock"
        _qty_match = re.match(r"^(\d+)\s+(.+)$", _prod_raw)
        _remove_qty = int(_qty_match.group(1)) if _qty_match else None
        _prod_query = _qty_match.group(2).strip() if _qty_match else _prod_raw
        if not _cart:
            return "Tu carrito está vacío."
        _matches = [it for it in _cart if _prod_query in norm_name(it.get("name", "")).lower()]
        if not _matches:
            _matches = [it for it in _cart if any(tok in norm_name(it.get("name", "")).lower() for tok in _prod_query.split() if len(tok) >= 3)]
        if len(_matches) == 1:
            _item = _matches[0]
            _current_qty = int(_item.get("qty") or 0)
            if _remove_qty and _remove_qty < _current_qty:
                # Quitar parcial: reducir cantidad
                _item["qty"] = _current_qty - _remove_qty
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _edit_state)
                return cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from) + f"\n\n✅ Quité {_remove_qty} de *{_item['name']}* (quedan {_item['qty']})"
            elif _remove_qty and _remove_qty >= _current_qty:
                # Quitar todo (pidió igual o más de lo que tiene)
                _cart = [it for it in _cart if it != _item]
                _edit_state["cart"] = _cart
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _edit_state)
                if not _cart:
                    return f"✅ Eliminé *{_item['name']}*. Tu carrito quedó vacío."
                return cart_render_quote(_edit_state, company_id=company_id, client_phone=wa_from) + "\n\n¿Agregamos o quitamos algo más?"
            elif _current_qty > 1:
                # No especificó cantidad y tiene más de 1 → preguntar
                _edit_state["awaiting_removal_qty"] = _item["name"]
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _edit_state)
                return f"Tienes *{_current_qty}* de *{_item['name']}*.\n¿Cuántas quieres quitar? (o escribe *todas*)"
            else:
                # Solo tiene 1 → quitar directo
                _cart = [it for it in _cart if it != _item]
                _edit_state["cart"] = _cart
                if wa_from:
                    upsert_quote_state(company_id, wa_from, _edit_state)
                if not _cart:
                    return f"✅ Eliminé *{_item['name']}*. Tu carrito quedó vacío."
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
        "🚪 salir", "🔄 nueva cotizacion", "🔄 nueva cotización",
        "volver al menu", "volver al menú", "⬅️ volver al menu", "⬅️ volver al menú",
    }
    if any(rt == tnorm or rt in tnorm for rt in reset_triggers):
        if wa_from:
            clear_quote_state(company_id, wa_from)
        try:
            conn_co = get_conn()
            cur_co = conn_co.cursor()
            cur_co.execute("SELECT name, construccion_ligera_enabled, rejacero_enabled, pintura_enabled, impermeabilizante_enabled, welcome_message FROM companies WHERE id=%s LIMIT 1", (company_id,))
            row_co = cur_co.fetchone()
            cur_co.close()
            conn_co.close()
            company_name = row_co[0] if row_co else "tu ferretería"
            _mod_cl = bool(row_co[1]) if row_co else False
            _mod_rj = bool(row_co[2]) if row_co else False
            _mod_pt = bool(row_co[3]) if row_co else False
            _mod_im = bool(row_co[4]) if row_co else False
            _welcome = (row_co[5] or "").strip() if row_co else ""
        except Exception:
            company_name = "tu ferretería"
            _mod_cl = False
            _mod_rj = False
            _mod_pt = False
            _mod_im = False
            _welcome = ""
        _menu_opts = ["🔨 Cotizar materiales"]
        _any_calc = _mod_cl or _mod_rj or _mod_pt or _mod_im
        if _any_calc:
            _menu_opts.append("📐 Cotizar cálculo")
        _menu_opts.extend(["🕐 Horarios y ubicación", "👤 Hablar con alguien"])
        _greeting = _welcome if _welcome else f"👋 ¡Hola! Soy el asistente de *{company_name}*\n\n¿En qué te puedo ayudar?"
        return {
            "type": "list",
            "body": _greeting,
            "options": _menu_opts,
            "button_label": "Ver opciones",
        }

    thanks_triggers = {"gracias", "muchas gracias", "mil gracias", "thx", "thanks"}
    if tnorm in thanks_triggers:
        return {
            "type": "text_then_buttons",
            "text": "¡Con gusto! 🙌",
            "body": "¿Necesitas algo más?",
            "buttons": ["🔨 Cotizar materiales", "🕐 Horarios y ubicación", "👤 Hablar con alguien"],
        }

    # Detect "Quitar producto" button with or without emoji/variations
    _tnorm_stripped = re.sub(r"[^\w\s]", "", tnorm).strip()
    _tnorm_stripped = re.sub(r"\s+", " ", _tnorm_stripped)
    _remove_triggers = {
        "quitar producto", "quitar productos", "quitar", "eliminar producto",
        "eliminar", "borrar producto", "borrar", "remover", "remover producto",
        "quitar un producto", "quitar algo",
    }
    if _tnorm_stripped in _remove_triggers:
        _cart_q = (_edit_state.get("cart") or [])
        if not _cart_q:
            return "Tu carrito está vacío."
        _edit_state["awaiting_removal"] = True
        if wa_from:
            upsert_quote_state(company_id, wa_from, _edit_state)
        # Show as interactive list so user doesn't have to type.
        # WhatsApp list rows limits: title ≤24 chars, description ≤72 chars.
        _removal_rows = []
        for _ri, _item in enumerate(_cart_q[:9]):  # leave room for "cancel" entry
            _rname = (_item.get("name") or "Producto")
            _rqty = int(_item.get("qty") or 0)
            _removal_rows.append({
                "id": f"remove_{_ri}",
                "title": _rname[:24],
                "description": f"{_rqty}x — quitar este"[:72],
            })
        # Add a cancel option at the end
        _removal_rows.append({
            "id": "remove_cancel",
            "title": "❌ Cancelar",
            "description": "No quitar nada",
        })
        # If there are many items, add a hint about typing
        _body_msg = "¿Cuál producto quieres quitar?"
        if len(_cart_q) > 9:
            _body_msg += f"\n\n(Mostrando 9 de {len(_cart_q)}. O escribe el nombre directo.)"
        return {
            "type": "list",
            "body": _body_msg,
            "sections": [{"title": "Tu carrito", "rows": _removal_rows}],
            "button_label": "Ver productos",
        }
    escalation_triggers = {
        "asesor", "asesor humano", "humano", "persona", "agente",
        "hablar con alguien", "hablar con una persona", "quiero hablar",
        "necesito ayuda", "ayuda humana",
        # Variantes comunes en México — "hablar con X" patterns
        "hablar con ejecutivo", "hablar con un ejecutivo", "quiero un ejecutivo",
        "hablar con representante", "hablar con un representante",
        "hablar con vendedor", "hablar con un vendedor", "quiero un vendedor",
        "hablar con encargado", "hablar con el encargado",
        "hablar con operador", "hablar con gerente",
        "hablar con atencion", "hablar con atención",
        # Direct words (standalone or at end of sentence)
        "ejecutivo", "representante",
        "atencion a cliente", "atención a cliente", "servicio a cliente",
        "necesito asesor", "quiero asesor",
    }
    # Frases de frustración — si cliente las envía mientras hay un pending/awaiting state,
    # escalamos automáticamente en vez de seguir en loop de desambiguación.
    _frustration_phrases = {
        "me pueden apoyar", "me puedes apoyar", "pueden apoyar", "apoyenme", "apóyenme",
        "ayudenme", "ayúdenme", "ayudame", "ayúdame", "necesito ayuda",
        "no entiendo", "no le entiendo", "no entendi", "no entendí",
        "no se", "no sé", "no me explico", "esta complicado", "está complicado",
        "muy complicado", "muy dificil", "muy difícil", "no funciona",
        "mejor hablen", "mejor llamen", "que me llamen", "que alguien me llame",
        "me pueden ayudar", "me puedes ayudar",
    }
    # ── Detección de intención NO-cotización ──────────────────────────────
    # Mensajes que claramente no son pedidos de productos → escalar a humano
    _non_quote_keywords = [
        "factura", "facturas", "facturar", "facturacion", "facturación",
        "pago", "pagos", "pagué", "pague", "transferencia", "deposito", "depósito",
        "comprobante", "recibo",
        "conciliacion", "conciliación",
        "entrega", "entregas", "envio", "envío", "enviar", "mandaron", "llegó", "llego",
        "pedido", "mi pedido", "orden", "mi orden", "status", "estatus",
        "reclamo", "queja", "problema", "error", "devolucion", "devolución", "cambio",
        "garantia", "garantía",
        "correo", "email", "mail",
        "credito", "crédito", "saldo", "adeudo", "deuda", "debo", "deben",
        "cuenta", "estado de cuenta",
        "vendedor", "encargado", "dueño", "gerente", "jefe",
        "llamar", "llamada", "telefono", "teléfono", "cel", "celular",
        "visita", "visitarlos", "dirección", "donde estan", "dónde están",
        "abierto", "abren", "cierran", "horario",
    ]
    _t_lower = user_text.lower().strip()
    # No interceptar si el mensaje tiene números con productos (ej: "10 cemento y factura")
    _has_qty = bool(re.search(r"\b\d+\s+[a-záéíóúñü]", _t_lower))
    # No interceptar preguntas legítimas de horario/ubicación — tienen su handler propio
    _is_hours_question = looks_like_hours_question(user_text)
    if not _has_qty and not _is_hours_question and any(kw in _t_lower for kw in _non_quote_keywords):
        # Verificar que no sea un saludo simple o que ya esté en escalation_triggers
        _is_escalation_kw = any(rt == tnorm or rt in tnorm for rt in escalation_triggers)
        if not _is_escalation_kw:
            state_esc = get_quote_state(company_id, wa_from) if wa_from else {}
            state_esc = state_esc or {}
            try:
                conn_esc = get_conn()
                cur_esc = conn_esc.cursor()
                cur_esc.execute("SELECT owner_phone, wa_api_key, wa_phone_number_id, name, telefono_atencion FROM companies WHERE id=%s", (company_id,))
                row_esc = cur_esc.fetchone()
                cur_esc.close()
                conn_esc.close()
                _nq_phone = (row_esc[4] or row_esc[0] or "").strip() if row_esc else ""
                if _nq_phone and row_esc[1] and row_esc[2]:
                    try:
                        notify_owner_escalation(
                            wa_api_key=row_esc[1], phone_number_id=row_esc[2], owner_phone=_nq_phone,
                            client_phone=wa_from,
                            reason=f"Mensaje no relacionado a cotización: \"{user_text[:100]}\"",
                            state=state_esc,
                        )
                    except Exception as ne:
                        print("NON-QUOTE NOTIFY ERROR:", repr(ne))
                company_name_esc = (row_esc[3] if row_esc else None) or "la empresa"
            except Exception as e:
                print("NON-QUOTE ESCALATION ERROR:", repr(e))
                company_name_esc = "la empresa"
                _nq_phone = ""
            if _nq_phone:
                _nq_clean = _normalize_mx_phone(_nq_phone)
                return (
                    f"Ese tema lo maneja directamente el equipo de *{company_name_esc}* 🙋\n\n"
                    f"Manda mensaje a:\n👉 https://wa.me/{_nq_clean}\n\n"
                    "Si quieres cotizar materiales, mándame tu lista con cantidades 📋"
                )
            return (
                f"Ese tema lo maneja directamente el equipo de *{company_name_esc}* 🙋\n\n"
                "Ya les avisé y te contactarán pronto.\n\n"
                "Si quieres cotizar materiales mientras tanto, mándame tu lista con cantidades 📋"
            )

    # ── Detección de frustración proactiva ────────────────────────────────
    # Si el cliente manda frases de frustración MIENTRAS hay un pending/awaiting state
    # o items sin resolver en el carrito, escalamos a humano en vez de seguir el loop.
    _is_frustrated = any(fp in _t_lower for fp in _frustration_phrases)
    if _is_frustrated and wa_from:
        _state_frust = get_quote_state(company_id, wa_from) or {}
        _has_pending = bool(
            _state_frust.get("pending")
            or _state_frust.get("awaiting")
            or _state_frust.get("awaiting_removal")
            or _state_frust.get("pending_ambiguous")
            or (_state_frust.get("items") and len(_state_frust.get("items", [])) > 0)
        )
        if _has_pending:
            print(f"FRUSTRATION DETECTED: escalating '{_t_lower}' (pending state exists)")
            # Force into escalation branch below
            tnorm = "asesor"

    if any(rt == tnorm or rt in tnorm for rt in escalation_triggers) or (_is_frustrated and tnorm == "asesor"):
        state = get_quote_state(company_id, wa_from) if wa_from else {}
        state = state or {}
        _atencion_phone = None
        _company_name_esc = "la empresa"
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT owner_phone, wa_api_key, wa_phone_number_id, telefono_atencion, name FROM companies WHERE id=%s", (company_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            _atencion_phone = (row[3] or row[0] or "").strip() if row else ""
            _company_name_esc = row[4] or "la empresa" if row else "la empresa"
            # Notify owner/attention phone via WhatsApp
            _notify_phone = (row[3] or row[0] or "").strip() if row else ""
            if _notify_phone and row[1] and row[2]:
                try:
                    notify_owner_escalation(
                        wa_api_key=row[1], phone_number_id=row[2], owner_phone=_notify_phone,
                        client_phone=wa_from, reason="Cliente solicitó hablar con un asesor", state=state,
                    )
                except Exception as ne:
                    print("ESCALATION NOTIFY ERROR:", repr(ne))
        except Exception as e:
            print("ESCALATION ERROR:", repr(e))
        # Build response with wa.me link if phone is available
        if _atencion_phone:
            _phone_clean = _normalize_mx_phone(_atencion_phone)
            return (
                f"Para atención personalizada manda mensaje a:\n"
                f"👉 https://wa.me/{_phone_clean}\n\n"
                f"El equipo de *{_company_name_esc}* te atenderá lo más rápido posible 🙏"
            )
        return (
            "Un asesor te contactará pronto 🙏\n\n"
            "Mientras tanto puedes seguir agregando productos "
            "o esperar a que te contacten."
        )

    # Opción "Cotizar cálculo" — submenú dinámico según módulos habilitados
    if tnorm in {"calculadoras", "📐 calculadoras", "calculadora", "cotizar calculo", "📐 cotizar calculo", "📐 cotizar cálculo", "cotizar cálculo"}:
        try:
            conn_calc = get_conn()
            cur_calc = conn_calc.cursor()
            cur_calc.execute("SELECT construccion_ligera_enabled, rejacero_enabled, pintura_enabled, impermeabilizante_enabled FROM companies WHERE id=%s", (company_id,))
            row_calc = cur_calc.fetchone()
            cur_calc.close()
            conn_calc.close()
        except Exception:
            row_calc = None
        _calc_opts = []
        if row_calc:
            if bool(row_calc[0]):
                _calc_opts.append("🏗️ Muros y plafones")
            if bool(row_calc[1]):
                _calc_opts.append("🧱 Calcular rejacero")
            if bool(row_calc[2]):
                _calc_opts.append("🎨 Calcular pintura")
            if bool(row_calc[3]):
                _calc_opts.append("🛡️ Calcular imper")
        if not _calc_opts:
            return "No hay calculadoras habilitadas en este momento."
        _calc_opts.append("⬅️ Volver al menú")
        return {
            "type": "list",
            "body": "📐 *Cálculos disponibles*\n\nSelecciona el tipo de cálculo que necesitas:",
            "options": _calc_opts,
            "button_label": "Ver opciones",
        }

    # Opción "Cotizar materiales" / "Agregar más" del menú principal
    if tnorm in {"cotizar materiales", "🔨 cotizar materiales", "agregar mas", "➕ agregar mas", "➕ agregar más"}:
        try:
            conn_wh = get_conn()
            cur_wh = conn_wh.cursor()
            cur_wh.execute("SELECT welcome_products_hint FROM companies WHERE id=%s", (company_id,))
            row_wh = cur_wh.fetchone()
            cur_wh.close()
            conn_wh.close()
            hint = (row_wh[0] or "").strip() if row_wh else ""
        except Exception:
            hint = ""
        if hint:
            ejemplos = "\n".join(f"10 {p.strip()}" for p in hint.split(",") if p.strip())
            hint_txt = f"\n\nEj:\n{ejemplos}"
        else:
            hint_txt = "\n\nEj:\n10 cemento\n5 varilla 3/8\n20 block 15x20"
        return f"📋 Mándame tu lista de materiales con cantidades:{hint_txt}\n\nO todo en una línea separado por comas."
    if _is_greeting_like(tnorm):
        try:
            conn_co = get_conn()
            cur_co = conn_co.cursor()
            cur_co.execute("SELECT name, construccion_ligera_enabled, rejacero_enabled, pintura_enabled, impermeabilizante_enabled, welcome_message FROM companies WHERE id=%s LIMIT 1", (company_id,))
            row_co = cur_co.fetchone()
            cur_co.close()
            conn_co.close()
            company_name = row_co[0] if row_co else "tu ferretería"
            _mod_cl2 = bool(row_co[1]) if row_co else False
            _mod_rj2 = bool(row_co[2]) if row_co else False
            _mod_pt2 = bool(row_co[3]) if row_co else False
            _mod_im2 = bool(row_co[4]) if row_co else False
            _welcome2 = (row_co[5] or "").strip() if row_co else ""
        except Exception:
            company_name = "tu ferretería"
            _mod_cl2 = False
            _mod_rj2 = False
            _mod_pt2 = False
            _mod_im2 = False
            _welcome2 = ""

        if wa_from:
            st = get_quote_state(company_id, wa_from) or {}
            has_pending = bool(st.get("pending"))
            has_cart = bool(st.get("cart"))
            if has_pending:
                return _build_reply_with_pending(st, company_id=company_id, wa_from=wa_from)
            if has_cart:
                clear_quote_state(company_id, wa_from)
        _menu_opts2 = ["🔨 Cotizar materiales"]
        _any_calc2 = _mod_cl2 or _mod_rj2 or _mod_pt2 or _mod_im2
        if _any_calc2:
            _menu_opts2.append("📐 Cotizar cálculo")
        _menu_opts2.extend(["🕐 Horarios y ubicación", "👤 Hablar con alguien"])
        _greeting2 = _welcome2 if _welcome2 else f"👋 ¡Hola! Soy el asistente de *{company_name}*\n\n¿En qué te puedo ayudar?"
        return {
            "type": "list",
            "body": _greeting2,
            "options": _menu_opts2,
            "button_label": "Ver opciones",
        }

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
                    _spec_bun = current.get("is_bundle", False)
                    _spec_fq = _resolve_bundle_qty(current["qty"], _spec_bun, result["item"])
                    _state_specs = cart_add_item(_state_specs, {
                        "sku": result["item"].get("sku"),
                        "name": result["item"].get("name"),
                        "unit": result["item"].get("unit") or "unidad",
                        "price": float(result["item"].get("price") or 0.0),
                        "vat_rate": result["item"].get("vat_rate"),
                        "qty": _spec_fq,
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
                if tnorm in {"si", "sí", "yes", "s", "dale", "va", "ok", "listo", "cotiza", "cotizar", "agregar productos", "🛒 agregar productos"}:                
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
            _cs_state.pop("construccion_state", None)
            upsert_quote_state(company_id, wa_from, _cs_state)
            return _handle_construccion(company_id, user_text, wa_from)
        
    # ── Rejacero calculator ─────────────────────────────────────────────────
    try:
        conn_rj = get_conn()
        cur_rj = conn_rj.cursor()
        cur_rj.execute("SELECT rejacero_enabled FROM companies WHERE id=%s", (company_id,))
        row_rj = cur_rj.fetchone()
        cur_rj.close()
        conn_rj.close()
        _rj_enabled = bool(row_rj[0]) if row_rj else False
    except Exception:
        _rj_enabled = False

    if _rj_enabled:
        _rj_state = get_quote_state(company_id, wa_from) if wa_from else {}
        _rj_state = _rj_state or {}

        # Step flow: rejacero_state.step = "metros" | "altura"
        if _rj_state.get("rejacero_state"):
            rjs = _rj_state["rejacero_state"]

            if rjs.get("step") == "metros":
                # Expecting meters number
                _m_num = re.match(r"^\s*(\d+(?:\.\d+)?)\s*", user_text.strip())
                if _m_num:
                    metros = float(_m_num.group(1))
                    rjs["metros"] = metros
                    rjs["step"] = "altura"
                    _rj_state["rejacero_state"] = rjs
                    upsert_quote_state(company_id, wa_from, _rj_state)
                    return {
                        "type": "list",
                        "body": f"📏 *{metros:.0f} metros lineales*. ¿Cuál es la altura de la reja?",
                        "options": ["1.00 m", "1.50 m", "2.00 m", "2.50 m"],
                        "button_label": "Elegir altura",
                    }
                else:
                    return "Necesito un número. ¿Cuántos metros lineales de reja? (ej: 25)"

            elif rjs.get("step") == "altura":
                # Parse height from selection or text
                _h_match = re.search(r"(\d+(?:\.\d+)?)", user_text.strip())
                if _h_match:
                    altura = float(_h_match.group(1))
                    metros = rjs.get("metros", 0)

                    # Calculate
                    import math
                    rejas = math.ceil(metros / 2.50)
                    postes = rejas + 1
                    # Abrazaderas per post based on height
                    if altura >= 2.50:
                        abr_per_post = 5
                    elif altura >= 2.00:
                        abr_per_post = 4
                    elif altura >= 1.50:
                        abr_per_post = 3
                    else:
                        abr_per_post = 2
                    abrazaderas = postes * abr_per_post

                    resultado = (
                        f"🧱 *Cálculo de rejacero*\n"
                        f"━━━━━━━━━━━━━━━━━━━\n"
                        f"📏 Metros lineales: *{metros:.0f} m*\n"
                        f"📐 Altura: *{altura:.2f} m*\n\n"
                        f"🔩 Rejas: *{rejas}*\n"
                        f"📍 Postes: *{postes}*\n"
                        f"🔗 Abrazaderas: *{abrazaderas}* ({abr_per_post} por poste)\n"
                        f"━━━━━━━━━━━━━━━━━━━"
                    )

                    # Poste height is always 0.50m taller than reja
                    _altura_poste_map = {1.0: 1.50, 1.50: 2.00, 2.0: 2.50, 2.50: 3.00}
                    _altura_poste = _altura_poste_map.get(altura, altura + 0.50)

                    # Build material list — map altura to exact catalog names
                    _altura_str = f"{altura:.2f}".rstrip("0").rstrip(".")  # 1.00→1, 1.50→1.5, 2.00→2
                    _reja_name_map = {
                        "1": "Rejacero 1 metro x 2.50 m",
                        "1.5": "Rejacero 1.50 metro x 2.50 m",
                        "2": "Rejacero 2 metro x 2.50 m",
                        "2.5": "Rejacero 2.50 metro x 2.50 m",
                    }
                    _poste_name_map = {
                        "1": "Poste 1.50 m para rejacero",
                        "1.5": "Poste 2.00 m para rejacero",
                        "2": "Poste 2.50 m para rejacero",
                        "2.5": "Poste 3.10 m para rejacero",
                    }
                    _mat_items = [
                        (rejas, _reja_name_map.get(_altura_str, f"Rejacero {_altura_str} metro x 2.50 m")),
                        (postes, _poste_name_map.get(_altura_str, f"Poste {_altura_poste:.2f} m para rejacero")),
                        (abrazaderas, "Abrazadera para rejacero"),
                    ]

                    state = _rj_state
                    conn = get_conn()
                    try:
                        cur_rj2 = conn.cursor()
                        for _lqty, _lname in _mat_items:
                            # Try exact name match first (fast, no ambiguity)
                            cur_rj2.execute(
                                "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                "WHERE company_id=%s AND lower(name)=lower(%s) LIMIT 1",
                                (company_id, _lname),
                            )
                            row_rj = cur_rj2.fetchone()
                            if row_rj:
                                state = cart_add_item(state, {
                                    "sku": row_rj[0], "name": row_rj[1],
                                    "unit": row_rj[2] or "pza",
                                    "price": float(row_rj[3] or 0),
                                    "vat_rate": row_rj[4],
                                    "qty": _lqty,
                                })
                            else:
                                # Fallback to smart_search if exact name not found
                                result = smart_search(conn, company_id, _lname, _lqty,
                                                      cart_context=_build_cart_context(state))
                                if result["status"] == "found":
                                    state = cart_add_item(state, {
                                        "sku": result["item"].get("sku"),
                                        "name": result["item"].get("name"),
                                        "unit": result["item"].get("unit") or "pza",
                                        "price": float(result["item"].get("price") or 0.0),
                                        "vat_rate": result["item"].get("vat_rate"),
                                        "qty": _lqty,
                                    })
                                elif result.get("candidates"):
                                    pend = state.get("pending") or []
                                    pend.append({"qty": _lqty, "raw": _lname, "candidates": result["candidates"]})
                                    state["pending"] = pend
                        cur_rj2.close()
                    finally:
                        conn.close()
                    state.pop("rejacero_state", None)
                    upsert_quote_state(company_id, wa_from, state)

                    # Send cart/pending reply first, then desglose at end
                    _reply = _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
                    if isinstance(_reply, dict):
                        _reply["text"] = (_reply.get("text") or "") + "\n\n" + resultado
                    else:
                        _reply = str(_reply) + "\n\n" + resultado
                    return _reply
                else:
                    return {
                        "type": "list",
                        "body": "No entendí la altura. Elige una opción:",
                        "options": ["1.00 m", "1.50 m", "2.00 m", "2.50 m"],
                        "button_label": "Elegir altura",
                    }

        # Trigger rejacero calculator
        _rj_triggers = {"calcular rejacero", "🧱 calcular rejacero", "rejacero", "reja ciclonica",
                        "reja ciclónica", "calcular reja", "calcular rejas"}
        if tnorm in _rj_triggers:
            _rj_state["rejacero_state"] = {"step": "metros"}
            upsert_quote_state(company_id, wa_from, _rj_state)
            return "📏 ¿Cuántos metros lineales de reja necesitas? (ej: 25)"

    # ── Helper: desglose litros → cubetas + galones + litros ──────────────
    def _desglose_litros(litros_total):
        """Desglosa litros en cubetas (19L), galones (3.785L), litros sueltos."""
        import math
        cubetas = int(litros_total // 19)
        resto = litros_total - (cubetas * 19)
        galones = int(resto // 3.785)
        resto2 = resto - (galones * 3.785)
        litros = math.ceil(resto2) if resto2 > 0.1 else 0
        return cubetas, galones, litros

    def _desglose_texto(cubetas, galones, litros):
        parts = []
        if cubetas > 0:
            parts.append(f"*{cubetas}* cubeta{'s' if cubetas > 1 else ''} (19L)")
        if galones > 0:
            parts.append(f"*{galones}* galón{'es' if galones > 1 else ''} (3.785L)")
        if litros > 0:
            parts.append(f"*{litros}* litro{'s' if litros > 1 else ''}")
        return " + ".join(parts) if parts else "*0*"

    # ── Pintura calculator ───────────────────────────────────────────────────
    try:
        conn_pt = get_conn()
        cur_pt = conn_pt.cursor()
        cur_pt.execute("SELECT pintura_enabled FROM companies WHERE id=%s", (company_id,))
        row_pt = cur_pt.fetchone()
        cur_pt.close()
        conn_pt.close()
        _pt_enabled = bool(row_pt[0]) if row_pt else False
    except Exception:
        _pt_enabled = False

    if _pt_enabled:
        _pt_state = get_quote_state(company_id, wa_from) if wa_from else {}
        _pt_state = _pt_state or {}

        if _pt_state.get("pintura_state"):
            pts = _pt_state["pintura_state"]

            if pts.get("step") == "m2":
                _m_num = re.match(r"^\s*(\d+(?:\.\d+)?)\s*", user_text.strip())
                if _m_num:
                    m2 = float(_m_num.group(1))
                    pts["m2"] = m2
                    pts["step"] = "tipo"
                    _pt_state["pintura_state"] = pts
                    upsert_quote_state(company_id, wa_from, _pt_state)
                    return {
                        "type": "list",
                        "body": f"📐 *{m2:.0f} m²*. ¿Qué tipo de pintura?",
                        "options": ["Vinílica", "Esmalte"],
                        "button_label": "Elegir tipo",
                    }
                else:
                    return "Necesito un número. ¿Cuántos m² vas a pintar? (ej: 120)"

            elif pts.get("step") == "tipo":
                tipo_lower = tnorm.strip()
                if "esmalte" in tipo_lower:
                    pts["tipo"] = "Esmalte"
                else:
                    pts["tipo"] = "Vinílica"
                pts["step"] = "uso"
                _pt_state["pintura_state"] = pts
                upsert_quote_state(company_id, wa_from, _pt_state)
                return {
                    "type": "text_then_buttons",
                    "text": f"🎨 Pintura *{pts['tipo']}*. ¿Es para interior o exterior?",
                    "body": "Elige el uso:",
                    "buttons": ["🏠 Interior", "☀️ Exterior"],
                }

            elif pts.get("step") == "uso":
                import math
                m2 = pts.get("m2", 0)
                tipo = pts.get("tipo", "Vinílica")
                uso_lower = tnorm.strip()
                if "exterior" in uso_lower:
                    uso = "Exterior"
                else:
                    uso = "Interior"

                if tipo == "Esmalte":
                    rendimiento_litro = 8.5  # m2 por litro
                else:
                    rendimiento_litro = 80.0 / 19.0  # ~4.21 m2 por litro

                litros_total = math.ceil(m2 / rendimiento_litro)
                # Round up to full cubetas for cotización (simpler, no mixed presentations)
                _cubetas_total = math.ceil(litros_total / 19)
                # Keep desglose for visual reference
                cubetas, galones, litros = _desglose_litros(litros_total)

                # Brocha/rodillo suggestion: mix of 4" and 2"
                _rodillos = max(1, math.ceil(m2 / 40))
                _brochas_4 = max(1, math.ceil(m2 / 120))
                _brochas_2 = max(1, math.ceil(m2 / 120))

                resultado = (
                    f"🎨 *Cálculo de pintura {tipo} {uso}*\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"📐 Superficie: *{m2:.0f} m²*\n"
                    f"📊 Rendimiento: *{rendimiento_litro:.1f} m²/litro*\n\n"
                    f"🪣 Total: *{litros_total} litros* → *{_cubetas_total} cubeta{'s' if _cubetas_total > 1 else ''}*\n"
                    f"🖌️ Sugerido: *{_rodillos} rodillo{'s' if _rodillos > 1 else ''}*, *{_brochas_4} brocha{'s' if _brochas_4 > 1 else ''} 4\"*, *{_brochas_2} brocha{'s' if _brochas_2 > 1 else ''} 2\"*\n"
                    f"━━━━━━━━━━━━━━━━━━━"
                )

                # Cotizar only cubetas (rounded up) — no galones/litros separate
                _tipo_search = "vinilica" if tipo == "Vinílica" else "esmalte"
                _uso_search = "interior" if uso == "Interior" else "exterior"

                # Build material items with targeted SQL search (same approach as rejacero)
                _paint_mat_items = [
                    (_cubetas_total, "pintura", _tipo_search, _uso_search, "cubeta"),
                    (_rodillos, "rodillo", None, None, None),
                    (_brochas_4, "brocha", "4", None, None),
                    (_brochas_2, "brocha", "2", None, None),
                ]

                # Auto-cotizar: search and add to cart directly
                state = _pt_state
                conn = get_conn()
                try:
                    cur_pt2 = conn.cursor()
                    for _item_tuple in _paint_mat_items:
                        _lqty = _item_tuple[0]
                        row_pt2 = None

                        if _item_tuple[1] == "pintura":
                            # Targeted paint search: must contain tipo AND uso AND cubeta
                            # Try exact match with all keywords using ILIKE
                            _tipo_kw = _item_tuple[2]  # vinilica / esmalte
                            _uso_kw = _item_tuple[3]   # interior / exterior
                            cur_pt2.execute(
                                "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                "WHERE company_id=%s "
                                "AND lower(name) LIKE '%%' || %s || '%%' "
                                "AND lower(name) LIKE '%%' || %s || '%%' "
                                "AND lower(name) LIKE '%%cubeta%%' "
                                "ORDER BY price DESC LIMIT 1",
                                (company_id, _tipo_kw, _uso_kw),
                            )
                            row_pt2 = cur_pt2.fetchone()
                            if not row_pt2:
                                # Fallback: just tipo + cubeta (maybe catalog doesn't specify interior/exterior)
                                cur_pt2.execute(
                                    "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                    "WHERE company_id=%s "
                                    "AND lower(name) LIKE '%%' || %s || '%%' "
                                    "AND lower(name) LIKE '%%cubeta%%' "
                                    "ORDER BY price DESC LIMIT 1",
                                    (company_id, _tipo_kw),
                                )
                                row_pt2 = cur_pt2.fetchone()
                        elif _item_tuple[1] == "rodillo":
                            cur_pt2.execute(
                                "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                "WHERE company_id=%s AND lower(name) LIKE '%%rodillo%%' "
                                "ORDER BY price DESC LIMIT 1",
                                (company_id,),
                            )
                            row_pt2 = cur_pt2.fetchone()
                        elif _item_tuple[1] == "brocha":
                            _brocha_size = _item_tuple[2]  # "4" or "2"
                            cur_pt2.execute(
                                "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                "WHERE company_id=%s AND lower(name) LIKE '%%brocha%%' "
                                "AND name LIKE '%%' || %s || '%%' "
                                "ORDER BY price DESC LIMIT 1",
                                (company_id, _brocha_size),
                            )
                            row_pt2 = cur_pt2.fetchone()

                        if row_pt2:
                            state = cart_add_item(state, {
                                "sku": row_pt2[0], "name": row_pt2[1],
                                "unit": row_pt2[2] or "pza",
                                "price": float(row_pt2[3] or 0),
                                "vat_rate": row_pt2[4],
                                "qty": _lqty,
                            })
                        else:
                            # Final fallback: smart_search
                            _fallback_name = f"{_item_tuple[1]} {_item_tuple[2] or ''} {_item_tuple[3] or ''}".strip()
                            result = smart_search(conn, company_id, _fallback_name, _lqty,
                                                  cart_context=_build_cart_context(state))
                            if result["status"] == "found":
                                state = cart_add_item(state, {
                                    "sku": result["item"].get("sku"),
                                    "name": result["item"].get("name"),
                                    "unit": result["item"].get("unit") or "pza",
                                    "price": float(result["item"].get("price") or 0.0),
                                    "vat_rate": result["item"].get("vat_rate"),
                                    "qty": _lqty,
                                })
                            elif result.get("candidates"):
                                pend = state.get("pending") or []
                                pend.append({"qty": _lqty, "raw": _fallback_name, "candidates": result["candidates"]})
                                state["pending"] = pend
                    cur_pt2.close()
                finally:
                    conn.close()
                state.pop("pintura_state", None)
                upsert_quote_state(company_id, wa_from, state)

                # Send cart/pending reply first, then desglose as reference at the end
                _reply = _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
                if isinstance(_reply, dict):
                    _reply["text"] = (_reply.get("text") or "") + "\n\n" + resultado
                else:
                    _reply = str(_reply) + "\n\n" + resultado
                return _reply

        _pt_triggers = {"calcular pintura", "🎨 calcular pintura", "pintura", "cuanta pintura",
                        "cuánta pintura", "calcular pintura m2"}
        if tnorm in _pt_triggers:
            _pt_state["pintura_state"] = {"step": "m2"}
            upsert_quote_state(company_id, wa_from, _pt_state)
            return "📐 ¿Cuántos m² vas a pintar? (ej: 120)"

    # ── Impermeabilizante calculator ─────────────────────────────────────────
    try:
        conn_im = get_conn()
        cur_im = conn_im.cursor()
        cur_im.execute("SELECT impermeabilizante_enabled FROM companies WHERE id=%s", (company_id,))
        row_im = cur_im.fetchone()
        cur_im.close()
        conn_im.close()
        _im_enabled = bool(row_im[0]) if row_im else False
    except Exception:
        _im_enabled = False

    if _im_enabled:
        _im_state = get_quote_state(company_id, wa_from) if wa_from else {}
        _im_state = _im_state or {}

        if _im_state.get("imper_state"):
            ims = _im_state["imper_state"]

            if ims.get("step") == "m2":
                _m_num = re.match(r"^\s*(\d+(?:\.\d+)?)\s*", user_text.strip())
                if _m_num:
                    import math
                    m2 = float(_m_num.group(1))

                    # Calculate directly — no type question, always include malla
                    litros_total = math.ceil(m2)
                    # Round up to full cubetas for cotización
                    _cubetas_im = math.ceil(litros_total / 19)
                    rollos_malla = math.ceil(m2 / 100)  # 1 rollo ≈ 100 m2

                    # Brocha/rodillo suggestion: mix of 4" and 2"
                    _rodillos_im = max(1, math.ceil(m2 / 40))
                    _brochas_4_im = max(1, math.ceil(m2 / 120))
                    _brochas_2_im = max(1, math.ceil(m2 / 120))

                    resultado = (
                        f"🛡️ *Cálculo de impermeabilizante*\n"
                        f"━━━━━━━━━━━━━━━━━━━\n"
                        f"📐 Azotea: *{m2:.0f} m²*\n"
                        f"📊 Rendimiento: *1 litro/m²*\n\n"
                        f"🪣 Impermeabilizante: *{litros_total} litros* → *{_cubetas_im} cubeta{'s' if _cubetas_im > 1 else ''}*\n"
                        f"🔗 Malla de refuerzo: *{rollos_malla} rollo{'s' if rollos_malla > 1 else ''}*\n"
                        f"🖌️ Sugerido: *{_rodillos_im} rodillo{'s' if _rodillos_im > 1 else ''}*, *{_brochas_4_im} brocha{'s' if _brochas_4_im > 1 else ''} 4\"*, *{_brochas_2_im} brocha{'s' if _brochas_2_im > 1 else ''} 2\"*\n"
                        f"━━━━━━━━━━━━━━━━━━━"
                    )

                    # Build material items with targeted SQL search
                    _imper_mat_items = [
                        (_cubetas_im, "impermeabilizante", "cubeta"),
                        (rollos_malla, "malla", "impermeabiliz"),
                        (_rodillos_im, "rodillo", None),
                        (_brochas_4_im, "brocha", "4"),
                        (_brochas_2_im, "brocha", "2"),
                    ]

                    # Auto-cotizar: search and add to cart directly
                    state = _im_state
                    conn = get_conn()
                    try:
                        cur_im2 = conn.cursor()
                        for _item_tuple in _imper_mat_items:
                            _lqty = _item_tuple[0]
                            _kw1 = _item_tuple[1]
                            _kw2 = _item_tuple[2]
                            row_im2 = None

                            if _kw2:
                                cur_im2.execute(
                                    "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                    "WHERE company_id=%s "
                                    "AND lower(name) LIKE '%%' || %s || '%%' "
                                    "AND lower(name) LIKE '%%' || %s || '%%' "
                                    "ORDER BY price DESC LIMIT 1",
                                    (company_id, _kw1, _kw2),
                                )
                                row_im2 = cur_im2.fetchone()

                            if not row_im2:
                                cur_im2.execute(
                                    "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                    "WHERE company_id=%s "
                                    "AND lower(name) LIKE '%%' || %s || '%%' "
                                    "ORDER BY price DESC LIMIT 1",
                                    (company_id, _kw1),
                                )
                                row_im2 = cur_im2.fetchone()

                            if row_im2:
                                state = cart_add_item(state, {
                                    "sku": row_im2[0], "name": row_im2[1],
                                    "unit": row_im2[2] or "pza",
                                    "price": float(row_im2[3] or 0),
                                    "vat_rate": row_im2[4],
                                    "qty": _lqty,
                                })
                            else:
                                # Final fallback: smart_search
                                _fallback_name = f"{_kw1} {_kw2 or ''}".strip()
                                result = smart_search(conn, company_id, _fallback_name, _lqty,
                                                      cart_context=_build_cart_context(state))
                                if result["status"] == "found":
                                    state = cart_add_item(state, {
                                        "sku": result["item"].get("sku"),
                                        "name": result["item"].get("name"),
                                        "unit": result["item"].get("unit") or "pza",
                                        "price": float(result["item"].get("price") or 0.0),
                                        "vat_rate": result["item"].get("vat_rate"),
                                        "qty": _lqty,
                                    })
                                elif result.get("candidates"):
                                    pend = state.get("pending") or []
                                    pend.append({"qty": _lqty, "raw": _fallback_name, "candidates": result["candidates"]})
                                    state["pending"] = pend
                        cur_im2.close()
                    finally:
                        conn.close()
                    state.pop("imper_state", None)
                    upsert_quote_state(company_id, wa_from, state)

                    # Send cart/pending reply first, then desglose as reference at the end
                    _reply = _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
                    if isinstance(_reply, dict):
                        _reply["text"] = (_reply.get("text") or "") + "\n\n" + resultado
                    else:
                        _reply = str(_reply) + "\n\n" + resultado
                    return _reply
                else:
                    return "Necesito un número. ¿Cuántos m² de azotea? (ej: 80)"

        _im_triggers = {"calcular impermeabilizante", "🛡️ calcular impermeabilizante", "impermeabilizante",
                        "cuanto impermeabilizante", "cuánto impermeabilizante", "calcular imper", "🛡️ calcular imper", "imper"}
        if tnorm in _im_triggers:
            _im_state["imper_state"] = {"step": "m2"}
            upsert_quote_state(company_id, wa_from, _im_state)
            return "📐 ¿Cuántos m² de azotea necesitas impermeabilizar? (ej: 80)"

    # ── Pick handler (one-at-a-time) ──────────────────────────────────────────
    _quick_picks = _parse_pending_picks(user_text)
    # Also handle bare number "1", "2" etc. as pick for the first pending item
    _bare_num = re.match(r"^\s*(\d)\s*$", user_text.strip())
    if _bare_num and not _quick_picks:
        _quick_picks = [("A", int(_bare_num.group(1)))]
    # Handle "ninguno"/"no"/"no está" as pick_A0 (skip) when there are pending options
    _skip_words = {"no", "ninguno", "ninguna", "no esta", "no está", "no lo tienen",
                   "no lo tengo", "no es", "saltar", "skip", "no aplica", "no hay"}
    if tnorm in _skip_words and not _quick_picks:
        _quick_picks = [("A", 0)]

    _state_picks = get_quote_state(company_id, wa_from) if wa_from else {}
    _state_picks = _state_picks or {}

    if _quick_picks and _state_picks.get("pending"):
        state = _state_picks
        pend = state.get("pending") or []

        # Since we show one at a time, any pick with letter "A" resolves the FIRST pending item
        pick_opt = None
        for letter, opt in _quick_picks:
            if letter == "A":
                pick_opt = opt
                break

        if pick_opt is not None and pend:
            # Find the first pending item WITH candidates (matches what _build_reply shows)
            first_idx = next((i for i, p in enumerate(pend) if p.get("candidates")), 0)
            first = pend[first_idx]
            cands = first.get("candidates") or []
            if pick_opt == 0:
                # "Ninguno" — skip this product, remove from pending
                pend.pop(first_idx)
            elif cands and 1 <= pick_opt <= len(cands):
                chosen = cands[pick_opt - 1]
                qty = int(first.get("qty") or 0)
                _pick_bundle = first.get("is_bundle", False)
                qty = _resolve_bundle_qty(qty, _pick_bundle, chosen)
                state = cart_add_item(state, {
                    "sku": chosen.get("sku"),
                    "name": chosen.get("name"),
                    "unit": chosen.get("unit") or "unidad",
                    "price": float(chosen.get("price") or 0.0),
                    "vat_rate": chosen.get("vat_rate"),
                    "qty": qty,
                })
                pend.pop(first_idx)  # Remove the resolved item
            # else: invalid option number, keep it pending

        if pend:
            state["pending"] = pend
        else:
            state.pop("pending", None)

        if wa_from:
            upsert_quote_state(company_id, wa_from, state)

        return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

    # ── Detect conversational "I want to quote" intent without a specific product ──
    # Messages like "me podrías cotizar un material", "quiero precio de algo",
    # "pasame precios", "necesito cotización" — no product name, just intent.
    _intent_patterns = [
        r"^(me\s+)?podri?a?s?\s+(pasar|cotizar|dar)\s+(precio|precios|cotizacion)",
        r"^(me\s+)?podri?a?s?\s+cotizar$",  # "me podria cotizar" (without noun)
        r"^(me\s+)?(puede[ns]?|podrian?)\s+cotizar$",  # "me puede cotizar"
        r"^quiero\s+(cotizar|precio|precios|una?\s+cotizacion)",
        r"^necesito\s+(cotizar|precio|precios|una?\s+cotizacion)",
        r"^(pasame|dame|mandame|enviame)\s+(precio|precios|cotizacion)",
        r"^(cotizar|cotizame|cotizacion)\s*(un\s+)?(material|producto)?$",
        r"^(me\s+)?cotiza(s|n)?$",  # "me cotiza" / "cotiza"
        r"^(un|de\s+un)\s+material$",
        r"^precio\s+de\s+(un\s+)?(material|producto)$",
        r"^(me\s+)?podri?a?s?\s+pasar\s+precio\s+de$",
    ]
    _tnorm_intent = re.sub(r"\b(por\s+favor|porfa|porfavor|plis|please|pls)\b", "", tnorm).strip()
    _tnorm_intent = re.sub(r"[¿?¡!.,]", "", _tnorm_intent).strip()
    _tnorm_intent = re.sub(r"\s+", " ", _tnorm_intent).strip()
    if any(re.search(p, _tnorm_intent) for p in _intent_patterns):
        try:
            conn_wh = get_conn()
            cur_wh = conn_wh.cursor()
            cur_wh.execute("SELECT welcome_products_hint FROM companies WHERE id=%s", (company_id,))
            row_wh = cur_wh.fetchone()
            cur_wh.close()
            conn_wh.close()
            hint = (row_wh[0] or "").strip() if row_wh else ""
        except Exception:
            hint = ""
        if hint:
            ejemplos = "\n".join(f"10 {p.strip()}" for p in hint.split(",") if p.strip())
            hint_txt = f"\n\nEj:\n{ejemplos}"
        else:
            hint_txt = "\n\nEj:\n10 cemento\n5 varilla 3/8\n20 block 15x20"
        return f"¡Claro! Mándame el nombre del material y la cantidad:{hint_txt}\n\nO todo en una línea separado por comas."

    # ── Message accumulator for rapid-fire short messages ─────────────────────
    # People sometimes type in fragments: "me puedes" / "pasar" / "precio" / "de material"
    # We buffer short messages (no numbers, ≤4 words) and concatenate within 8s window
    import time as _time
    _msg_words = user_text.strip().split()
    _has_digits = bool(re.search(r"\d", user_text))
    _is_short_fragment = len(_msg_words) <= 4 and not _has_digits and len(user_text.strip()) < 30

    # Button clicks (interactive) should NEVER be buffered
    if _is_short_fragment and wa_from and not _is_button_click:
        _buf_state = get_quote_state(company_id, wa_from) or {}
        _buf = _buf_state.get("_msg_buffer") or {}
        _buf_ts = _buf.get("ts") or 0
        _buf_texts = _buf.get("texts") or []
        _now = _time.time()

        if _buf_texts and (_now - _buf_ts) < 8:
            # Append to existing buffer
            _buf_texts.append(user_text.strip())
            _combined = " ".join(_buf_texts)
            # Check if combined text matches a conversational intent
            _combined_norm = re.sub(r"[¿?¡!.,]", "", norm_name(_combined)).strip()
            _is_intent = any(re.search(p, _combined_norm) for p in _intent_patterns)
            if _is_intent:
                # Clear buffer and redirect to cotizar
                _buf_state.pop("_msg_buffer", None)
                upsert_quote_state(company_id, wa_from, _buf_state)
                try:
                    conn_wh = get_conn()
                    cur_wh = conn_wh.cursor()
                    cur_wh.execute("SELECT welcome_products_hint FROM companies WHERE id=%s", (company_id,))
                    row_wh = cur_wh.fetchone()
                    cur_wh.close()
                    conn_wh.close()
                    hint = (row_wh[0] or "").strip() if row_wh else ""
                except Exception:
                    hint = ""
                if hint:
                    ejemplos = "\n".join(f"10 {p.strip()}" for p in hint.split(",") if p.strip())
                    hint_txt = f"\n\nEj:\n{ejemplos}"
                else:
                    hint_txt = "\n\nEj:\n10 cemento\n5 varilla 3/8\n20 block 15x20"
                return f"¡Claro! Mándame el nombre del material y la cantidad:{hint_txt}"
            else:
                # Keep buffering — don't respond yet
                _buf_state["_msg_buffer"] = {"ts": _buf_ts, "texts": _buf_texts}
                upsert_quote_state(company_id, wa_from, _buf_state)
                return None  # Signal to webhook: don't send any reply
        else:
            # Start new buffer
            _buf_state["_msg_buffer"] = {"ts": _now, "texts": [user_text.strip()]}
            upsert_quote_state(company_id, wa_from, _buf_state)
            return None  # Signal to webhook: don't send any reply

    # Clear any stale buffer since we got a real message (with numbers or long enough)
    if wa_from:
        _buf_state2 = get_quote_state(company_id, wa_from) or {}
        if _buf_state2.get("_msg_buffer"):
            # There was a buffer — prepend buffered text to current message
            _old_texts = _buf_state2["_msg_buffer"].get("texts") or []
            if _old_texts:
                user_text = " ".join(_old_texts) + " " + user_text
                tnorm = norm_name(user_text)
            _buf_state2.pop("_msg_buffer", None)
            upsert_quote_state(company_id, wa_from, _buf_state2)

    # ── LLM-first parser: intenta LLM primero, regex como fallback ──
    _llm_result = None
    if _PARSER_LLM_FIRST:
        _llm_result = _try_llm_parse(company_id, user_text)

    # ── LLM detectó que NO es una orden → escalar a humano directo ──
    if _llm_result and _llm_result.get("non_order"):
        print(f"LLM NON_ORDER: escalating to human. text='{user_text[:60]}'")
        return _escalate_non_quote(company_id, wa_from, user_text)

    if _llm_result and _llm_result.get("items") and not _llm_result.get("non_order"):
        # ── LLM PATH: procesa items directamente sin regex ni smart_search ──
        from llm_parser import norm_key as _llm_norm_key
        _cat_by_key = _llm_result.get("_cat_by_key") or {}
        _parser_used = "llm"
        print(f"LLM ITEMS: {[(it.get('qty'), it.get('key') or it.get('name')) for it in _llm_result['items']]}")
        conn = get_conn()
        try:
            state = get_quote_state(company_id, wa_from) if wa_from else None
            if not state:
                state = {}
            state.pop("pending_specs", None)
            state.pop("pending", None)
            missing = []
            for item in _llm_result["items"]:
                _key = item.get("key")
                _qty = item.get("qty", 1)
                _conf = item.get("confidence", 0)
                _matched = item.get("matched_text", "")
                _name = item.get("name", _matched)
                if _key and _conf >= 0.7:
                    cat_item = _cat_by_key.get(_key)
                    if not cat_item:
                        # Intenta buscar con norm_key por si hay diferencias menores
                        _nk = _llm_norm_key(_key)
                        cat_item = _cat_by_key.get(_nk)
                    if not cat_item:
                        # Intenta también con el nombre (LLM a veces devuelve name como key)
                        _nn = _llm_norm_key(_name)
                        cat_item = _cat_by_key.get(_nn)
                    if not cat_item:
                        # Fuzzy: busca si algún key del catálogo contiene el norm_key o viceversa
                        _nk2 = _llm_norm_key(_key)
                        for _ck, _cv in _cat_by_key.items():
                            if _nk2 and (_nk2 in _ck or _ck in _nk2):
                                cat_item = _cv
                                print(f"LLM FUZZY MATCH: {_key!r} → {_cv.get('name')!r}")
                                break
                    if cat_item:
                        state = cart_add_item(state, {
                            "sku": cat_item.get("sku"),
                            "name": cat_item.get("name"),
                            "unit": cat_item.get("unit") or "unidad",
                            "price": float(cat_item.get("price") or 0.0),
                            "vat_rate": cat_item.get("vat_rate"),
                            "qty": _qty,
                        })
                    else:
                        # Key del LLM no matcheó en catálogo → intenta smart_search
                        print(f"LLM KEY NOT IN CATALOG: key={_key!r}, trying smart_search")
                        _search_name2 = _name or _matched or _key
                        try:
                            _fb2 = smart_search(conn, company_id, _search_name2, _qty,
                                                 cart_context=user_text[:200])
                            if _fb2 and _fb2.get("status") == "found":
                                state = cart_add_item(state, {
                                    "sku": _fb2["item"].get("sku"),
                                    "name": _fb2["item"].get("name"),
                                    "unit": _fb2["item"].get("unit") or "unidad",
                                    "price": float(_fb2["item"].get("price") or 0.0),
                                    "vat_rate": _fb2["item"].get("vat_rate"),
                                    "qty": _qty,
                                })
                                print(f"LLM KEY MISS → SMART_SEARCH: {_search_name2!r} → {_fb2['item'].get('name')!r}")
                            elif _fb2 and _fb2.get("candidates"):
                                missing.append({"qty": _qty, "raw": _search_name2,
                                                "candidates": _fb2["candidates"]})
                            else:
                                missing.append({"qty": _qty, "raw": _matched or _name, "candidates": []})
                        except Exception as _fb2e:
                            print(f"LLM KEY MISS FALLBACK ERROR: {repr(_fb2e)}")
                            missing.append({"qty": _qty, "raw": _matched or _name, "candidates": []})
                else:
                    # Low confidence or no key → intenta smart_search como último recurso
                    _fallback_found = False
                    _search_name = _name or _matched
                    if _search_name and len(_search_name.strip()) >= 3:
                        try:
                            _fb_result = smart_search(conn, company_id, _search_name, _qty,
                                                       cart_context=user_text[:200])
                            if _fb_result and _fb_result.get("status") == "found":
                                state = cart_add_item(state, {
                                    "sku": _fb_result["item"].get("sku"),
                                    "name": _fb_result["item"].get("name"),
                                    "unit": _fb_result["item"].get("unit") or "unidad",
                                    "price": float(_fb_result["item"].get("price") or 0.0),
                                    "vat_rate": _fb_result["item"].get("vat_rate"),
                                    "qty": _qty,
                                })
                                _fallback_found = True
                                print(f"LLM FALLBACK SMART_SEARCH: {_search_name!r} → {_fb_result['item'].get('name')!r}")
                            elif _fb_result and _fb_result.get("candidates"):
                                # Ambiguo: pasar candidatos para que el bot pregunte al cliente
                                missing.append({"qty": _qty, "raw": _search_name,
                                                "candidates": _fb_result["candidates"]})
                                _fallback_found = True
                                print(f"LLM FALLBACK AMBIGUOUS: {_search_name!r} → {len(_fb_result['candidates'])} candidates")
                        except Exception as _fbe:
                            print(f"LLM FALLBACK ERROR: {repr(_fbe)}")
                    if not _fallback_found:
                        missing.append({"qty": _qty, "raw": _matched or _name, "candidates": []})

            if missing:
                state["pending"] = missing
                _retry_map = state.get("retry_count") or {}
                for _m in missing:
                    _rkey = (_m.get("raw") or "").strip().lower()
                    if _rkey and _rkey != "producto ilegible":
                        _retry_map[_rkey] = int(_retry_map.get(_rkey, 0)) + 1
                state["retry_count"] = _retry_map
                _total_items = len(_llm_result["items"])
                _miss_count = len([m for m in missing if m.get("raw") != "producto ilegible"])
                _should_escalate_low = (
                    _total_items >= 3
                    and _miss_count >= 3
                    and (_miss_count / max(_total_items, 1)) >= 0.5
                )
                _retry_escalated = [k for k, v in _retry_map.items() if v >= 2]
                _should_escalate_retry = bool(_retry_escalated)
                if (_should_escalate_low or _should_escalate_retry) and wa_from:
                    try:
                        _conn_esc = get_conn()
                        _cur_esc = _conn_esc.cursor()
                        _cur_esc.execute(
                            "SELECT owner_phone, wa_api_key, wa_phone_number_id, telefono_atencion, name FROM companies WHERE id=%s",
                            (company_id,),
                        )
                        _row_esc = _cur_esc.fetchone()
                        _cur_esc.close()
                        _conn_esc.close()
                        _atencion = (_row_esc[3] or _row_esc[0] or "").strip() if _row_esc else ""
                        _cname = (_row_esc[4] if _row_esc else "la empresa") or "la empresa"
                        _notify_phone = (_row_esc[3] or _row_esc[0] or "").strip() if _row_esc else ""
                        if _notify_phone and _row_esc and _row_esc[1] and _row_esc[2]:
                            _reason = (
                                f"Múltiples productos sin match ({_miss_count}/{_total_items})"
                                if _should_escalate_low
                                else f"Producto intentado {max(_retry_map.values())}x sin éxito"
                            )
                            try:
                                notify_owner_escalation(
                                    wa_api_key=_row_esc[1], phone_number_id=_row_esc[2],
                                    owner_phone=_notify_phone, client_phone=wa_from,
                                    reason=_reason, state=state,
                                )
                            except Exception as _ne:
                                print("LLM ESCALATION NOTIFY ERROR:", repr(_ne))
                        state["escalated_proactive"] = True
                        if wa_from:
                            upsert_quote_state(company_id, wa_from, state)
                        print(f"LLM ESCALATION: low={_should_escalate_low} retry={_should_escalate_retry} miss={_miss_count}/{_total_items}")
                        _phone_clean = _normalize_mx_phone(_atencion) if _atencion else ""
                        _prefix = cart_render_quote(state, company_id=company_id, client_phone=wa_from) + "\n\n" if state.get("cart") else ""
                        if _phone_clean:
                            return _prefix + (
                                f"Veo que varios productos no los tengo identificados exacto. "
                                f"Para que no batalles, mejor te atiende un asesor de *{_cname}* directo 🙏\n\n"
                                f"👉 https://wa.me/{_phone_clean}\n\n"
                                f"Ya le avisé para que te contacte."
                            )
                        return _prefix + (
                            "Veo que varios productos no los tengo identificados exacto. "
                            "Un asesor te va a contactar para ayudarte directo 🙏"
                        )
                    except Exception as _e:
                        print("LLM ESCALATION ERROR:", repr(_e))
            else:
                state.pop("pending", None)
                state.pop("retry_count", None)
                state.pop("escalated_proactive", None)
            if wa_from:
                upsert_quote_state(company_id, wa_from, state)
            if not state.get("cart") and not missing:
                return "No encontré esos productos en el catálogo."
            return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
        finally:
            conn.close()

    # ── Fallback: regex parser (si LLM no está activo o falló) ──
    # Detect free-text: single long line with multiple products but no line breaks/bullets
    # e.g. "40 tablaroca 20 angulo 30 canal carga 2000 pijas de 1 1 Kg alambre"
    # These are almost impossible for regex to parse correctly — use GPT first
    _is_freetext = (
        "\n" not in user_text.strip()
        and "•" not in user_text
        and len(user_text.strip()) > 60
        and len(re.findall(r"\b\d+\s+[a-záéíóúñ]", user_text, re.IGNORECASE)) >= 3
    )
    if _is_freetext:
        multi = ner_extract_items(user_text)
        _parser_used = "ner"
        if not multi:
            multi = extract_qty_items_robust(user_text)
            _parser_used = "robust"
    else:
        # Structured input (line breaks, bullets) — regex parser is reliable
        multi = extract_qty_items_robust(user_text)
        _parser_used = "robust"
        if not multi:
            multi = ner_extract_items(user_text)
            _parser_used = "ner"
    try:
        log_parser_shadow(company_id, wa_from or "", user_text or "", multi or [])
    except Exception as _e:
        print("SHADOW: skipped:", repr(_e))
    if multi:
        print(f"MULTI ITEMS ({_parser_used}): {[(q, p) for q, p, *_ in multi]}")
        conn = get_conn()
        try:
            state = get_quote_state(company_id, wa_from) if wa_from else None
            if not state:
                state = {}
            state.pop("pending_specs", None)
            state.pop("pending", None)  # Clear stale pending — new product list takes priority
            missing = []
            _pedido_raw = ", ".join(p for _, p, *_ in multi if p.strip() != "???")
            for _mi in multi:
                qty, prod_raw = _mi[0], _mi[1]
                _mi_bundle = _mi[2] if len(_mi) > 2 else False
                if not looks_like_product_phrase(prod_raw) and len(prod_raw.strip()) < 2:
                    continue
                _skip_phrases = {"???", "producto ilegible", "ilegible", "no identificado",
                                 "producto no identificado", "producto desconocido", "desconocido",
                                 "no se entiende", "no legible"}
                if prod_raw.strip().lower() in _skip_phrases or prod_raw.strip() == "???":
                    # Count illegible items but only add ONE summary entry (avoid 76x "ilegible" spam)
                    _illegible_count = sum(1 for m in missing if m.get("raw") == "producto ilegible")
                    if _illegible_count == 0:
                        missing.append({"qty": qty, "raw": "producto ilegible", "candidates": []})
                    else:
                        # Update the existing illegible entry count
                        for m in missing:
                            if m.get("raw") == "producto ilegible":
                                m["qty"] = m.get("qty", 1) + qty
                                break
                    continue
                steps = get_spec_steps(prod_raw)
                if steps and not already_has_specs(prod_raw, steps):
                    specs_pending = state.get("pending_specs") or []
                    specs_pending.append({"raw": prod_raw, "qty": qty, "is_bundle": _mi_bundle, "steps": steps, "step_idx": 0, "resolved": {}})
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
                    _fq = _resolve_bundle_qty(qty, _mi_bundle, result["item"])
                    state = cart_add_item(state, {
                        "sku": result["item"].get("sku"),
                        "name": result["item"].get("name"),
                        "unit": result["item"].get("unit") or "unidad",
                        "price": float(result["item"].get("price") or 0.0),
                        "vat_rate": result["item"].get("vat_rate"),
                        "qty": _fq,
                    })
                else:
                    missing.append({"qty": qty, "raw": prod_raw, "is_bundle": _mi_bundle, "candidates": result["candidates"]})
            if missing:
                state["pending"] = missing
                # ── Retry counter: incrementa por producto raw que no se resolvió
                _retry_map = state.get("retry_count") or {}
                for _m in missing:
                    _rkey = (_m.get("raw") or "").strip().lower()
                    if _rkey and _rkey != "producto ilegible":
                        _retry_map[_rkey] = int(_retry_map.get(_rkey, 0)) + 1
                state["retry_count"] = _retry_map
                # ── Low-confidence list detector: si ≥50% de los items (y al menos 3)
                # no se resolvieron, escalamos proactivamente a humano
                _total_items = len(multi)
                _miss_count = len([m for m in missing if m.get("raw") != "producto ilegible"])
                _should_escalate_low = (
                    _total_items >= 3
                    and _miss_count >= 3
                    and (_miss_count / max(_total_items, 1)) >= 0.5
                )
                # ── Retry escalation: si algún producto ya lleva ≥2 intentos fallidos
                _retry_escalated = [k for k, v in _retry_map.items() if v >= 2]
                _should_escalate_retry = bool(_retry_escalated)
                if (_should_escalate_low or _should_escalate_retry) and wa_from:
                    try:
                        _conn_esc = get_conn()
                        _cur_esc = _conn_esc.cursor()
                        _cur_esc.execute(
                            "SELECT owner_phone, wa_api_key, wa_phone_number_id, telefono_atencion, name FROM companies WHERE id=%s",
                            (company_id,),
                        )
                        _row_esc = _cur_esc.fetchone()
                        _cur_esc.close()
                        _conn_esc.close()
                        _atencion = (_row_esc[3] or _row_esc[0] or "").strip() if _row_esc else ""
                        _cname = (_row_esc[4] if _row_esc else "la empresa") or "la empresa"
                        # Notify owner
                        _notify_phone = (_row_esc[3] or _row_esc[0] or "").strip() if _row_esc else ""
                        if _notify_phone and _row_esc and _row_esc[1] and _row_esc[2]:
                            _reason = (
                                f"Múltiples productos sin match ({_miss_count}/{_total_items})"
                                if _should_escalate_low
                                else f"Producto intentado {max(_retry_map.values())}x sin éxito"
                            )
                            try:
                                notify_owner_escalation(
                                    wa_api_key=_row_esc[1], phone_number_id=_row_esc[2],
                                    owner_phone=_notify_phone, client_phone=wa_from,
                                    reason=_reason, state=state,
                                )
                            except Exception as _ne:
                                print("PROACTIVE ESCALATION NOTIFY ERROR:", repr(_ne))
                        # Mark escalated so we don't spam
                        state["escalated_proactive"] = True
                        if wa_from:
                            upsert_quote_state(company_id, wa_from, state)
                        print(f"PROACTIVE ESCALATION: low={_should_escalate_low} retry={_should_escalate_retry} miss={_miss_count}/{_total_items}")
                        _phone_clean = _normalize_mx_phone(_atencion) if _atencion else ""
                        _prefix = cart_render_quote(state, company_id=company_id, client_phone=wa_from) + "\n\n" if state.get("cart") else ""
                        if _phone_clean:
                            return _prefix + (
                                f"Veo que varios productos no los tengo identificados exacto. "
                                f"Para que no batalles, mejor te atiende un asesor de *{_cname}* directo 🙏\n\n"
                                f"👉 https://wa.me/{_phone_clean}\n\n"
                                f"Ya le avisé para que te contacte."
                            )
                        return _prefix + (
                            "Veo que varios productos no los tengo identificados exacto. "
                            "Un asesor te va a contactar para ayudarte directo 🙏"
                        )
                    except Exception as _e:
                        print("PROACTIVE ESCALATION ERROR:", repr(_e))
            else:
                state.pop("pending", None)
                state.pop("retry_count", None)
                state.pop("escalated_proactive", None)
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

    qty, prod_query, _is_bundle = extract_qty_and_product(user_text)
    if qty and prod_query:
        steps = get_spec_steps(prod_query)
        if steps and not already_has_specs(prod_query, steps):
            state = get_quote_state(company_id, wa_from) if wa_from else {}
            state = state or {}
            state["pending_specs"] = [{"raw": prod_query, "qty": qty, "is_bundle": _is_bundle, "steps": steps, "step_idx": 0, "resolved": {}}]
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
                _final_qty = _resolve_bundle_qty(qty, _is_bundle, result["item"])
                state = _single_state
                state = cart_add_item(state, {
                    "sku": result["item"].get("sku"),
                    "name": result["item"].get("name"),
                    "unit": result["item"].get("unit") or "unidad",
                    "price": float(result["item"].get("price") or 0.0),
                    "vat_rate": result["item"].get("vat_rate"),
                    "qty": _final_qty,
                })
                state.pop("pending", None)
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

            elif result["status"] == "ambiguous":
                pending = [{"qty": qty, "raw": prod_query, "is_bundle": _is_bundle, "candidates": result["candidates"]}]
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
            return {
                "type": "text_then_buttons",
                "text": "Encontré estos precios:\n" + "\n".join(lines),
                "body": "¿Quieres cotizar alguno?",
                "buttons": ["🔨 Cotizar materiales", "➕ Agregar más", "🚪 Salir"],
            }

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
        qty, prod_query, _is_bundle2 = extract_qty_and_product(user_text)
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
                _final_qty2 = _resolve_bundle_qty(qty, _is_bundle2, result["item"])
                state = _fallback_state
                state = cart_add_item(state, {
                    "sku": result["item"].get("sku"),
                    "name": result["item"].get("name"),
                    "unit": result["item"].get("unit") or "unidad",
                    "price": float(result["item"].get("price") or 0.0),
                    "vat_rate": result["item"].get("vat_rate"),
                    "qty": _final_qty2,
                })
                state.pop("pending", None)
                if wa_from:
                    upsert_quote_state(company_id, wa_from, state)
                return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)

            elif result["status"] == "ambiguous":
                state = _fallback_state
                state["pending"] = [{"qty": qty, "raw": prod_query, "is_bundle": _is_bundle, "candidates": result["candidates"]}]
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
        # --- Default qty=1: productos sin cantidad se cotizan automáticamente ---
        productos_detectados = []
        # Intentar splitear por separadores comunes
        for sep in ["\n", " y ", " e ", ",", "/"]:
            if sep in user_text.lower():
                partes = [p.strip() for p in user_text.lower().split(sep) if p.strip()]
                if len(partes) > 1:
                    productos_detectados = partes
                    break

        if productos_detectados:
            # Multi-producto sin cantidad → asumir qty=1 cada uno
            multi = [(1, p) for p in productos_detectados]
            conn = get_conn()
            try:
                state = get_quote_state(company_id, wa_from) if wa_from else None
                if not state:
                    state = {}
                state.pop("pending_specs", None)
                missing = []
                _pedido_raw = ", ".join(p for _, p, *_ in multi if p.strip() != "???")
                for _mi2 in multi:
                    qty, prod_raw = _mi2[0], _mi2[1]
                    _mi2_bundle = _mi2[2] if len(_mi2) > 2 else False
                    if not looks_like_product_phrase(prod_raw) and len(prod_raw.strip()) < 2:
                        continue
                    steps = get_spec_steps(prod_raw)
                    if steps and not already_has_specs(prod_raw, steps):
                        specs_pending = state.get("pending_specs") or []
                        specs_pending.append({"raw": prod_raw, "qty": qty, "is_bundle": _mi2_bundle, "steps": steps, "step_idx": 0, "resolved": {}})
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
                        _fq2 = _resolve_bundle_qty(qty, _mi2_bundle, result["item"])
                        state = cart_add_item(state, {
                            "sku": result["item"].get("sku"),
                            "name": result["item"].get("name"),
                            "unit": result["item"].get("unit") or "unidad",
                            "price": float(result["item"].get("price") or 0.0),
                            "vat_rate": result["item"].get("vat_rate"),
                            "qty": _fq2,
                        })
                    else:
                        missing.append({"qty": qty, "raw": prod_raw, "is_bundle": _mi2_bundle, "candidates": result["candidates"]})
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

        # Producto individual sin cantidad → clasificar y asumir qty=1
        intent = _classify_intent(user_text)
        if intent == "product":
            conn = get_conn()
            try:
                state = get_quote_state(company_id, wa_from) if wa_from else None
                if not state:
                    state = {}
                state.pop("pending_specs", None)
                prod_raw = user_text.strip()
                steps = get_spec_steps(prod_raw)
                if steps and not already_has_specs(prod_raw, steps):
                    specs_pending = state.get("pending_specs") or []
                    specs_pending.append({"raw": prod_raw, "qty": 1, "steps": steps, "step_idx": 0, "resolved": {}})
                    state["pending_specs"] = specs_pending
                    if wa_from:
                        upsert_quote_state(company_id, wa_from, state)
                    first = specs_pending[0]
                    first_step = first["steps"][first["step_idx"]]
                    return {
                        "type": "list",
                        "body": first_step["question"],
                        "options": first_step["options"],
                        "button_label": "Ver opciones",
                    }
                try:
                    result = smart_search(conn, company_id, prod_raw, 1,
                                          cart_context="")
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
                        "qty": 1,
                    })
                    state.pop("pending", None)
                    if wa_from:
                        upsert_quote_state(company_id, wa_from, state)
                    return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
                else:
                    missing = [{"qty": 1, "raw": prod_raw, "candidates": result["candidates"]}]
                    state["pending"] = missing
                    if wa_from:
                        upsert_quote_state(company_id, wa_from, state)
                    return _build_reply_with_pending(state, company_id=company_id, wa_from=wa_from)
            finally:
                conn.close()
        else:
            # No es producto → escalar a humano
            return _escalate_non_quote(company_id, wa_from, user_text)

    # Último fallback: clasificar con GPT
    intent = _classify_intent(user_text)
    if intent == "product":
        return "¿Me repites eso? No entendí bien tu pedido 🤔"
    else:
        return _escalate_non_quote(company_id, wa_from, user_text)


@app.post("/api/admin/rebuild-embeddings")
def rebuild_embeddings_endpoint(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    """Re-generate embeddings for all products of a company."""
    conn = get_conn()
    conn.autocommit = False
    try:
        result = rebuild_embeddings_for_company(conn, company_id)
        conn.commit()
        # Auto-generate context groups after rebuilding embeddings
        try:
            cg_result = auto_generate_context_groups(conn, company_id)
            print(f"AUTO CONTEXT GROUPS after rebuild: {cg_result.get('status')}")
        except Exception as cge:
            print(f"AUTO CONTEXT GROUPS ERROR: {repr(cge)}")
        return {"ok": True, **result}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@app.post("/api/admin/generate-context-groups")
def generate_context_groups_endpoint(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    """Generate context groups for a company using LLM clustering."""
    conn = get_conn()
    try:
        result = auto_generate_context_groups(conn, company_id)
        return {"ok": result.get("status") == "ok", **result}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


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


@app.get("/api/admin/synonyms-audit")
def synonyms_audit(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    """List all products with their synonyms for auditing"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT name, synonyms FROM pricebook_items WHERE company_id=%s ORDER BY name",
            (company_id,)
        )
        rows = cur.fetchall()
        result = []
        total_with_syn = 0
        total_without_syn = 0
        for name, synonyms in rows:
            syn = (synonyms or "").strip()
            if syn:
                total_with_syn += 1
                result.append({"name": name, "synonyms": syn})
            else:
                total_without_syn += 1
        return {
            "total": len(rows),
            "with_synonyms": total_with_syn,
            "without_synonyms": total_without_syn,
            "items": result
        }
    finally:
        cur.close()
        conn.close()


def _is_junk_synonym(syn: str, product_name: str) -> bool:
    """Detect garbage synonyms that should be removed."""
    s = syn.strip().lower()
    pn = product_name.strip().lower()
    pn_tokens = set(norm_name(pn).split())

    # Broken: contains parentheses, empty, too short
    if not s or len(s) < 3:
        return True
    if "(" in s or ")" in s:
        return True

    # Single-word synonym that's just a plural/singular of a product name token
    if " " not in s:
        # Check if it's a trivial inflection of any token in the name
        for tok in pn_tokens:
            # "laminas" from "lamina", "galvanizadas" from "galvanizado", etc.
            if s == tok:
                return True  # exact duplicate of name token
            # s is tok+"s", tok+"es", or tok minus "s"/"es"
            if s == tok + "s" or s == tok + "es":
                return True
            if tok == s + "s" or tok == s + "es":
                return True
            if s.endswith("es") and s[:-2] == tok:
                return True
            if tok.endswith("es") and tok[:-2] == s:
                return True
            if s.endswith("s") and s[:-1] == tok:
                return True
            if tok.endswith("s") and tok[:-1] == s:
                return True
            # "galvanizadas" ↔ "galvanizados" (gender swap)
            if len(s) > 4 and len(tok) > 4:
                if s[:-1] == tok[:-1] and s[-1] in "aeos" and tok[-1] in "aeos":
                    return True

    return False


@app.post("/api/admin/synonyms-clean")
def synonyms_clean(
    company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75",
    dry_run: bool = True
):
    """
    Clean garbage synonyms from pricebook_items.

    Removes:
    1. Broken auto-plurals (parentheses, too short, etc.)
    2. Single-word trivial inflections of product name tokens
    3. Duplicate synonyms that appear in 10+ products (too generic to help)

    Keeps:
    - Multi-word jerga phrases (e.g. "cinta para durock", "teja roja")
    - Unique alternative names

    Pass dry_run=false to actually update the database.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, synonyms FROM pricebook_items WHERE company_id=%s AND synonyms IS NOT NULL AND synonyms != '' ORDER BY name",
            (company_id,)
        )
        rows = cur.fetchall()

        # First pass: count synonym frequency across products
        syn_frequency = {}
        for item_id, name, synonyms in rows:
            for s in (synonyms or "").split(","):
                s = s.strip().lower()
                if s:
                    syn_frequency[s] = syn_frequency.get(s, 0) + 1

        # Second pass: clean each product
        changes = []
        total_removed = 0
        total_kept = 0
        removed_examples = []

        for item_id, name, synonyms in rows:
            original_syns = [s.strip() for s in (synonyms or "").split(",") if s.strip()]
            kept = []
            removed = []

            for syn in original_syns:
                sl = syn.strip().lower()
                reason = None

                if _is_junk_synonym(syn, name):
                    reason = "junk_inflection"
                elif syn_frequency.get(sl, 0) >= 10:
                    reason = f"too_generic(appears_in_{syn_frequency[sl]}_products)"

                if reason:
                    removed.append({"synonym": syn, "reason": reason})
                    total_removed += 1
                else:
                    kept.append(syn)
                    total_kept += 1

            if removed:
                new_synonyms = ", ".join(kept) if kept else ""
                changes.append({
                    "id": item_id,
                    "name": name,
                    "original": synonyms,
                    "cleaned": new_synonyms,
                    "removed": removed,
                    "kept": kept
                })
                if len(removed_examples) < 30:
                    removed_examples.append({
                        "product": name,
                        "removed": [r["synonym"] + f" ({r['reason']})" for r in removed],
                        "kept": kept
                    })

        # Apply changes if not dry_run
        updated = 0
        if not dry_run:
            for change in changes:
                cur.execute(
                    "UPDATE pricebook_items SET synonyms=%s, updated_at=NOW() WHERE id=%s",
                    (change["cleaned"], change["id"])
                )
                updated += 1
            conn.commit()

        return {
            "dry_run": dry_run,
            "total_products_analyzed": len(rows),
            "products_with_changes": len(changes),
            "total_synonyms_removed": total_removed,
            "total_synonyms_kept": total_kept,
            "updated_in_db": updated,
            "examples": removed_examples,
            "hint": "Pass dry_run=false to apply changes" if dry_run else "Changes applied! Run rebuild-embeddings next."
        }
    finally:
        cur.close()
        conn.close()


@app.post("/api/admin/set-bundle-size")
def set_bundle_size(
    company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75",
    name_contains: str = "",
    bundle_size: int = 12,
    dry_run: bool = True
):
    """Set bundle_size for products matching name_contains. E.g. name_contains=poste&bundle_size=12"""
    if not name_contains:
        raise HTTPException(status_code=400, detail="name_contains requerido")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, bundle_size FROM pricebook_items WHERE company_id=%s AND lower(name) LIKE lower(%s) ORDER BY name",
            (company_id, f"%{name_contains}%")
        )
        rows = cur.fetchall()
        results = []
        for item_id, name, current_bs in rows:
            results.append({"id": item_id, "name": name, "old_bundle_size": current_bs, "new_bundle_size": bundle_size})
            if not dry_run:
                cur.execute("UPDATE pricebook_items SET bundle_size=%s WHERE id=%s", (bundle_size, item_id))
        if not dry_run:
            conn.commit()
        return {
            "dry_run": dry_run,
            "matched": len(results),
            "bundle_size": bundle_size,
            "products": results,
            "hint": "Pass dry_run=false to apply" if dry_run else "Done!"
        }
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

    # Build fully dynamic SET clause — only update fields that were actually sent
    # This prevents module toggle requests from wiping unrelated settings to NULL
    _sets = []
    _vals = []

    # Helper: add field to SET clause only if it was explicitly provided
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

    # Module toggles
    _add_bool(body.construccion_ligera_enabled, "construccion_ligera_enabled")
    _add_bool(body.rejacero_enabled, "rejacero_enabled")
    _add_bool(body.pintura_enabled, "pintura_enabled")
    _add_bool(body.impermeabilizante_enabled, "impermeabilizante_enabled")

    if not _sets:
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
        # Brand context columns
        for _bcol in ("marcas_propias", "marcas_competencia"):
            cur.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='companies' AND column_name='{_bcol}')
                    THEN ALTER TABLE companies ADD COLUMN {_bcol} TEXT;
                    END IF;
                END $$;
            """)
        conn.commit()

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
                   telefono_atencion, marcas_propias, marcas_competencia
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
                "welcome_products_hint": row[15] or None,
                "welcome_message": row[16] or None,
                "telefono_atencion": row[17] or None,
                "marcas_propias": row[18] or None,
                "marcas_competencia": row[19] or None,
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

# ── Promo Codes ──────────────────────────────────────────────────────────────

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


@app.post("/api/pagos/promo/crear")
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
        raise HTTPException(status_code=409, detail=f"El código '{code}' ya existe")
    except Exception as e:
        print("PROMO CREAR ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/pagos/promo/listar")
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
        print("PROMO LISTAR ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.patch("/api/pagos/promo/{promo_id}/toggle")
def promo_toggle(promo_id: str, request: Request, body: PromoCodeToggle):
    """Admin: activar/desactivar un código promo."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "UPDATE promo_codes SET active=%s WHERE id=%s RETURNING id, active",
            (body.active, promo_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Código no encontrado")
        return {"ok": True, "id": str(row[0]), "active": row[1]}
    except HTTPException:
        raise
    except Exception as e:
        print("PROMO TOGGLE ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.delete("/api/pagos/promo/{promo_id}")
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
        print("PROMO DELETE ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.post("/api/pagos/promo/validar")
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
        print("PROMO VALIDAR ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.post("/api/pagos/promo/aplicar")
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

        print(f"PROMO APLICADO: company={company_id} code={code} type={dtype} value={dval}")
        return {"ok": True, "message": result_msg, "discount_type": dtype, "discount_value": float(dval)}
    except HTTPException:
        raise
    except Exception as e:
        print("PROMO APLICAR ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


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
        # Ensure module columns exist
        for _mcol, _mtype in [
            ("construccion_ligera_enabled", "BOOLEAN DEFAULT FALSE"),
            ("rejacero_enabled", "BOOLEAN DEFAULT FALSE"),
            ("pintura_enabled", "BOOLEAN DEFAULT FALSE"),
            ("impermeabilizante_enabled", "BOOLEAN DEFAULT FALSE"),
            ("welcome_message", "TEXT"),
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
        cur.execute("SELECT id::text, name, slug, twilio_phone, plan_code, construccion_ligera_enabled, rejacero_enabled, pintura_enabled, impermeabilizante_enabled, trial_end FROM companies WHERE id=%s LIMIT 1", (company_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company no encontrada")
        trial_end_val = row[9]
        return {"ok": True, "company": {
            "id": row[0], "name": row[1], "slug": row[2], "twilio_phone": row[3], "plan_code": row[4],
            "construccion_ligera_enabled": bool(row[5]) if row[5] is not None else False,
            "rejacero_enabled": bool(row[6]) if row[6] is not None else False,
            "pintura_enabled": bool(row[7]) if row[7] is not None else False,
            "impermeabilizante_enabled": bool(row[8]) if row[8] is not None else False,
            "trial_end": trial_end_val.isoformat() if trial_end_val else None,
        }}
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


class AdminDeleteTestUserBody(BaseModel):
    email: str
    confirm: str  # must be "DELETE"


@app.post("/api/admin/delete-test-user")
def admin_delete_test_user(body: AdminDeleteTestUserBody):
    """Delete a test user and ALL their company data. Requires confirm='DELETE'."""
    if body.confirm != "DELETE":
        raise HTTPException(status_code=400, detail="Debes enviar confirm='DELETE'")
    email = (body.email or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="email requerido")

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Find user and company
        cur.execute("SELECT id, company_id FROM users WHERE email = %s LIMIT 1", (email,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Usuario '{email}' no encontrado")
        user_id, company_id = row[0], str(row[1])

        deleted = {}

        # Delete in dependency order
        for table, col in [
            ("sessions", "user_id"),
        ]:
            cur.execute(f"DELETE FROM {table} WHERE {col} = %s", (user_id,))
            deleted[table] = cur.rowcount

        for table, col in [
            ("item_embeddings", "company_id"),
            ("pricebook_items", "company_id"),
            ("wa_quote_state", "company_id"),
            ("wa_conversation_windows", "company_id"),
            ("wa_usage_monthly", "company_id"),
            ("search_misses", "company_id"),
            ("conversations", "company_id"),
            ("api_keys", "company_id"),
        ]:
            try:
                cur.execute(f"DELETE FROM {table} WHERE {col} = %s", (company_id,))
                deleted[table] = cur.rowcount
            except Exception as e:
                deleted[table] = f"skip: {repr(e)}"
                conn.rollback()
                conn.autocommit = True

        # Delete user
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        deleted["users"] = cur.rowcount

        # Delete company
        cur.execute("DELETE FROM companies WHERE id = %s", (company_id,))
        deleted["companies"] = cur.rowcount

        return {"ok": True, "email": email, "company_id": company_id, "deleted": deleted}
    except HTTPException:
        raise
    except Exception as e:
        print(f"DELETE TEST USER ERROR: {repr(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
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

        # Aplicar código promo si se proporcionó
        promo_applied = None
        _promo = (body.promo_code or "").strip().upper()
        if _promo:
            try:
                cur.execute(
                    """
                    SELECT id, discount_type, discount_value, max_uses, times_used, active, expires_at
                    FROM promo_codes WHERE code=%s LIMIT 1
                    """,
                    (_promo,),
                )
                _prow = cur.fetchone()
                if _prow:
                    _pid, _dtype, _dval, _max, _used, _active, _exp = _prow
                    _now = datetime.now(timezone.utc)
                    _valid = _active and (_exp is None or _now <= _exp) and (_max is None or _used < _max)
                    if _valid and _dtype == "trial_days":
                        _trial_end = _now + timedelta(days=int(_dval))
                        cur.execute(
                            "UPDATE companies SET plan_code='pro', trial_end=%s, updated_at=now() WHERE id=%s",
                            (_trial_end, company_id),
                        )
                        cur.execute(
                            "INSERT INTO promo_code_uses (promo_code_id, company_id) VALUES (%s, %s)",
                            (_pid, company_id),
                        )
                        cur.execute(
                            "UPDATE promo_codes SET times_used = times_used + 1 WHERE id=%s",
                            (_pid,),
                        )
                        promo_applied = f"Trial Pro {int(_dval)} días"
                        print(f"REGISTRO+PROMO: company={company_id} code={_promo} trial_end={_trial_end}")
            except Exception as pe:
                print(f"REGISTRO PROMO ERROR (non-fatal): {repr(pe)}")

        return {"ok": True, "user_id": user_id, "company_id": str(company_id), "api_key": token, "promo_applied": promo_applied}
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
    company_id = u.get("company_id")
    u["onboarding_completed"] = False
    if company_id:
        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor()
            # Check if onboarding_completed column exists
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='companies' AND column_name='onboarding_completed'"
            )
            has_col = cur.fetchone() is not None
            if has_col:
                cur.execute("SELECT name, onboarding_completed FROM companies WHERE id=%s LIMIT 1", (company_id,))
                row = cur.fetchone()
                if row:
                    u["empresa_nombre"] = row[0]
                    u["onboarding_completed"] = bool(row[1]) if row[1] is not None else False
            else:
                cur.execute("SELECT name FROM companies WHERE id=%s LIMIT 1", (company_id,))
                row = cur.fetchone()
                if row:
                    u["empresa_nombre"] = row[0]
        except Exception as e:
            print(f"AUTH ME ENRICH ERROR: {repr(e)}")
        finally:
            if cur: cur.close()
            if conn: conn.close()
    # Marcar si es admin de CotizaExpress
    if (u.get("email") or "").lower() in ADMIN_EMAILS:
        u["rol"] = "admin"
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
            # Auto-generate context groups after rebuilding embeddings
            try:
                cg_result = auto_generate_context_groups(conn, company_id)
                print(f"BG CONTEXT GROUPS: {cg_result.get('status')}")
            except Exception as cge:
                print(f"BG CONTEXT GROUPS ERROR: {repr(cge)}")
        finally:
            conn.close()
    except Exception as e:
        print(f"BG EMBEDDINGS ERROR: {repr(e)}")

@app.post("/api/carga-productos/rapida")
async def carga_productos_rapida(request: Request, background_tasks: BackgroundTasks = None):
    """Carga rápida de productos: recibe lista de {nombre, precio_base, categoria, unidad}."""
    user = get_user_from_session(request)
    company_id = require_company_id(request)
    body = await request.json()
    productos = body.get("productos") or []
    if not productos:
        raise HTTPException(status_code=400, detail="No hay productos para cargar")

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        insertados = 0
        actualizados = 0
        errores = []

        # Generar sinónimos por batch
        names_list = [normalize_display_name(p["nombre"]) for p in productos if p.get("nombre")]
        synonyms_map = {}
        if openai_client:
            for i in range(0, len(names_list), 20):
                batch = names_list[i:i+20]
                try:
                    numbered = "\n".join(f"{j+1}. {n}" for j, n in enumerate(batch))
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
                    raw = (resp.choices[0].message.content or "{}").strip().replace("```json", "").replace("```", "").strip()
                    parsed_syns = json.loads(raw)
                    for k, v in parsed_syns.items():
                        if k.isdigit() and int(k)-1 < len(batch):
                            synonyms_map[batch[int(k)-1]] = v
                except Exception as e:
                    print(f"BATCH SYNONYMS ERROR (rapida): {repr(e)}")

        for p in productos:
            nombre = normalize_display_name(p.get("nombre", "").strip())
            if not nombre:
                continue
            try:
                precio = float(str(p.get("precio_base", 0)).replace("$", "").replace(",", ""))
            except Exception:
                errores.append(f"Precio inválido para '{nombre}'")
                continue
            if precio <= 0:
                errores.append(f"Precio debe ser > 0 para '{nombre}'")
                continue

            unidad = p.get("unidad", "Pieza") or "Pieza"
            name_norm = norm_name(nombre)
            auto_syn = synonyms_map.get(nombre, "")

            try:
                cur.execute(
                    """
                    INSERT INTO pricebook_items
                        (company_id, name, name_norm, unit, price, synonyms, source, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, 'rapida', now())
                    ON CONFLICT (company_id, name_norm)
                    DO UPDATE SET
                        name = EXCLUDED.name, unit = EXCLUDED.unit,
                        price = EXCLUDED.price,
                        synonyms = COALESCE(NULLIF(pricebook_items.synonyms, ''), EXCLUDED.synonyms),
                        source = 'rapida', updated_at = now()
                    RETURNING (xmax = 0) AS is_insert
                    """,
                    (company_id, nombre, name_norm, unidad, precio, auto_syn),
                )
                row = cur.fetchone()
                if row and row[0]:
                    insertados += 1
                else:
                    actualizados += 1
            except Exception as e:
                print(f"RAPIDA INSERT ERROR {nombre}: {repr(e)}")
                errores.append(f"Error con '{nombre}': {str(e)}")

        conn.commit()

        # Rebuild embeddings + context groups en background
        if background_tasks:
            background_tasks.add_task(_rebuild_embeddings_bg, company_id)
        else:
            try:
                rebuild_embeddings_for_company(conn, company_id)
                auto_generate_context_groups(conn, company_id)
            except Exception as e:
                print(f"EMBEDDINGS/CTX REBUILD ERROR (rapida): {repr(e)}")

        return {
            "ok": True,
            "productos_insertados": insertados,
            "productos_actualizados": actualizados,
            "errores": errores,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"CARGA RAPIDA ERROR: {repr(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


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
                select id, company_id, sku, name, unit, price, vat_rate, source, updated_at, created_at, bundle_size, coalesce(is_default, false)
                from pricebook_items
                where company_id = %s and (sku ilike %s or name ilike %s or name_norm ilike %s)
                order by name asc limit %s
                """,
                (company_id, like, like, like, limit),
            )
        else:
            cur.execute(
                "select id, company_id, sku, name, unit, price, vat_rate, source, updated_at, created_at, bundle_size, coalesce(is_default, false) from pricebook_items where company_id = %s order by name asc limit %s",
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
                "bundle_size": r[10],
                "is_default": bool(r[11]) if len(r) > 11 else False,
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
    """
    Genera variantes plural/singular para los tokens del nombre.
    Solo genera para tokens limpios (sin caracteres especiales).
    Returns empty list — we no longer auto-generate plural synonyms
    because they pollute the synonym field and embeddings without
    providing meaningful search value (the search already handles
    plurals via _singulars_es and phonetic matching).
    """
    # Disabled: auto-plurals cause more harm than good.
    # The search system already handles plural/singular via:
    # - _singulars_es() in synonym matching
    # - _phonetic() normalization
    # - ILIKE patterns with wildcards
    return []
    
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

    bundle_size = body.bundle_size
    if bundle_size is not None:
        bundle_size = int(bundle_size) if bundle_size > 0 else None

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pricebook_items (company_id, sku, name, name_norm, unit, price, vat_rate, synonyms, source, bundle_size, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (company_id, name_norm)
            DO UPDATE SET sku=EXCLUDED.sku, name=EXCLUDED.name, unit=EXCLUDED.unit,
                price=EXCLUDED.price, vat_rate=EXCLUDED.vat_rate,
                synonyms=COALESCE(NULLIF(pricebook_items.synonyms,''), EXCLUDED.synonyms),
                source=EXCLUDED.source, bundle_size=EXCLUDED.bundle_size, updated_at=now()
            RETURNING id
            """,
            (company_id, sku, name, name_norm, unit, price, vat_rate, synonyms, source, bundle_size),
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


class PricebookBulkBody(BaseModel):
    items: list  # list of {name, price, unit, category}


@app.post("/api/pricebook/bulk")
def pricebook_bulk_create(request: Request, body: PricebookBulkBody):
    """Bulk create products (used by onboarding wizard)."""
    company_id = require_company_id(request)
    if not body.items:
        raise HTTPException(status_code=400, detail="items vacío")
    if len(body.items) > 50:
        raise HTTPException(status_code=400, detail="Máximo 50 productos por lote")

    conn = None
    cur = None
    created = 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        for item in body.items:
            name = normalize_display_name((item.get("name") or "").strip())
            if not name:
                continue
            price = item.get("price")
            try:
                price = float(price) if price is not None else None
            except (ValueError, TypeError):
                price = None
            unit = (item.get("unit") or "Pieza").strip()
            name_n = norm_name(name)

            # Auto-generate plural/singular synonyms
            synonyms_list = _auto_plural_singular(name)
            synonyms = ", ".join(synonyms_list) if synonyms_list else ""

            cur.execute(
                """
                INSERT INTO pricebook_items (company_id, name, name_norm, unit, price, vat_rate, synonyms, source, updated_at, created_at)
                VALUES (%s, %s, %s, %s, %s, 0.16, %s, 'onboarding', now(), now())
                ON CONFLICT (company_id, name_norm)
                DO UPDATE SET price = EXCLUDED.price, unit = EXCLUDED.unit, updated_at = now()
                RETURNING id
                """,
                (company_id, name, name_n, unit, price, synonyms),
            )
            row = cur.fetchone()
            if row:
                created += 1
                try:
                    upsert_single_embedding(conn, company_id, row[0], name, "", unit, synonyms)
                except Exception as e:
                    print(f"BULK EMBEDDING ERROR for '{name}': {repr(e)}")
        conn.commit()
        # Auto-generate context groups after bulk product upload
        if created >= 3:
            try:
                cg_result = auto_generate_context_groups(conn, company_id)
                print(f"BULK CONTEXT GROUPS: {cg_result.get('status')}")
            except Exception as cge:
                print(f"BULK CONTEXT GROUPS ERROR: {repr(cge)}")
        return {"ok": True, "created": created}
    except HTTPException:
        raise
    except Exception as e:
        print(f"PRICEBOOK BULK ERROR: {repr(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error en carga masiva")
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
            "SELECT name, sku, unit, price, vat_rate, synonyms, bundle_size, coalesce(is_default, false) FROM pricebook_items WHERE id=%s AND company_id=%s",
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
        bundle_size = body.bundle_size if body.bundle_size is not None else row[6]
        if bundle_size is not None and bundle_size <= 0:
            bundle_size = None
        is_default = body.is_default if body.is_default is not None else row[7]

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
            SET name=%s, name_norm=%s, sku=%s, unit=%s, price=%s, vat_rate=%s, synonyms=%s, bundle_size=%s, is_default=%s, updated_at=now()
            WHERE id=%s AND company_id=%s
            RETURNING id
            """,
            (name, name_norm, sku, unit, price, vat_rate, synonyms, bundle_size, is_default, item_id, company_id),
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
                    "REGLA PRINCIPAL: Extrae el nombre del producto TAL COMO LO ESCRIBIÓ EL CLIENTE, "
                    "sin abreviar, sin simplificar, sin quitar palabras. "
                    "CRÍTICO: Las medidas (4.10, 6.35, 3.05) y calibres (cal 20, cal 22, cal 26) "
                    "son PARTE DEL NOMBRE del producto. NUNCA los separes ni los omitas. "
                    "Interpreta cantidades en texto: 'una'=1, 'un'=1, 'dos'=2, 'media'=0.5. "
                    "Solo corrige errores ortográficos obvios: 'tabla roca'='tablaroca', "
                    "'redemix'='redimix', 'takete'='taquete', 'flamer'='framer', 'durok'='durock'. "
                    "Ejemplos correctos:\n"
                    "'4 postes 4.10 cal 20' → {\"qty\": 4, \"product\": \"postes 4.10 cal 20\"}\n"
                    "'2 canal 4.10 cal 22' → {\"qty\": 2, \"product\": \"canal 4.10 cal 22\"}\n"
                    "'10 poste 6.35 calibre 26' → {\"qty\": 10, \"product\": \"poste 6.35 calibre 26\"}\n"
                    "'canal listón' → 'canal liston', "
                    "'angulo de amarre' → 'angulo de amarre', 'reborde jota' → 'reborde jota', "
                    "'pijas de tabla roca' → 'pijas tablaroca', 'alambre calibre 16' → 'alambre calibre 16'. "
                    "Responde SOLO JSON sin explicación: "
                    '[{"qty": 10, "product": "cemento"}, {"qty": 6, "product": "canal liston"}] '
                    "Si no hay productos claros, responde: []"
                )},
                {"role": "user", "content": user_text},
            ],
            temperature=0.1, max_tokens=600,
        )
        raw = (resp.choices[0].message.content or "[]").replace("```json", "").replace("```", "").strip()
        parsed = json.loads(raw)
        return [(int(it["qty"]), str(it["product"]).strip(), False) for it in parsed if it.get("qty") and it.get("product")]
    except Exception as e:
        print("NER ERROR:", repr(e))
        return []

def extract_qty_items_robust(text: str):
    t = (text or "").strip()
    # Strip invisible unicode chars (word joiners, zero-width spaces, etc.)
    # WhatsApp bullet lists inject \u2060 (word joiner) and \u200b (zero-width space)
    t = re.sub(r"[\u2060\u200b\u200c\u200d\ufeff\u00a0]", " ", t)
    t = re.sub(r"[•;]", "\n", t)
    # Convert "* " bullet markers to newlines (WhatsApp inline bullets)
    # Match * followed by a digit (e.g. "* 5 hojas") or * at line start
    t = re.sub(r"(?:^|\s)\*\s+(?=\d)", "\n", t)
    # Strip leading bullet markers: -, –, —, >, · at start of each line
    # Example: "-5 canal de amarre" → "5 canal de amarre"
    # "-Tornillo para tablaroca 300" → "Tornillo para tablaroca 300"
    t = re.sub(r"(?m)^\s*[-–—>·]+\s*", "", t)
    # Strip greeting prefixes BEFORE stripping request verbs
    # "Buenas tardes quiero 10 tablarocas" → "quiero 10 tablarocas" → "10 tablarocas"
    t = re.sub(r"^\s*(hola|hey|buenas?\s*(?:tardes?|noches?|dias?|días?)?|buenos?\s*(?:dias?|días?)|buen\s*(?:dia|día))\s*[,.]?\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^\s*(ocupo|necesito|quiero|quisiera|dame|deme|manda|mandame|mandeme|pasame|pásame|paseme|necesitamos|queremos|ocupamos|me puede dar|me pueden dar|me das|me mandas|favor de|necesito cotizar|quiero cotizar|solicito|solicita|solicitamos|solicitame)\s+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"^\s*(me\s+)?(puede[ns]?|podría[ns]?|podrías|podras|podrás|podra|podrá)\s+(cotizar|dar|mandar|pasar|solicitar|conseguir|enviar|preparar|armar|hacer)\s+(el\s+siguiente\s+material|la\s+siguiente\s+lista|lo\s+siguiente|estos?\s+materiales?)?\s*", "", t, flags=re.IGNORECASE)
    # Strip standalone preamble phrases: "el siguiente material", "la siguiente lista", etc.
    t = re.sub(r"^\s*(el\s+siguiente\s+material|la\s+siguiente\s+lista|lo\s+siguiente|estos?\s+materiales?|la\s+lista)\s+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(cotiza|cotización|cotizacion|precio|precios|por favor|porfa|pls)\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"(\d+)\s*/\s*(\d+)", r"\1_\2", t)
    t = re.sub(r"\s+y\s+(?=\d)", "\n", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+e\s+(?=\d)", "\n", t, flags=re.IGNORECASE)

    # Proteger specs de medida que aparecen DENTRO del nombre del producto
    # (después de al menos una palabra), NO al inicio de la línea (que serían cantidad + unidad empaque).
    # "rejas de 2 metros" → "rejas SPEC_2_metros"  (spec de producto, proteger)
    # "clavos de 3 pulgadas" → "clavos SPEC_3_pulgadas"  (spec de producto, proteger)
    # "5 kilos alambre" → no proteger (5 kilos es cantidad + empaque al inicio)
    _measure_units = r"(metros?|mts?|m|cm|centimetros?|pulgadas?|pulg|mm)"
    # Solo proteger si viene después de una letra (parte del nombre del producto)
    t = re.sub(rf"(?<=[a-záéíóúñ])\s+de\s+(\d+(?:\.\d+)?)\s*{_measure_units}\b", r" SPEC_\1_\2", t, flags=re.IGNORECASE)
    t = re.sub(rf"(?<=[a-záéíóúñ])\s+(\d+(?:\.\d+)?)\s*{_measure_units}\b", r" SPEC_\1_\2", t, flags=re.IGNORECASE)

    items = []
    lines = [l.strip() for l in re.split(r"[\n\r]+", t) if l.strip()]
    for line in lines:
        # Split by comma, then further split bare parts by " y " (when no digit follows)
        raw_parts = [p.strip() for p in line.split(",") if p.strip()]
        parts = []
        for rp in raw_parts:
            # Split "marco y cerradura" into ["marco", "cerradura"]
            # but NOT "2 postes y 3 canales" (those have digits)
            if not re.match(r"^\s*\d+\s+", rp):
                # No leading quantity — split by " y " to get individual products
                y_parts = re.split(r"\s+y\s+", rp, flags=re.IGNORECASE)
                parts.extend(y_parts)
            else:
                parts.append(rp)
        last_qty = 1  # default qty for items without explicit quantity
        for part in parts:
            # Split "5 cemento 3 varilla" into ["5 cemento", "3 varilla"]
            # but NOT "postes 635 calibre 26" (635 is a product spec, not qty)
            # Only split when digit is followed by a letter-word (product name)
            _spec_words = r"(?:cal(?:ibre)?|x|mm|cm|m|mts?|kg|pulgadas?|pulg|metros?|litros?|lts?|gal)"
            # Negative lookbehind on [xX×]: prevent splitting dimensions like
            # "3 mts x 1 mto .26" or "2 x 2" x 6 mts" where x separates dimensions
            sub_parts = re.split(rf'(?<=\S)(?<![xX×])\s+(?=\d+\s+(?!{_spec_words}\b)[a-záéíóúñ])', part.strip())
            for sub in sub_parts:
                # Restaurar specs protegidas: SPEC_2_metros → "2 metros"
                sub = re.sub(r"SPEC_(\d+(?:\.\d+)?)_(\w+)", r"\1 \2", sub)
                m = re.match(r"^\s*(\d+(?:\.\d+)?)\s+(.+)$", sub.strip())
                if m:
                    qty = int(float(m.group(1)))  # "175.00" → 175
                    last_qty = qty
                    prod = m.group(2).replace("_", "/").strip()
                else:
                    # Trailing qty: "Tornillo para tablaroca 300" → qty=300, prod="Tornillo para tablaroca"
                    # Also handles "Tiras de madera 7 mts" → qty=7, prod="Tiras de madera"
                    # (optional trailing unit like mts, metros, pzas, kg, etc.)
                    _m_trail = re.match(
                        r"^\s*([a-záéíóúñ].*?[a-záéíóúñ])\s+(\d{1,6})\s*(mts?|metros?|pzas?|piezas?|kg|kgs?|lts?|litros?|m|cm|mm|pulg|pulgadas?)?\s*$",
                        sub.strip(), flags=re.IGNORECASE,
                    )
                    if _m_trail and int(_m_trail.group(2)) >= 2:
                        qty = int(_m_trail.group(2))
                        last_qty = qty
                        prod = _m_trail.group(1).replace("_", "/").strip()
                    else:
                        # No leading quantity — inherit from last seen qty (or default 1)
                        qty = last_qty
                        prod = sub.strip().replace("_", "/")
                if not prod:
                    continue
                # "X y Y ... N paquete de cada uno/1" → N of each product
                # Ej: "Taquete y tornillo 1/4  1 paquete de cada 1" → 1 taquete 1/4 + 1 tornillo 1/4
                _de_cada_match = re.search(
                    r"\s+(\d+)\s*(?:paquetes?|piezas?|pzas?|unidades?|bolsas?|cajas?)?\s*de\s+cada\s*(?:uno|una|1)?\s*\d*\s*$",
                    prod, re.IGNORECASE
                )
                if _de_cada_match:
                    _de_cada_qty = int(_de_cada_match.group(1))
                    qty = _de_cada_qty
                    last_qty = qty
                    prod = prod[:_de_cada_match.start()].strip()

                # Split "tornillos y taquetes de ¼ de plástico" into separate products
                # Only split on " y " between alphabetic words (not specs like "6x1 y 8x2")
                _y_split = re.split(r"\s+y\s+(?=[a-záéíóúñ])", prod, flags=re.IGNORECASE)
                # If split produced multiple parts, check they each have an alpha word
                if len(_y_split) > 1 and all(re.search(r"[a-záéíóúñ]{3,}", p, re.IGNORECASE) for p in _y_split):
                    # Shared trailing spec: "tornillos y taquetes de ¼ de plástico"
                    # If the LAST part has a spec pattern and earlier parts don't, share it.
                    _last = _y_split[-1].strip()
                    _spec_pat = r"(?:\d+[/\-x×]\d+|\d+/\d+|\bcal\s*\d+|\b\d+\s*(?:mm|cm|pulg)\b)"
                    _last_spec_m = re.search(_spec_pat, _last, re.IGNORECASE)
                    if _last_spec_m:
                        _last_spec = _last_spec_m.group(0)
                        _shared = []
                        for _i, _p in enumerate(_y_split):
                            _p = _p.strip()
                            # Append spec to earlier parts that lack any spec
                            if _i < len(_y_split) - 1 and not re.search(_spec_pat, _p, re.IGNORECASE):
                                _p = f"{_p} {_last_spec}"
                            _shared.append(_p)
                        prod_list = _shared
                    else:
                        prod_list = _y_split
                else:
                    prod_list = [prod]
                for _yp in prod_list:
                    _yp = _yp.strip()
                    if not _yp:
                        continue
                    # ── Packaging multiplier: "2 bolsas de pija (200 pzas)" → qty=2×200=400
                    # Patterns: "(200 pzas)", "(200 piezas)", "(c/200)", "(200 c/u)"
                    _pkg_mult = re.search(
                        r"\(\s*(?:c\s*/\s*)?(\d+)\s*(?:pzas?|piezas?|pzs?|c\s*/\s*u)?\s*\)",
                        _yp, re.IGNORECASE
                    )
                    if not _pkg_mult:
                        # Also match "de 200 pzas" without parens (only at end of string)
                        _pkg_mult = re.search(
                            r"\bde\s+(\d+)\s+(?:pzas?|piezas?|pzs?)\s*$",
                            _yp, re.IGNORECASE
                        )
                    _already_multiplied = False
                    if _pkg_mult:
                        _mult_val = int(_pkg_mult.group(1))
                        if _mult_val > 1:
                            qty = qty * _mult_val
                            _already_multiplied = True
                            # Remove the multiplier text from product name
                            _yp = _yp[:_pkg_mult.start()] + _yp[_pkg_mult.end():]
                            _yp = _yp.strip()

                    # Detect bundle words BEFORE stripping them
                    # If already multiplied by explicit piece count, DON'T flag as bundle
                    # to avoid double-multiplication with bundle_size
                    if _already_multiplied:
                        _is_bun = False
                    else:
                        _is_bun = bool(re.search(r"\b(atados?|paquetes?|bultos?|cajas?|bolsas?)\b", _yp, re.IGNORECASE))
                    # Solo quitar unidades de EMPAQUE (no de medida/spec)
                    _packaging_re = r"\b(hojas?|piezas?|rollos?|bultos?|sacos?|atados?|paquetes?|costales?|cubetas?|bolsas?|botes?|latas?|tiras?|cajas?|cientos?|millares?|kilos?|kgs?|gramos?|litros?|lts?|galones?)\b"
                    _yp = re.sub(_packaging_re, "", _yp, flags=re.IGNORECASE).strip()
                    # Extract parenthetical hints as alternate search terms:
                    # "plafón registrable(galleta)" → "plafón registrable galleta"
                    # This helps when customers add brand/slang hints in parens.
                    _yp = re.sub(r"\s*\(([^)]+)\)\s*", r" \1 ", _yp)
                    _yp = re.sub(r"\s+", " ", _yp).strip()
                    _yp = re.sub(r"\bde\b", " ", _yp, flags=re.IGNORECASE).strip()
                    _yp = re.sub(r"\s+", " ", _yp).strip()
                    if _yp and qty > 0:
                        # Filter out bogus items that are just packaging words (e.g. "pzas.", "piezas")
                        _stripped = re.sub(r"[.\s]", "", _yp).lower()
                        _junk_words = {"pzas", "pzs", "pza", "pz", "piezas", "pieza", "unidades", "unidad"}
                        if _stripped in _junk_words:
                            continue
                        # Filter out preamble phrases that leaked through.
                        # If product is ONLY preamble/request verbs with no concrete product noun,
                        # drop it. Common leaked preambles: "podrías solicitar el siguiente material",
                        # "me podrías cotizar", "el siguiente material", etc.
                        _yp_lc = _yp.lower()
                        _preamble_markers = {
                            "podrias", "podría", "podrías", "solicitar", "material",
                            "materiales", "siguiente", "lista", "cotizar", "conseguir",
                            "enviar", "preparar", "mandar", "armar", "favor",
                        }
                        _yp_tokens = set(re.findall(r"[a-záéíóúñ]{3,}", _yp_lc))
                        # If >=70% of meaningful tokens are preamble markers, it's a phantom
                        if _yp_tokens and len(_yp_tokens & _preamble_markers) / len(_yp_tokens) >= 0.7:
                            print(f"PARSER FILTER: dropping phantom preamble item '{_yp}'")
                            continue
                        items.append((qty, _yp, _is_bun))
    return items

@app.post("/api/chat")
async def chat(req: ChatRequest, authorization: str = Header(default="")):
    app_id = (getattr(req, "app", None) or "cotizabot").lower().strip()
    user_text = (getattr(req, "message", None) or "").strip()

    if not user_text:
        return {"reply": "Escribe un mensaje para poder ayudarte."}

    if app_id == "cotizabot":
        qty, prod_query, _is_bundle3 = extract_qty_and_product(user_text)
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
                opciones += "\n\n✅ Responde con el número, ej: 1"
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


# ─────────────────────────────────────────────────────────────
# ONBOARDING endpoints
# ─────────────────────────────────────────────────────────────

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
        print("ONBOARDING MIGRATIONS: OK")
    except Exception as e:
        print(f"ONBOARDING MIGRATION ERROR: {repr(e)}")
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


class EmpresaPerfilBody(BaseModel):
    giro: Optional[str] = None
    ciudad: Optional[str] = None
    descripcion: Optional[str] = None
    whatsapp: Optional[str] = None
    empresa_nombre: Optional[str] = None
    horario_semana: Optional[str] = None
    horario_sabado: Optional[str] = None
    horario_domingo: Optional[str] = None


@app.put("/api/empresa/perfil")
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
        _run_pricebook_migrations(conn)
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
        return {"ok": True, "tenant_context": tenant_context}
    except HTTPException:
        raise
    except Exception as e:
        print(f"EMPRESA PERFIL ERROR: {repr(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error guardando perfil")
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.post("/api/empresa/onboarding-complete")
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
        print(f"ONBOARDING COMPLETE ERROR: {repr(e)}")
        raise HTTPException(status_code=500, detail="Error actualizando onboarding")
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/empresa/onboarding-status")
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
        print(f"ONBOARDING STATUS ERROR: {repr(e)}")
        return {"ok": True, "onboarding_completed": False}
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# WHATSAPP EMBEDDED SIGNUP — Auto-connect WhatsApp via Meta OAuth
# ═══════════════════════════════════════════════════════════════════════════════

_META_APP_ID = "1461694011992339"
_META_APP_SECRET = "28989d2a761e5be1b8ff4eb77c795715"
_META_GRAPH_VERSION = "v21.0"
_META_GRAPH_URL = f"https://graph.facebook.com/{_META_GRAPH_VERSION}"

class EmbeddedSignupBody(BaseModel):
    code: str
    phone_number_id: Optional[str] = None
    waba_id: Optional[str] = None

@app.post("/api/whatsapp/embedded-signup")
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
    print(f"EMBEDDED SIGNUP: exchanging code for company={company_id}")
    token_resp = http_requests.get(f"{_META_GRAPH_URL}/oauth/access_token", params={
        "client_id": _META_APP_ID,
        "client_secret": _META_APP_SECRET,
        "code": code,
    })
    if token_resp.status_code != 200:
        print(f"EMBEDDED SIGNUP TOKEN ERROR: {token_resp.status_code} {token_resp.text}")
        raise HTTPException(status_code=400, detail="Error al obtener token de Meta")
    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        print(f"EMBEDDED SIGNUP: no access_token in response: {token_data}")
        raise HTTPException(status_code=400, detail="No se obtuvo token de acceso")

    print(f"EMBEDDED SIGNUP: got access token for company={company_id}")

    # Step 2: If we don't have phone_number_id/waba_id from session, try to get from API
    if not waba_id:
        # Get shared WABAs
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
            print(f"EMBEDDED SIGNUP: resolved waba_id={waba_id} from debug_token")
        except Exception as e:
            print(f"EMBEDDED SIGNUP: debug_token error: {repr(e)}")

    if not phone_number_id and waba_id:
        # Get phone numbers for this WABA
        try:
            phones_resp = http_requests.get(
                f"{_META_GRAPH_URL}/{waba_id}/phone_numbers",
                params={"access_token": access_token}
            )
            phones_data = phones_resp.json().get("data", [])
            if phones_data:
                phone_number_id = phones_data[0].get("id")
                print(f"EMBEDDED SIGNUP: resolved phone_number_id={phone_number_id}")
        except Exception as e:
            print(f"EMBEDDED SIGNUP: phone_numbers error: {repr(e)}")

    if not phone_number_id:
        print(f"EMBEDDED SIGNUP: could not resolve phone_number_id")
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
        print(f"EMBEDDED SIGNUP: phone={phone_display} name={verified_name}")
    except Exception as e:
        print(f"EMBEDDED SIGNUP: phone info error: {repr(e)}")

    # Step 4: Subscribe app to WABA (to receive webhooks)
    if waba_id:
        try:
            sub_resp = http_requests.post(
                f"{_META_GRAPH_URL}/{waba_id}/subscribed_apps",
                params={"access_token": access_token}
            )
            print(f"EMBEDDED SIGNUP: subscribe app to WABA: {sub_resp.status_code} {sub_resp.text}")
        except Exception as e:
            print(f"EMBEDDED SIGNUP: subscribe error: {repr(e)}")

    # Step 5: Register phone number for Cloud API
    try:
        reg_resp = http_requests.post(
            f"{_META_GRAPH_URL}/{phone_number_id}/register",
            json={"messaging_product": "whatsapp", "pin": "123456"},
            params={"access_token": access_token}
        )
        print(f"EMBEDDED SIGNUP: register phone: {reg_resp.status_code} {reg_resp.text}")
    except Exception as e:
        print(f"EMBEDDED SIGNUP: register phone error: {repr(e)}")

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
        print(f"EMBEDDED SIGNUP: SUCCESS company={company_id} channel={channel_id} phone={phone_display}")
        return {
            "ok": True,
            "channel_id": channel_id,
            "phone_number_id": phone_number_id,
            "waba_id": waba_id,
            "phone_display": phone_display,
        }
    except Exception as e:
        conn.rollback()
        print(f"EMBEDDED SIGNUP DB ERROR: {repr(e)}")
        raise HTTPException(status_code=500, detail="Error al guardar la configuración")
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN PANEL — God mode para CotizaExpress (solo cuentas admin)
# ═══════════════════════════════════════════════════════════════════════════════

ADMIN_EMAILS = {"ealejandro.robledo@gmail.com"}

def _require_admin(request: Request):
    """Verifica que el usuario logueado sea admin de CotizaExpress."""
    u = get_user_from_session(request)
    if u["email"].lower() not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="No autorizado")
    return u


@app.delete("/api/admin/quote-state/{wa_from}")
def admin_clear_quote_state(wa_from: str, request: Request):
    """Clear a stuck quote state for a given WhatsApp number."""
    _require_admin(request)
    # Use Aceromax company_id by default (only tenant currently)
    company_id = "30208e3c-70c6-4203-97d9-172fad7d3c75"
    clear_quote_state(company_id, wa_from)
    return {"ok": True, "cleared": wa_from}


@app.get("/api/admin/stats/overview")
def admin_stats_overview(request: Request):
    """Dashboard overview: totales, búsquedas recientes, tasa de éxito."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()

        # Total companies activas (con productos)
        cur.execute("SELECT COUNT(DISTINCT company_id) FROM pricebook_items")
        total_companies = cur.fetchone()[0]

        # Total productos
        cur.execute("SELECT COUNT(*) FROM pricebook_items")
        total_products = cur.fetchone()[0]

        # Query events stats (últimos 7 días)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE search_status = 'found') as found,
                COUNT(*) FILTER (WHERE search_status = 'ambiguous') as ambiguous,
                COUNT(*) FILTER (WHERE search_status = 'not_found') as not_found
            FROM query_events
            WHERE created_at > NOW() - INTERVAL '7 days'
        """)
        row = cur.fetchone()
        total_queries = row[0] or 0
        found_queries = row[1] or 0
        ambiguous_queries = row[2] or 0
        not_found_queries = row[3] or 0
        success_rate = round(found_queries / max(total_queries, 1) * 100, 1)

        # Queries hoy
        cur.execute("""
            SELECT COUNT(*) FROM query_events
            WHERE created_at > NOW() - INTERVAL '1 day'
        """)
        queries_today = cur.fetchone()[0] or 0

        # Total jerga global
        cur.execute("SELECT COUNT(*) FROM diccionario_jerga_global")
        total_jerga = cur.fetchone()[0]

        # Jerga auto-promovida
        cur.execute("""
            SELECT COUNT(*) FROM diccionario_jerga_global
            WHERE is_protected = TRUE AND source != 'seed'
        """)
        auto_promoted = cur.fetchone()[0]

        # Total conversaciones WhatsApp
        cur.execute("SELECT COUNT(*) FROM conversations")
        total_conversations = cur.fetchone()[0]

        return {
            "ok": True,
            "stats": {
                "total_companies": total_companies,
                "total_products": total_products,
                "total_conversations": total_conversations,
                "total_jerga": total_jerga,
                "auto_promoted_jerga": auto_promoted,
                "queries_7d": {
                    "total": total_queries,
                    "found": found_queries,
                    "ambiguous": ambiguous_queries,
                    "not_found": not_found_queries,
                    "success_rate": success_rate,
                },
                "queries_today": queries_today,
            },
        }
    except Exception as e:
        print(f"ADMIN STATS ERROR: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/admin/stats/top-errors")
def admin_top_errors(request: Request, days: int = 7, limit: int = 20):
    """Top búsquedas que terminaron en not_found — dónde falla el bot."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT
                original_text,
                normalized_text,
                normalization_source,
                COUNT(*) as count,
                MIN(created_at) as first_seen,
                MAX(created_at) as last_seen
            FROM query_events
            WHERE search_status = 'not_found'
              AND created_at > NOW() - INTERVAL '%s days'
            GROUP BY original_text, normalized_text, normalization_source
            ORDER BY count DESC
            LIMIT %s
        """, (days, limit))
        rows = cur.fetchall()
        errors = []
        for r in rows:
            errors.append({
                "original_text": r[0],
                "normalized_text": r[1],
                "normalization_source": r[2],
                "count": r[3],
                "first_seen": r[4].isoformat() if r[4] else None,
                "last_seen": r[5].isoformat() if r[5] else None,
            })
        return {"ok": True, "errors": errors}
    except Exception as e:
        print(f"ADMIN TOP ERRORS: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/admin/stats/top-searches")
def admin_top_searches(request: Request, days: int = 7, limit: int = 30):
    """Top búsquedas exitosas — qué piden más los clientes."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT
                original_text,
                normalized_text,
                matched_item_name,
                search_paso,
                COUNT(*) as count,
                ROUND(AVG(confidence_score)::numeric, 2) as avg_confidence
            FROM query_events
            WHERE search_status = 'found'
              AND created_at > NOW() - INTERVAL '%s days'
            GROUP BY original_text, normalized_text, matched_item_name, search_paso
            ORDER BY count DESC
            LIMIT %s
        """, (days, limit))
        rows = cur.fetchall()
        searches = []
        for r in rows:
            searches.append({
                "original_text": r[0],
                "normalized_text": r[1],
                "matched_item": r[2],
                "paso": r[3],
                "count": r[4],
                "avg_confidence": float(r[5]) if r[5] else None,
            })
        return {"ok": True, "searches": searches}
    except Exception as e:
        print(f"ADMIN TOP SEARCHES: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/admin/stats/by-company")
def admin_stats_by_company(request: Request, days: int = 7):
    """Stats por empresa — quién usa más, quién tiene más errores."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT
                qe.company_id,
                c.name as company_name,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE qe.search_status = 'found') as found,
                COUNT(*) FILTER (WHERE qe.search_status = 'not_found') as not_found,
                COUNT(*) FILTER (WHERE qe.search_status = 'ambiguous') as ambiguous
            FROM query_events qe
            LEFT JOIN companies c ON c.id = qe.company_id
            WHERE qe.created_at > NOW() - INTERVAL '%s days'
            GROUP BY qe.company_id, c.name
            ORDER BY total DESC
        """, (days,))
        rows = cur.fetchall()
        companies = []
        for r in rows:
            total = r[2] or 1
            companies.append({
                "company_id": str(r[0]),
                "name": r[1] or "Sin nombre",
                "total": r[2],
                "found": r[3],
                "not_found": r[4],
                "ambiguous": r[5],
                "success_rate": round((r[3] or 0) / total * 100, 1),
            })
        return {"ok": True, "companies": companies}
    except Exception as e:
        print(f"ADMIN BY COMPANY: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/admin/jerga")
def admin_jerga_list(request: Request, page: int = 1, per_page: int = 50):
    """Lista la jerga global con stats de uso."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        offset = (max(page, 1) - 1) * per_page

        cur.execute("SELECT COUNT(*) FROM diccionario_jerga_global")
        total = cur.fetchone()[0]

        cur.execute("""
            SELECT
                termino_original,
                termino_normalizado,
                is_protected,
                usage_count,
                success_count,
                CASE WHEN usage_count > 0
                     THEN ROUND((success_count::numeric / usage_count) * 100, 1)
                     ELSE 0 END as confidence,
                industry,
                source
            FROM diccionario_jerga_global
            ORDER BY usage_count DESC, termino_original ASC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        rows = cur.fetchall()
        jerga = []
        for r in rows:
            jerga.append({
                "termino_original": r[0],
                "termino_normalizado": r[1],
                "is_protected": r[2],
                "usage_count": r[3],
                "success_count": r[4],
                "confidence": float(r[5]) if r[5] else 0,
                "industry": r[6],
                "source": r[7],
            })
        return {"ok": True, "jerga": jerga, "total": total, "page": page, "per_page": per_page}
    except Exception as e:
        print(f"ADMIN JERGA LIST: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


class AdminJergaUpdate(BaseModel):
    termino_original: str
    termino_normalizado: Optional[str] = None
    is_protected: Optional[bool] = None
    industry: Optional[str] = None

@app.put("/api/admin/jerga")
def admin_jerga_update(request: Request, body: AdminJergaUpdate):
    """Actualizar/proteger/corregir un término de jerga."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        updates = []
        params = []
        if body.termino_normalizado is not None:
            updates.append("termino_normalizado = %s")
            params.append(body.termino_normalizado.strip().lower())
        if body.is_protected is not None:
            updates.append("is_protected = %s")
            params.append(body.is_protected)
        if body.industry is not None:
            updates.append("industry = %s")
            params.append(body.industry.strip() or None)
        if not updates:
            raise HTTPException(status_code=400, detail="Nada que actualizar")
        updates.append("source = 'admin'")
        params.append(body.termino_original.strip().lower())
        cur.execute(
            f"UPDATE diccionario_jerga_global SET {', '.join(updates)} WHERE termino_original = %s",
            tuple(params),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Término no encontrado")
        return {"ok": True, "updated": body.termino_original}
    except HTTPException:
        raise
    except Exception as e:
        print(f"ADMIN JERGA UPDATE: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


class AdminJergaCreate(BaseModel):
    termino_original: str
    termino_normalizado: str
    industry: Optional[str] = None

@app.post("/api/admin/jerga")
def admin_jerga_create(request: Request, body: AdminJergaCreate):
    """Crear nuevo término de jerga protegido manualmente."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "INSERT INTO diccionario_jerga_global "
            "(termino_original, termino_normalizado, is_protected, source, industry) "
            "VALUES (%s, %s, TRUE, 'admin', %s) "
            "ON CONFLICT (termino_original) DO UPDATE "
            "SET termino_normalizado = EXCLUDED.termino_normalizado, "
            "    is_protected = TRUE, source = 'admin', "
            "    industry = EXCLUDED.industry",
            (body.termino_original.strip().lower(),
             body.termino_normalizado.strip().lower(),
             (body.industry or "").strip() or None),
        )
        return {"ok": True, "created": body.termino_original}
    except Exception as e:
        print(f"ADMIN JERGA CREATE: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.delete("/api/admin/jerga/{termino}")
def admin_jerga_delete(request: Request, termino: str):
    """Eliminar término de jerga global."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "DELETE FROM diccionario_jerga_global WHERE termino_original = %s",
            (termino.strip().lower(),),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Término no encontrado")
        return {"ok": True, "deleted": termino}
    except HTTPException:
        raise
    except Exception as e:
        print(f"ADMIN JERGA DELETE: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ── Per-tenant jerga local (equivalencias de marca / sinónimos locales) ──────

class JergaLocalBody(BaseModel):
    termino_original: str
    termino_normalizado: str

@app.get("/api/company/jerga")
def company_jerga_list(request: Request):
    """Lista la jerga local del tenant (equivalencias de marcas, sinónimos propios)."""
    company_id = require_company_id(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT termino_original, termino_normalizado, source, usage_count "
            "FROM diccionario_jerga_local WHERE company_id = %s "
            "ORDER BY termino_original",
            (company_id,),
        )
        rows = cur.fetchall()
        return {"ok": True, "jerga": [
            {"termino_original": r[0], "termino_normalizado": r[1],
             "source": r[2] or "manual", "usage_count": r[3] or 0}
            for r in rows
        ]}
    except Exception as e:
        print(f"JERGA LOCAL LIST: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()

class BrandSuggestBody(BaseModel):
    marcas_propias: str = ""
    giro: str = ""

@app.post("/api/company/suggest-brands")
def suggest_competitor_brands(request: Request, body: BrandSuggestBody):
    """Use AI to suggest competitor brands based on the tenant's own brands."""
    require_company_id(request)
    if not openai_client:
        return {"suggestions": []}
    marcas = (body.marcas_propias or "").strip()
    giro = (body.giro or "").strip()
    if not marcas and not giro:
        return {"suggestions": []}
    try:
        prompt = (
            "Eres experto en el mercado de materiales de construcción, ferretería y distribución en México.\n\n"
            f"Este negocio vende estas marcas: {marcas}\n"
        )
        if giro:
            prompt += f"Giro del negocio: {giro}\n"
        prompt += (
            "\nPara CADA marca que vende, lista las marcas COMPETIDORAS directas en México "
            "(marcas que venden productos equivalentes/similares). "
            "Incluye también los nombres comerciales de productos específicos de esas marcas "
            "que los clientes podrían usar como sinónimo.\n\n"
            "Ejemplos:\n"
            "- USG/Tablaroca → competidores: Panel Rey (productos: Lightrey, MR Panel Rey, Volcanrey)\n"
            "- Redimix USG → competidores: Knauf (Readyfix), Panel Rey (Compuesto PR)\n"
            "- Coflex → competidores: Rugo, Nacobre\n"
            "- Truper → competidores: Surtej, Pretul, Surtek\n\n"
            "Responde SOLO con una lista de marcas/nombres separados por coma, sin explicaciones. "
            "No repitas las marcas que el negocio ya vende. "
            "Máximo 15 sugerencias, las más relevantes primero."
        )
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=200,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Parse comma-separated list, clean up
        suggestions = [s.strip() for s in raw.split(",") if s.strip()]
        # Remove any that match the tenant's own brands
        _own = set(m.strip().lower() for m in marcas.split(",") if m.strip())
        suggestions = [s for s in suggestions if s.lower() not in _own]
        return {"suggestions": suggestions[:15]}
    except Exception as e:
        print(f"BRAND SUGGEST ERROR: {repr(e)}")
        return {"suggestions": []}


@app.post("/api/company/jerga")
def company_jerga_create(request: Request, body: JergaLocalBody):
    """Crear/actualizar equivalencia local."""
    company_id = require_company_id(request)
    orig = body.termino_original.strip().lower()
    norm = body.termino_normalizado.strip()
    if not orig or not norm:
        raise HTTPException(status_code=400, detail="Ambos campos son requeridos")
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "INSERT INTO diccionario_jerga_local "
            "(company_id, termino_original, termino_normalizado, source, usage_count) "
            "VALUES (%s, %s, %s, 'manual', 0) "
            "ON CONFLICT (company_id, termino_original) DO UPDATE "
            "SET termino_normalizado = EXCLUDED.termino_normalizado, source = 'manual'",
            (company_id, orig, norm),
        )
        return {"ok": True, "termino_original": orig, "termino_normalizado": norm}
    except Exception as e:
        print(f"JERGA LOCAL CREATE: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.delete("/api/company/jerga/{termino}")
def company_jerga_delete(request: Request, termino: str):
    """Eliminar equivalencia local."""
    company_id = require_company_id(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(
            "DELETE FROM diccionario_jerga_local WHERE company_id = %s AND termino_original = %s",
            (company_id, termino.strip().lower()),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Término no encontrado")
        return {"ok": True, "deleted": termino}
    except HTTPException:
        raise
    except Exception as e:
        print(f"JERGA LOCAL DELETE: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/admin/query-log")
def admin_query_log(request: Request, days: int = 1, limit: int = 100,
                    status: Optional[str] = None, company_id: Optional[str] = None):
    """Log detallado de búsquedas recientes."""
    _require_admin(request)
    conn = None; cur = None
    try:
        conn = get_conn(); cur = conn.cursor()
        where = ["qe.created_at > NOW() - INTERVAL '%s days'"]
        params = [days]
        if status:
            where.append("qe.search_status = %s")
            params.append(status)
        if company_id:
            where.append("qe.company_id = %s::uuid")
            params.append(company_id)
        params.append(limit)

        cur.execute(f"""
            SELECT
                qe.original_text,
                qe.cleaned_text,
                qe.normalized_text,
                qe.normalization_source,
                qe.matched_item_name,
                qe.search_status,
                qe.search_paso,
                qe.confidence_score,
                qe.created_at,
                c.name as company_name
            FROM query_events qe
            LEFT JOIN companies c ON c.id = qe.company_id
            WHERE {' AND '.join(where)}
            ORDER BY qe.created_at DESC
            LIMIT %s
        """, tuple(params))
        rows = cur.fetchall()
        events = []
        for r in rows:
            events.append({
                "original_text": r[0],
                "cleaned_text": r[1],
                "normalized_text": r[2],
                "normalization_source": r[3],
                "matched_item": r[4],
                "status": r[5],
                "paso": r[6],
                "confidence": float(r[7]) if r[7] else None,
                "created_at": r[8].isoformat() if r[8] else None,
                "company": r[9],
            })
        return {"ok": True, "events": events}
    except Exception as e:
        print(f"ADMIN QUERY LOG: {repr(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()
