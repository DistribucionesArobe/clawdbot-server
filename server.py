from prompts_cotizabot import COTIZABOT_SYSTEM_PROMPT
from fastapi.background import BackgroundTasks
import asyncio
import json
import logging
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

# ── Logging configuration ──
_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cotizaexpress")

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
# NOTE: app.include_router() calls are after router imports (~line 715)

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
            log.error("SILENCE LOOP ERROR:", repr(_e))

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
            WHERE updated_at < now() - (%s || ' minutes')::interval
            RETURNING company_id::text, wa_from
            """, (str(CONVERSATION_DEATH_MIN),)
        )
        killed = cur.fetchall() or []
        conn.commit()
        cur.close()
        conn.close()
        for company_id, wa_from in killed:
            log.info(f"CONVERSATION DEATH: company={company_id} client={wa_from} idle>{CONVERSATION_DEATH_MIN}min — state cleared")
    except Exception as _e:
        log.error("CONVERSATION DEATH ERROR:", repr(_e))

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
            WHERE wqs.updated_at < now() - (%s || ' minutes')::interval
              AND wqs.updated_at > now() - interval '2 hours'
            """, (str(SILENCE_THRESHOLD_MIN),)
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
                log.info(f"SILENCE ESCALATION: company={company_id} client={wa_from} silent>{SILENCE_THRESHOLD_MIN}min")
            except Exception as _ne:
                log.error("SILENCE NOTIFY ERROR:", repr(_ne))
    except Exception as _e:
        log.error("SILENCE ESCALATION RUN ERROR:", repr(_e))

@app.on_event("startup")
async def _start_silence_loop():
    asyncio.create_task(_silence_escalation_loop())
    log.info(f"Silence escalation loop started (check every {SILENCE_CHECK_INTERVAL_SEC}s, threshold {SILENCE_THRESHOLD_MIN} min)")

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

from middleware import register_middleware
register_middleware(app)

# -------------------------

# Basic endpoints

# -------------------------

# -------------------------
# Config
# -------------------------
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

# API_KEY_PREFIX_LEN, SESSION_COOKIE_NAME, SESSION_TTL_DAYS imported from auth.py
DEFAULT_COMPANY_ID = (os.getenv("DEFAULT_COMPANY_ID") or "").strip()
# WA_LIMIT_COMPLETE, WA_LIMIT_PRO, WA_CONVERSATION_WINDOW_HOURS imported from queries.py


# -------------------------
from auth import (
    hash_password, verify_password, hash_api_key, api_key_prefix,
    create_session, get_user_from_session, require_company_id,
    get_company_from_bearer, SESSION_COOKIE_NAME, SESSION_TTL_DAYS,
)

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

def search_pricebook_candidates(conn, company_id: str, q: str, limit: int = 10):
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
            log.error("CART RENDER SAVE QUOTE ERROR:", repr(e))

    # Only show "pagar" if the company has payment data configured
    pagar_txt = ""
    if company_id:
        try:
            _conn_p = get_conn()
            _cur_p = _conn_p.cursor()
            _cur_p.execute(
                "SELECT bank_clabe, bank_account_number, mercadopago_url FROM companies WHERE id=%s",
                (company_id,),
            )
            _pay_row = _cur_p.fetchone()
            _cur_p.close()
            _conn_p.close()
            if _pay_row and ((_pay_row[0] or "").strip() or (_pay_row[1] or "").strip() or (_pay_row[2] or "").strip()):
                pagar_txt = "\n\n💳 Escribe *pagar* y te mandamos datos bancarios o link para pago con tarjeta."
        except Exception:
            pass

    return (
        "Cotización:\n"
        + "\n".join(lines)
        + descuento_txt
        + f"\n\n*Total: ${total_final:,.0f}* (IVA incluido)"
        + folio_txt
        + pagar_txt
    )

# api_key_prefix and hash_api_key imported from auth.py
api_key_hash = hash_api_key  # alias for backward compat

# -------------------------
# DB
# -------------------------
# save_search_miss imported from queries.py

from db import get_conn, print_db_fingerprint

print_db_fingerprint()


import migrations
from routes.pricebook import router as pricebook_router
from routes.pagos import router as pagos_router
from routes.company import router as company_router
from routes.admin import router as admin_router
from routes.empresa import router as empresa_router
from routes.whatsapp import router as whatsapp_router

app.include_router(pricebook_router)
app.include_router(pagos_router)
app.include_router(company_router)
app.include_router(admin_router)
app.include_router(empresa_router)
app.include_router(whatsapp_router)


# ── Contact form endpoint ─────────────────────────────────────────────────────

class ContactoBody(BaseModel):
    nombre: str = ""
    email: str = ""
    telefono: str = ""
    mensaje: str = ""

@app.post("/api/contacto")
def api_contacto(body: ContactoBody):
    """Receive contact form submission, store in DB, notify owner via WhatsApp."""
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contact_leads (
                id SERIAL PRIMARY KEY,
                nombre TEXT,
                email TEXT,
                telefono TEXT,
                mensaje TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        cur.execute(
            "INSERT INTO contact_leads (nombre, email, telefono, mensaje) VALUES (%s, %s, %s, %s) RETURNING id",
            (body.nombre.strip(), body.email.strip(), body.telefono.strip(), body.mensaje.strip()),
        )
        lead_id = cur.fetchone()[0]
        conn.commit()

        # Notify owner via WhatsApp (best effort)
        _owner_wa = os.environ.get("OWNER_WHATSAPP", "528130850381")
        try:
            from whatsapp_api import send_whatsapp_message_twilio
            notif = (
                f"📩 Nuevo lead de contacto (#{ lead_id})\n\n"
                f"Nombre: {body.nombre}\n"
                f"Email: {body.email}\n"
                f"Tel: {body.telefono}\n"
                f"Mensaje: {body.mensaje[:200]}"
            )
            send_whatsapp_message_twilio(f"whatsapp:+{_owner_wa}", notif)
        except Exception as we:
            log.warning("CONTACTO WA NOTIFY ERROR: %s", repr(we))

        # Notify owner via email (best effort)
        _smtp_host = os.environ.get("SMTP_HOST", "mail.privateemail.com")
        _smtp_port = int(os.environ.get("SMTP_PORT", "465"))
        _smtp_user = os.environ.get("SMTP_USER", "")
        _smtp_pass = os.environ.get("SMTP_PASS", "")
        if _smtp_user and _smtp_pass:
            try:
                import smtplib
                from email.mime.text import MIMEText
                from email.mime.multipart import MIMEMultipart

                msg = MIMEMultipart()
                msg["From"] = _smtp_user
                msg["To"] = _smtp_user  # send to self
                msg["Subject"] = f"Nuevo lead CotizaBot #{lead_id} - {body.nombre}"
                email_body = (
                    f"Nuevo contacto desde cotizaexpress.com\n\n"
                    f"Nombre: {body.nombre}\n"
                    f"Email: {body.email}\n"
                    f"Teléfono: {body.telefono}\n\n"
                    f"Mensaje:\n{body.mensaje}\n\n"
                    f"---\nLead #{lead_id}"
                )
                msg.attach(MIMEText(email_body, "plain", "utf-8"))

                with smtplib.SMTP_SSL(_smtp_host, _smtp_port) as smtp:
                    smtp.login(_smtp_user, _smtp_pass)
                    smtp.send_message(msg)
                log.info("CONTACTO EMAIL SENT to %s", _smtp_user)
            except Exception as me:
                log.warning("CONTACTO EMAIL ERROR: %s", repr(me))

        return {"ok": True, "id": lead_id}
    except Exception as e:
        log.error("CONTACTO ERROR: %s", repr(e))
        if conn:
            conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        if cur: cur.close()
        if conn: conn.close()


# Run migrations at startup (idempotent)
try:
    _mig_conn = get_conn()
    migrations.run_all(_mig_conn)
    _mig_conn.close()
except Exception as e:
    log.error(f"MIGRATION STARTUP ERROR: {repr(e)}")

# Seed jerga_global con términos críticos al iniciar
try:
    _seed_conn = get_conn()
    seed_jerga_global(_seed_conn)
    _seed_conn.close()
except Exception as e:
    log.error(f"SEED STARTUP ERROR: {repr(e)}")

from queries import (
    get_company_by_twilio_number, get_company_by_phone_number_id,
    get_quote_state, upsert_quote_state, clear_quote_state,
    save_quote, save_search_miss, log_message,
    get_company_plan_code, get_plan_limit,
    get_monthly_usage, increment_monthly_usage, track_conversation_if_new,
    WA_LIMIT_COMPLETE, WA_LIMIT_PRO, WA_CONVERSATION_WINDOW_HOURS,
)

from whatsapp_api import (
    send_whatsapp_text, send_whatsapp_list, send_whatsapp_list_sections,
    download_whatsapp_media, extract_text_from_image,
    normalize_mx_phone, notify_owner_escalation, notify_owner_comprobante,
)
# Backward compat alias
_normalize_mx_phone = normalize_mx_phone

# -------------------------
# Sessions
# -------------------------
# Session management imported from auth.py:
# create_session, get_user_from_session, require_company_id, get_company_from_bearer

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

# PricebookItemCreateBody, PricebookItemUpdateBody → routes/pricebook.py


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

# ── Per-user message accumulator for rapid-fire messages ─────────────────
# When a client sends 15 product lines as individual WhatsApp messages,
# each arrives as a separate webhook. Without batching, we'd process each
# one independently causing race conditions, duplicate responses, and chaos.
#
# Strategy:
#   1. Each webhook extracts msg data and appends to an in-memory queue
#   2. A per-user asyncio.Lock prevents concurrent processing
#   3. Before processing, wait BATCH_WAIT_SECS for more messages to arrive
#   4. Combine all queued text messages into one and process as a single call
#   5. Interactive (button) clicks and images bypass batching — processed immediately
import time as _time_mod

_BATCH_WAIT_SECS = 3.0           # seconds to wait for more messages
_BATCH_MAX_WAIT_SECS = 12.0      # absolute max wait from first message in batch
_user_locks: dict = {}            # (company_id, phone) → asyncio.Lock
_user_msg_queues: dict = {}       # (company_id, phone) → [{"text":..., "ts":...}, ...]
_user_batch_first_ts: dict = {}   # (company_id, phone) → timestamp of first msg in batch

def _get_user_lock(company_id: str, phone: str) -> asyncio.Lock:
    key = (company_id, phone)
    if key not in _user_locks:
        _user_locks[key] = asyncio.Lock()
    # Periodic cleanup of stale locks (>30 min unused)
    if len(_user_locks) > 200:
        _now = _time_mod.time()
        stale = [k for k in list(_user_locks.keys())
                 if k not in _user_msg_queues and k not in _user_batch_first_ts]
        for k in stale[:50]:
            _user_locks.pop(k, None)
    return _user_locks[key]


def _queue_message(company_id: str, phone: str, text: str):
    """Append a text message to the user's queue for batching."""
    key = (company_id, phone)
    if key not in _user_msg_queues:
        _user_msg_queues[key] = []
    _user_msg_queues[key].append({"text": text, "ts": _time_mod.time()})
    if key not in _user_batch_first_ts:
        _user_batch_first_ts[key] = _time_mod.time()


def _drain_queue(company_id: str, phone: str) -> list:
    """Take all queued messages for this user and clear the queue."""
    key = (company_id, phone)
    msgs = _user_msg_queues.pop(key, [])
    _user_batch_first_ts.pop(key, None)
    return msgs


def _reply_text_for_log(r) -> str:
    if isinstance(r, dict):
        rtype = r.get("type", "")
        if rtype in ("text_then_list_sections", "text_then_buttons"):
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


def _send_reply(company, from_phone, reply):
    """Send a reply dict/str to the user via WhatsApp."""
    if not reply:
        return

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


def _extract_msg_content(msg, company):
    """
    Extract text from a WhatsApp message object.
    Returns (text, msg_type, should_batch, early_reply).
    - should_batch: True if this message should be accumulated with others
    - early_reply: if set, send this reply immediately and skip further processing
    """
    msg_type = msg.get("type", "text")
    from_phone = msg.get("from")
    text = ""
    early_reply = None

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
                log.error("COMPROBANTE NOTIFY ERROR:", repr(e))
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
            return "", msg_type, False, "HANDLED"

        if image_id and company.get("wa_api_key"):
            try:
                img_bytes = download_whatsapp_media(image_id, company["wa_api_key"])
                extracted = extract_text_from_image(img_bytes)
                if extracted:
                    text = f"{caption}\n{extracted}".strip() if caption else extracted
                    log.info("IMAGE EXTRACTED:", text[:200])
                else:
                    send_whatsapp_text(
                        wa_api_key=company["wa_api_key"],
                        phone_number_id=company["wa_phone_number_id"],
                        to=from_phone,
                        text="📷 Vi tu imagen pero no encontré una lista de productos.\n\nMándame el pedido así:\n10 cemento, 5 varilla 3/8",
                    )
                    return "", msg_type, False, "HANDLED"
            except Exception as e:
                log.error("IMAGE PROCESSING ERROR:", repr(e))
                send_whatsapp_text(
                    wa_api_key=company["wa_api_key"],
                    phone_number_id=company["wa_phone_number_id"],
                    to=from_phone,
                    text="No pude leer la imagen 😔 Intenta enviarla más clara o escribe el pedido.",
                )
                return "", msg_type, False, "HANDLED"

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
        log.debug(f"INTERACTIVE MSG: type={itype} text={text!r} from={from_phone}")

    # Decide if this message should be batched:
    # - Interactive (button/list clicks) → NEVER batch, process immediately
    # - Images → don't batch (already handled above or extracted text is complete)
    # - Text messages → batch if they look like product lines or short fragments
    should_batch = (msg_type == "text")

    return text, msg_type, should_batch, early_reply


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
    wa_msg_id = msg.get("id", "")
    if wa_msg_id:
        if wa_msg_id in _processed_msg_ids:
            log.debug(f"DEDUP: skipping already-processed message {wa_msg_id}")
            return {"ok": True}
        _processed_msg_ids[wa_msg_id] = _time_mod.time()
        if len(_processed_msg_ids) > 500:
            _dedup_cleanup()

    from_phone = msg.get("from")

    # ── Extract message content ─────────────────────────────────────────
    text, msg_type, should_batch, early_reply = _extract_msg_content(msg, company)
    if early_reply == "HANDLED":
        return {"ok": True}

    # ── Interactive messages (button clicks) bypass batching entirely ────
    if msg_type == "interactive" or not should_batch:
        # Acquire lock to avoid racing with a batch that's being processed
        lock = _get_user_lock(company["company_id"], from_phone)
        async with lock:
            if text:
                log_message(company["company_id"], from_phone, "user", text)
            log.info(f"WEBHOOK IMMEDIATE: msg_type={msg_type} text={text!r}")
            # Run blocking bot logic in a thread to not block the event loop
            reply = await asyncio.to_thread(
                build_reply_for_company,
                company["company_id"], text,
                wa_from=from_phone,
                is_interactive=(msg_type == "interactive"),
            )
            if reply:
                try:
                    _state_for_log = get_quote_state(company["company_id"], from_phone) or {}
                    _log_extra = {"cart": _state_for_log.get("cart") or [], "folio": _state_for_log.get("folio") or None}
                except Exception:
                    _log_extra = {}
                log_message(company["company_id"], from_phone, "bot", _reply_text_for_log(reply), _log_extra)
                _send_reply(company, from_phone, reply)
        return {"ok": True}

    # ── Text messages: accumulate and batch ──────────────────────────────
    key = (company["company_id"], from_phone)
    lock = _get_user_lock(company["company_id"], from_phone)

    # If someone else is already processing for this user, just queue and return fast
    if lock.locked():
        if text:
            _queue_message(company["company_id"], from_phone, text)
            log.info(f"BATCH QUEUE (lock held): queued '{text[:60]}' for {from_phone}")
        return {"ok": True}

    # We're the first — acquire lock and become the batch processor
    async with lock:
        if text:
            _queue_message(company["company_id"], from_phone, text)
            log.info(f"BATCH START: queued '{text[:60]}' for {from_phone}, waiting {_BATCH_WAIT_SECS}s...")

        # Wait for more messages to accumulate
        _batch_start = _time_mod.time()
        _last_count = 0
        while True:
            await asyncio.sleep(_BATCH_WAIT_SECS)
            queued = _user_msg_queues.get(key, [])
            _elapsed = _time_mod.time() - _batch_start

            # If no new messages arrived since last check, or we hit max wait → process
            if len(queued) == _last_count or _elapsed >= _BATCH_MAX_WAIT_SECS:
                break
            # New messages arrived — wait one more cycle
            _last_count = len(queued)
            log.info(f"BATCH EXTEND: {len(queued)} msgs queued for {from_phone}, waiting more... ({_elapsed:.1f}s)")

        # Drain all accumulated messages
        batch = _drain_queue(company["company_id"], from_phone)
        if not batch:
            return {"ok": True}

        # Combine all texts: join with newline (like a product list)
        all_texts = [m["text"] for m in batch if m.get("text")]
        combined_text = "\n".join(all_texts)
        log.info(f"BATCH PROCESS: {len(batch)} msgs for {from_phone} → combined {len(combined_text)} chars: {combined_text[:120]!r}")

        # Log each individual message as "user" for conversation history
        for m_text in all_texts:
            log_message(company["company_id"], from_phone, "user", m_text)

        # Process the combined batch as one call (in thread to not block event loop)
        reply = await asyncio.to_thread(
            build_reply_for_company,
            company["company_id"], combined_text,
            wa_from=from_phone,
            is_interactive=False,
        )
        log.info(f"BATCH REPLY: type={type(reply).__name__} empty={not reply} preview={str(reply)[:120] if reply else 'NONE'}")

        if reply:
            try:
                _state_for_log = get_quote_state(company["company_id"], from_phone) or {}
                _log_extra = {"cart": _state_for_log.get("cart") or [], "folio": _state_for_log.get("folio") or None}
            except Exception:
                _log_extra = {}
            log_message(company["company_id"], from_phone, "bot", _reply_text_for_log(reply), _log_extra)
            _send_reply(company, from_phone, reply)

    return {"ok": True}

# ── Conversation logging ──────────────────────────────────────────────────────

# ── Conversation logging ──────────────────────────────────────────────────────

# log_message imported from queries.py


# ============================================================
# Parser shadow mode: corre LLM en paralelo y loguea para comparar
# ============================================================
_PARSER_SHADOW_ENABLED = os.environ.get("PARSER_SHADOW", "0") in ("1", "true", "True")
_PARSER_LLM_FIRST = os.environ.get("PARSER_LLM_FIRST", "1") in ("1", "true", "True")

def _load_catalog_for_shadow(company_id: str) -> list[dict]:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT sku, name, unit, price, vat_rate, is_default FROM pricebook_items "
            "WHERE company_id = %s ORDER BY name",
            (company_id,),
        )
        rows = cur.fetchall() or []
        cur.close()
        conn.close()
        return [
            {"sku": r[0], "name": r[1], "unit": r[2],
             "price": float(r[3]) if r[3] is not None else None,
             "vat_rate": float(r[4]) if r[4] is not None else None,
             "is_default": bool(r[5]) if r[5] is not None else False}
            for r in rows
        ]
    except Exception as e:
        log.error("SHADOW: catalog load error:", repr(e))
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
        result = llm_parse_order(user_text, catalog, company_id=company_id)
        ms = int((_t.time() - t0) * 1000)
        log.info(f"LLM PARSE: {ms}ms, items={len(result.get('items', []))}, "
              f"non_order={result.get('non_order')}, error={result.get('error')}")
        if result.get("error"):
            log.error(f"LLM PARSE FAILED: {result['error']} — falling back to regex")
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
        log.error(f"LLM PARSE EXCEPTION: {repr(e)} — falling back to regex")
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
            result = llm_parse_order(user_text, catalog, company_id=company_id)
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
            log.error("SHADOW: log error:", repr(e))
    try:
        import threading
        threading.Thread(target=_runner, daemon=True).start()
    except Exception as e:
        log.error("SHADOW: thread spawn error:", repr(e))


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

# send_whatsapp_list_sections imported from whatsapp_api.py

# ── Módulo Construcción Ligera (datos y cálculos en calculators.py) ───────────
import math
from calculators import (
    CONSTRUCCION_TIPOS, CONSTRUCCION_PRODUCTOS, CALC_FUNCTIONS,
    is_construccion_trigger as _is_construccion_trigger,
)

def _buscar_precio_exacto(conn, company_id: str, nombre: str):
    """Search pricebook by exact name_norm, then fall back to fuzzy LIKE search."""
    nombre_norm = norm_name(nombre)
    cur = conn.cursor()
    try:
        # 1) Exact match on name_norm
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

        # 2) Fuzzy fallback: search by significant tokens (≥3 chars) with LIKE
        tokens = [t for t in nombre_norm.split() if len(t) >= 3]
        if not tokens:
            return None
        where_parts = [f"name_norm LIKE %s" for _ in tokens]
        params = [company_id] + [f"%{t}%" for t in tokens]
        cur.execute(
            f"""
            SELECT sku, name, unit, price, vat_rate
            FROM pricebook_items
            WHERE company_id = %s AND {' AND '.join(where_parts)}
            ORDER BY length(name) ASC
            LIMIT 5
            """,
            tuple(params),
        )
        rows = cur.fetchall()
        if rows:
            # Pick best match using fuzzy scoring
            best = None
            best_score = 0
            for r in rows:
                score = fuzz.token_set_ratio(nombre_norm, norm_name(r[1] or ""))
                if score > best_score:
                    best_score = score
                    best = r
            if best and best_score >= 60:
                log.info(f"CALC PRICE FUZZY: '{nombre}' → '{best[1]}' (score={best_score})")
                return {"sku": best[0], "name": best[1], "unit": best[2],
                        "price": float(best[3]) if best[3] else None,
                        "vat_rate": float(best[4]) if best[4] else None}

        return None
    finally:
        cur.close()

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
        calc_fn = CALC_FUNCTIONS.get(tipo_key)
        if not calc_fn:
            raise ValueError(f"tipo desconocido: {tipo_key}")
        if tipo_key.startswith("muro"):
            materiales = calc_fn(datos["alto_muro"], datos["largo_muro"])
            m2 = datos["alto_muro"] * datos["largo_muro"]
            dim_txt = f"Alto: {datos['alto_muro']}m × Largo: {datos['largo_muro']}m = *{m2:.1f} m²*"
        else:
            materiales = calc_fn(datos["largo"], datos["ancho"])
            m2 = datos["largo"] * datos["ancho"]
            dim_txt = f"Largo: {datos['largo']}m × Ancho: {datos['ancho']}m = *{m2:.1f} m²*"
    except Exception as e:
        log.error("CALC ERROR:", repr(e))
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

    # ── Plan / trial check ─────────────────────────────────────────
    # get_company_plan_code() already handles trial expiry (downgrades
    # to "free" if trial_end < now).  If plan is "free", the company
    # hasn't paid yet — send a friendly message instead of processing.
    _current_plan = get_company_plan_code(company_id)
    if _current_plan == "free":
        # Check if they EVER had a trial (trial expired vs never subscribed)
        try:
            _pc = get_conn()
            _pcur = _pc.cursor()
            _pcur.execute("SELECT trial_end FROM companies WHERE id=%s LIMIT 1", (company_id,))
            _prow = _pcur.fetchone()
            _pcur.close()
            _pc.close()
            _had_trial = _prow and _prow[0] is not None
        except Exception:
            _had_trial = False

        if _had_trial:
            return (
                "¡Hola! Tu periodo de prueba ha terminado. 😊\n\n"
                "Para seguir recibiendo cotizaciones automáticas, "
                "activa tu plan en:\n"
                "👉 https://cotizaexpress.com/precios\n\n"
                "¡Gracias por usar CotizaBot!"
            )
        # If they never had a trial and plan is free, they might be in
        # initial setup — let them use the bot (onboarding flow gives
        # some free usage).  Or this is the shared Twilio number.
        # Don't block — fall through to normal processing.

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
        log.debug(f"BTN_DEBUG: raw_stripped={_raw_current_stripped!r} is_button_click={_is_button_click} in_triggers={_raw_current_stripped in _button_click_triggers}")

    # Clean up any legacy _msg_buffer from DB state (batching now handled at webhook level)
    if wa_from:
        _flush_state = get_quote_state(company_id, wa_from) or {}
        if _flush_state.get("_msg_buffer"):
            _flush_state.pop("_msg_buffer", None)
            upsert_quote_state(company_id, wa_from, _flush_state)

    if is_interactive:
        user_text = (user_text or "").strip()
    else:
        user_text = (user_text or "").strip().replace('\u201c', '').replace('\u201d', '').replace('"', '')
    wa_from = (wa_from or "").strip()

    try:
        usage_info = track_conversation_if_new(company_id, wa_from)
        if usage_info.get("limit", 0) > 0 and usage_info.get("usage", 0) > usage_info.get("limit", 0):
            log.info("WA LIMIT EXCEEDED:", usage_info)
    except Exception as e:
        log.error("WA TRACK ERROR:", repr(e))

    import string as _string
    from spec_definitions import get_spec_steps, already_has_specs, build_spec_query

    def _search_pricebook_candidates(conn, company_id: str, q: str, limit: int = 10):
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
        Clasifica si un mensaje es intención de cotizar productos, explorar catálogo,
        o conversación general.
        Retorna 'product', 'browse', o 'other'. Usa GPT-4o-mini para clasificación rápida.
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
                        "- PRODUCT: quiere cotizar, preguntar por un producto ESPECÍFICO, material, herramienta o precio, "
                        "O PREGUNTAR SI MANEJAN/TIENEN/VENDEN un producto (ej: 'manejan angulo?', 'tienen cemento?', "
                        "'venden tablaroca?', 'hay varilla?', 'cuando vale el canal?'). "
                        "Cualquier mención de un material o producto específico = PRODUCT.\n"
                        "- BROWSE: quiere explorar el catálogo, ver qué productos hay disponibles para una categoría "
                        "o uso específico, pide ver opciones o productos sin mencionar un producto exacto "
                        "(ej: 'que productos tienen para cortinas?', 'tienen catálogo?', 'que materiales manejan "
                        "para plafones?', 'productos disponibles en stock para instalar X').\n"
                        "- OTHER: conversación casual, preguntas personales, temas administrativos, saludos extendidos, "
                        "quejas, pagos, facturas, entregas, o cualquier cosa que NO sea pedir/explorar productos\n\n"
                        "Responde SOLO con: PRODUCT, BROWSE o OTHER"
                    )},
                    {"role": "user", "content": t},
                ],
                temperature=0.0,
                max_tokens=5,
            )
            result = (resp.choices[0].message.content or "").strip().upper()
            log.info(f"INTENT CLASSIFY: '{t[:50]}' → {result}")
            if "PRODUCT" in result:
                return "product"
            if "BROWSE" in result:
                return "browse"
            return "other"
        except Exception as e:
            log.error(f"INTENT CLASSIFY ERROR: {repr(e)}")
            return "product"  # fallback seguro: asumir producto

    def _escalate_non_quote(company_id_esc: str, wa_from_esc: str, text_esc: str) -> str:
        """Escala un mensaje no-cotización al dueño y responde al cliente."""
        company_name_esc = "la empresa"
        _esc_phone = ""
        try:
            conn_esc = get_conn()
            cur_esc = conn_esc.cursor()
            cur_esc.execute(
                "SELECT owner_phone, wa_api_key, wa_phone_number_id, name, telefono_atencion FROM companies WHERE id=%s",
                (company_id_esc,),
            )
            row_esc = cur_esc.fetchone()
            cur_esc.close()
            conn_esc.close()
            if row_esc:
                company_name_esc = row_esc[3] or "la empresa"
                _esc_phone = (row_esc[4] or row_esc[0] or "").strip()
                if row_esc[0]:
                    try:
                        state_esc = get_quote_state(company_id_esc, wa_from_esc) or {}
                        notify_owner_escalation(
                            wa_api_key=row_esc[1], phone_number_id=row_esc[2], owner_phone=row_esc[0],
                            client_phone=wa_from_esc,
                            reason=f"Mensaje no relacionado a cotización: \"{(text_esc or '')[:100]}\"",
                            state=state_esc,
                        )
                    except Exception as ne:
                        log.error(f"ESCALATE NOTIFY ERROR: {repr(ne)}")
        except Exception as e:
            log.error(f"ESCALATE NON-QUOTE ERROR: {repr(e)}")
        if _esc_phone:
            _esc_clean = _normalize_mx_phone(_esc_phone)
            return (
                f"Ese tema lo maneja directamente el equipo de *{company_name_esc}* 🙋\n\n"
                f"Contáctalos directamente:\n📞 {_esc_phone}\n👉 https://wa.me/{_esc_clean}\n\n"
                "Si quieres cotizar materiales, mándame tu lista con cantidades 📋"
            )
        return (
            f"Ese tema lo maneja directamente el equipo de *{company_name_esc}* 🙋\n\n"
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
                 "buenas tardes", "buenas noches",
                 "que onda", "qué onda", "que tal", "qué tal", "que paso", "qué paso",
                 "como estas", "cómo estás", "como andas", "cómo andas"}:
            return True
        # If the message contains product/order signals, it's NOT a pure greeting
        # e.g. "hola buenas tardes me podras cotizar 3 cajas de redimix"
        # e.g. "hola vendemos productos para techo de lamina?"
        _order_signals = re.search(
            r"\b(cotiz|precio|cuanto|cuánto|cuando\s+vale|mand|necesito|ocupo|quiero|dame|lista|material|"
            r"producto|productos|venden|vendemos|manejan|tienen para|catalogo|catálogo|"
            r"techo|lamina|lámina|varilla|cemento|block|poste|canal|tablaroca|"
            r"\d+\s+[a-záéíóúñ]{3,})\b",
            t, re.IGNORECASE
        )
        if _order_signals:
            return False
        if t.startswith("hola"):
            return True
        if t.startswith("buenos") or t.startswith("buenas") or t.startswith("buen"):
            return True
        if t.startswith("que onda") or t.startswith("qué onda"):
            return True
        if t.startswith("que tal") or t.startswith("qué tal"):
            return True
        if t.startswith("como estas") or t.startswith("cómo estás"):
            return True
        return False

    def _is_clearly_off_topic(tnorm: str) -> bool:
        """Only return True for messages that are CLEARLY unrelated to products/quoting.
        E.g. complaints, invoicing, payment issues, personal chat, job inquiries.
        This is intentionally narrow — when in doubt, we try to process as product
        rather than escalating and losing a potential customer."""
        t = (tnorm or "").strip()
        if not t:
            return False
        # Only escalate for clearly non-product topics
        return bool(re.search(
            r"\b(factura|facturar|facturaci[oó]n|rfc|raz[oó]n\s+social"  # invoicing
            r"|reclamaci[oó]n|queja|devoluci[oó]n|garant[ií]a|reembolso"  # complaints/returns
            r"|pago\s+(?:no\s+)?(?:lleg|pas|refle)|no\s+me\s+(?:lleg|cobr)"  # payment issues
            r"|trabajo|empleo|vacante|contrat|curr[ií]cul"               # job inquiries
            r"|qui[eé]n\s+(?:eres|es\s+el\s+due[nñ]o)|eres\s+(?:humano|robot|persona)"  # identity questions
            r"|proveedor|(?:quiero|puedo)\s+(?:ser|vender(?:les|te))"    # supplier inquiries
            r"|entrega|env[ií]o\s+(?:no\s+)?lleg|rastreo|gu[ií]a|paqueter[ií]a"  # shipping issues
            r"|cancelar\s+(?:mi\s+)?(?:pedido|orden|suscripci)"          # cancellations
            r")\b",
            t, re.IGNORECASE
        ))

    def _has_specific_product(tnorm: str) -> bool:
        """Detect if the message mentions a specific product (not just generic words like 'material').
        Used to decide: fall through to regex (has product) vs ask for list (generic intent)."""
        t = (tnorm or "").strip()
        if not t:
            return False
        # Remove generic/filler words to see if anything specific remains
        _generic = re.sub(
            r"\b(hola|buenas?|tardes?|dias?|noches?|me|pueden?|puedes?|podr[aá]s?|"
            r"cotiz\w*|material(?:es)?|producto(?:s)?|un|una|unos|unas|el|la|los|las|"
            r"de|del|para|por|con|que|qué|este|esta|estos|estas|ese|eso|"
            r"necesito|quiero|ocupo|ando|buscando|busco|buscar|"
            r"cuanto|cu[aá]nto|cuesta|vale|sale|precio|precios?|"
            r"dame|deme|favor|manden?|pedir|comprar|conseguir|tienen|manejan|venden|hay)\b",
            " ", t, flags=re.IGNORECASE
        ).strip()
        _generic = re.sub(r"\s+", " ", _generic).strip().rstrip("?.,!¿¡")
        # If after removing generic words there's a meaningful word left (3+ chars), it's specific
        _remaining_words = [w for w in _generic.split() if len(w) >= 3]
        if _remaining_words:
            log.debug(f"HAS_SPECIFIC_PRODUCT: remaining words after generic removal: {_remaining_words}")
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

                # Sort by price (cheapest first) as tiebreaker, then limit to 9
                # (WhatsApp allows max 10 rows per list; 9 candidates + 1 "Ninguno" = 10)
                # IMPORTANT: save sorted+truncated list back to state so pick handler
                # uses the same order as what was displayed to the user
                cands.sort(key=lambda x: float(x.get("price") or 999999))
                cands = cands[:9]
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
        log.debug(f"EARLY REMOVE TRIGGER: raw={user_text!r} tnorm={tnorm!r} stripped={_early_stripped!r}")
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
            "type": "list_sections",
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
            # No plan — redirect to human agent
            return "Escribe *asesor* para que te atiendan con los datos de pago 🙏"
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
            return "Escribe *asesor* para que te atiendan con los datos de pago 🙏"

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
                        log.error("NON-QUOTE NOTIFY ERROR:", repr(ne))
                company_name_esc = (row_esc[3] if row_esc else None) or "la empresa"
            except Exception as e:
                log.error("NON-QUOTE ESCALATION ERROR:", repr(e))
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
                "Si quieres cotizar materiales, mándame tu lista con cantidades 📋"
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
            log.info(f"FRUSTRATION DETECTED: escalating '{_t_lower}' (pending state exists)")
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
                    log.error("ESCALATION NOTIFY ERROR:", repr(ne))
        except Exception as e:
            log.error("ESCALATION ERROR:", repr(e))
        # Build response with wa.me link if phone is available
        if _atencion_phone:
            _phone_clean = _normalize_mx_phone(_atencion_phone)
            return (
                f"Te atiende un asesor de *{_company_name_esc}* directo 🙏\n\n"
                f"👉 https://wa.me/{_phone_clean}"
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

    # ── Catalog browsing: "que tienen para X", "productos para X", "catalogo" ──
    _catalog_match = re.match(
        r"^(?:(?:que|qué)\s+(?:productos?|materiales?|artículos?|articulos?)\s+(?:tienen|manejan|venden|hay)\s+(?:para|de|en)\s+(.+)|"
        r"(?:tienen|manejan|venden)\s+(?:algo|productos?|materiales?)\s+(?:para|de)\s+(.+)|"
        r"(?:productos?|materiales?)\s+(?:disponibles?|en\s+stock)\s+(?:para|de)\s+(.+)|"
        r"(?:tienes?|tienen)\s+(?:catalogo|catálogo)(?:\s+(?:de|para)\s+(.+))?|"
        r"(?:catalogo|catálogo)(?:\s+(?:de|para)\s+(.+))?|"
        r"(?:me\s+)?(?:puedes?|pueden)\s+(?:mencionar(?:me)?|mostrar(?:me)?|decir(?:me)?|listar(?:me)?|dar(?:me)?)\s+(?:los\s+)?(?:productos?|materiales?)\s+(?:disponibles?\s+)?(?:(?:en\s+stock\s+)?(?:para|de|en|que\s+tienen))\s*(.+)?|"
        r"(?:que|qué)\s+(?:tienen|manejan|venden)\s+(?:en\s+)?(?:stock|existencia|inventario)(?:\s+(?:para|de)\s+(.+))?)$",
        tnorm, re.IGNORECASE
    )
    if _catalog_match:
        # Extract the topic/category from whichever group matched
        _cat_topic = next((g.strip().rstrip("?.,!¿") for g in _catalog_match.groups() if g), None)

        if _cat_topic and len(_cat_topic) >= 2:
            # Search pricebook for the topic
            try:
                conn_cat = get_conn()
                _cat_results = _search_pricebook_candidates(conn_cat, company_id, _cat_topic, limit=15)
                conn_cat.close()
            except Exception:
                _cat_results = []

            if _cat_results:
                _cat_lines = []
                for i, it in enumerate(_cat_results[:10], 1):
                    _p = float(it.get("price") or 0)
                    _u = it.get("unit") or "pza"
                    if _p > 0:
                        _cat_lines.append(f"{i}. {it['name']} — ${_p:,.2f}/{_u}")
                    else:
                        _cat_lines.append(f"{i}. {it['name']}")
                return (
                    f"Estos son los productos que tenemos relacionados con *{_cat_topic}*:\n\n"
                    + "\n".join(_cat_lines)
                    + "\n\nPara cotizar, mándame la cantidad y el producto.\nEj: 10 " + (_cat_results[0].get("name") or "producto")
                )
            else:
                return (
                    f"No encontré productos relacionados con *{_cat_topic}* en nuestro catálogo.\n\n"
                    "¿Podrías ser más específico? O mándame el nombre del producto directamente."
                )
        else:
            # Generic "catálogo" without topic — show categories/guidance
            try:
                conn_cnt = get_conn()
                cur_cnt = conn_cnt.cursor()
                cur_cnt.execute("SELECT COUNT(*) FROM pricebook_items WHERE company_id=%s", (company_id,))
                _total_prods = cur_cnt.fetchone()[0] or 0
                cur_cnt.close()
                conn_cnt.close()
            except Exception:
                _total_prods = 0

            return (
                f"Tenemos *{_total_prods}* productos en catálogo.\n\n"
                "Dime qué buscas y te muestro lo que tenemos. Por ejemplo:\n"
                "• \"productos para cortinas\"\n"
                "• \"cemento\"\n"
                "• \"varilla\"\n\n"
                "O mándame tu lista con cantidades para cotizar al instante."
            )

    # ── "What can you do?" / service inquiry → respond as helpful agent ──
    _capabilities_patterns = re.search(
        r"\b(?:que\s+(?:puedes|pueden|sabes|haces)|qué\s+(?:puedes|pueden|sabes|haces)|"
        r"cuales\s+son\s+(?:tus|sus)\s+servicios|cuáles\s+son\s+(?:tus|sus)\s+servicios|"
        r"que\s+servicios|qué\s+servicios|"
        r"como\s+funciona|cómo\s+funciona|"
        r"para\s+que\s+sirve|para\s+qué\s+sirve|"
        r"que\s+ofreces|qué\s+ofreces|que\s+ofrecen|qué\s+ofrecen|"
        r"en\s+que\s+(?:me\s+)?(?:puedes|pueden)\s+ayudar|"
        r"en\s+qué\s+(?:me\s+)?(?:puedes|pueden)\s+ayudar|"
        r"a\s+que\s+se\s+dedican|a\s+qué\s+se\s+dedican|"
        r"info(?:rmacion)?(?:\s+de\s+(?:la\s+)?empresa)?|"
        r"que\s+es\s+esto|qué\s+es\s+esto|"
        r"como\s+te\s+uso|cómo\s+te\s+uso)\b",
        tnorm, re.IGNORECASE
    )
    if _capabilities_patterns:
        # Respond like the greeting handler — show the menu with services
        try:
            conn_cap = get_conn()
            cur_cap = conn_cap.cursor()
            cur_cap.execute("SELECT name, giro, descripcion, construccion_ligera_enabled, rejacero_enabled, pintura_enabled, impermeabilizante_enabled FROM companies WHERE id=%s LIMIT 1", (company_id,))
            row_cap = cur_cap.fetchone()
            cur_cap.close()
            conn_cap.close()
            _cap_name = row_cap[0] if row_cap else "tu ferretería"
            _cap_giro = (row_cap[1] or "").strip() if row_cap else ""
            _cap_desc = (row_cap[2] or "").strip() if row_cap else ""
            _cap_cl = bool(row_cap[3]) if row_cap else False
            _cap_rj = bool(row_cap[4]) if row_cap else False
            _cap_pt = bool(row_cap[5]) if row_cap else False
            _cap_im = bool(row_cap[6]) if row_cap else False
        except Exception:
            _cap_name = "tu ferretería"
            _cap_giro = ""
            _cap_desc = ""
            _cap_cl = _cap_rj = _cap_pt = _cap_im = False

        _cap_intro = f"*{_cap_name}*"
        if _cap_giro:
            _cap_intro += f" — {_cap_giro}"
        if _cap_desc:
            _cap_intro += f"\n{_cap_desc}"

        _cap_services = (
            f"Soy el asistente virtual de {_cap_intro}.\n\n"
            "Te puedo ayudar con:\n"
            "• *Cotizar materiales* — mándame tu lista y te doy precios al instante\n"
            "• *Consultar disponibilidad* — pregúntame si manejamos algún producto\n"
        )
        _any_cap_calc = _cap_cl or _cap_rj or _cap_pt or _cap_im
        if _any_cap_calc:
            _cap_services += "• *Calcular cantidades* — muros, plafones, pintura, impermeabilizante\n"
        _cap_services += "• *Horarios y ubicación*\n\n¿En qué te puedo ayudar?"

        _cap_opts = ["🔨 Cotizar materiales"]
        if _any_cap_calc:
            _cap_opts.append("📐 Cotizar cálculo")
        _cap_opts.extend(["🕐 Horarios y ubicación", "👤 Hablar con alguien"])
        return {
            "type": "list",
            "body": _cap_services,
            "options": _cap_opts,
            "button_label": "Ver opciones",
        }

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
        # Normalize common shorthand: "410" → "4.10", "635" → "6.35", "305" → "3.05"
        _t_norm = t_low
        if re.match(r"^\d{3}$", _t_norm):
            _t_norm = _t_norm[0] + "." + _t_norm[1:]  # "410" → "4.10"
        chosen = next(
            (opt for opt in current_step["options"]
             if t_low == opt.lower() or opt.lower() in t_low
             or _t_norm == opt.lower() or opt.lower() in _t_norm),
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
                                else:
                                    # Calculator items: add to cart with price=0 instead
                                    # of showing ambiguous candidates one-by-one
                                    state = cart_add_item(state, {
                                        "sku": None, "name": _lname,
                                        "unit": "pza", "price": 0,
                                        "vat_rate": None, "qty": _lqty,
                                    })
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
                            else:
                                # Calculator items: add with price=0 instead of ambiguous candidates
                                state = cart_add_item(state, {
                                    "sku": None, "name": _fallback_name,
                                    "unit": "pza", "price": 0,
                                    "vat_rate": None, "qty": _lqty,
                                })
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
                                else:
                                    # Calculator items: add with price=0 instead of ambiguous candidates
                                    state = cart_add_item(state, {
                                        "sku": None, "name": _fallback_name,
                                        "unit": "pza", "price": 0,
                                        "vat_rate": None, "qty": _lqty,
                                    })
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

    # NOTE: Message batching/accumulation is now handled at the webhook level
    # (per-user async lock + in-memory queue with 3s debounce window).
    # The old _msg_buffer DB-based approach has been removed.

    # ── Handle hours/location question BEFORE LLM parser ──────────────
    # Otherwise LLM parses "Horarios y ubicación" as product names
    if looks_like_hours_question(user_text):
        try:
            conn_hrs = get_conn()
            cur_hrs = conn_hrs.cursor()
            cur_hrs.execute("SELECT hours_text, address_text, google_maps_url FROM companies WHERE id=%s", (company_id,))
            row_hrs = cur_hrs.fetchone()
            cur_hrs.close()
            conn_hrs.close()
        except Exception:
            row_hrs = None
        if row_hrs:
            hours = (row_hrs[0] or "").strip()
            address = (row_hrs[1] or "").strip()
            maps_url = (row_hrs[2] or "").strip()
            parts = []
            if hours: parts.append(f"🕐 *Horarios:* {hours}")
            if address: parts.append(f"📍 *Dirección:* {address}")
            if maps_url: parts.append(f"🗺️ *Google Maps:* {maps_url}")
            if parts:
                log.info(f"HOURS HANDLER (early): text='{user_text[:60]}'")
                return "\n".join(parts) + "\n\n¿Cotizamos algo? Mándame ej: 10 cemento, 5 varilla 3/8"
        log.info(f"HOURS HANDLER (early, no data): text='{user_text[:60]}'")
        return (
            "📍 Escríbenos directamente para darte la ubicación y horarios.\n\n"
            "Si quieres cotizar: mándame ej: 10 cemento, 5 varilla 3/8"
        )

    # ── LLM-first parser: intenta LLM primero, regex como fallback ──
    # SKIP LLM for known button clicks — they have their own handlers downstream
    _llm_result = None
    if _PARSER_LLM_FIRST and not _is_button_click:
        _llm_result = _try_llm_parse(company_id, user_text)

    # ── LLM detectó que NO es una orden → solo escalar si es CLARAMENTE off-topic ──
    # Default: try to process as product. Only escalate for invoicing, complaints, etc.
    if _llm_result and _llm_result.get("non_order"):
        if looks_like_hours_question(user_text):
            log.warning(f"LLM NON_ORDER but is hours question — skipping escalation. text='{user_text[:60]}'")
        elif _is_greeting_like(tnorm):
            log.warning(f"LLM NON_ORDER but is greeting — skipping escalation. text='{user_text[:60]}'")
        elif _is_clearly_off_topic(tnorm):
            log.info(f"LLM NON_ORDER + clearly off-topic: escalating. text='{user_text[:60]}'")
            return _escalate_non_quote(company_id, wa_from, user_text)
        elif _has_specific_product(tnorm):
            # Has a specific product mention (e.g. "que precio tiene el block") → let regex try
            log.info(f"LLM NON_ORDER but has specific product — falling through to regex. text='{user_text[:60]}'")
            _llm_result = None
        else:
            # Generic intent without specific product (e.g. "me pueden cotizar un material")
            log.info(f"LLM NON_ORDER, generic intent — asking for product list. text='{user_text[:60]}'")
            return (
                "¡Claro! Con gusto te cotizo 😊\n\n"
                "Mándame los productos con cantidades, por ejemplo:\n"
                "• 10 bultos de cemento\n"
                "• 5 varillas 3/8\n"
                "• 20 blocks\n\n"
                "¿Qué necesitas?"
            )

    if _llm_result and _llm_result.get("items") and not _llm_result.get("non_order"):
        # ── LLM PATH: procesa items directamente sin regex ni smart_search ──
        from llm_parser import norm_key as _llm_norm_key
        _cat_by_key = _llm_result.get("_cat_by_key") or {}
        _parser_used = "llm"
        log.info(f"LLM ITEMS: {[(it.get('qty'), it.get('key') or it.get('name')) for it in _llm_result['items']]}")
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
                                log.info(f"LLM FUZZY MATCH: {_key!r} → {_cv.get('name')!r}")
                                break
                    if cat_item:
                        # Check if user specified a size — if NOT, prefer is_default product
                        _user_raw = _matched or _name or ""
                        log.debug(f"IS_DEFAULT CHECK: cat='{cat_item.get('name')}' user_raw='{_user_raw}' matched='{_matched}' name='{_name}'")
                        _has_user_size = bool(re.search(r"\b\d+\.\d+\b", _user_raw))  # e.g. "4.10", "6.35"
                        if not _has_user_size and not cat_item.get("is_default"):
                            # User didn't specify size → look for is_default product with same base type
                            # Extract first word as product type (Poste, Canal, Tablaroca, etc.)
                            _cat_name = (cat_item.get("name") or "").strip()
                            _first_word = _cat_name.split()[0] if _cat_name else ""
                            # Also try user's original first word (handles "canal de amarre" → "canal")
                            _user_first_word = (_user_raw.split()[0] if _user_raw else "").strip()
                            _search_words = []
                            if _first_word and len(_first_word) >= 3:
                                _search_words.append(_first_word)
                            if _user_first_word and len(_user_first_word) >= 3 and _user_first_word.lower() != _first_word.lower():
                                _search_words.append(_user_first_word)
                            for _sw in _search_words:
                                try:
                                    _cur_def = conn.cursor()
                                    _cur_def.execute(
                                        "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                                        "WHERE company_id=%s AND is_default=true "
                                        "AND lower(name) LIKE lower(%s) || ' %%' LIMIT 1",
                                        (company_id, _sw),
                                    )
                                    _def_row = _cur_def.fetchone()
                                    _cur_def.close()
                                    if _def_row:
                                        log.debug(f"IS_DEFAULT OVERRIDE: '{cat_item.get('name')}' → '{_def_row[1]}' (user had no size, matched on '{_sw}')")
                                        cat_item = {
                                            "sku": _def_row[0], "name": _def_row[1],
                                            "unit": _def_row[2], "price": _def_row[3],
                                            "vat_rate": _def_row[4],
                                        }
                                        break
                                except Exception as _def_e:
                                    log.error(f"IS_DEFAULT CHECK ERROR: {repr(_def_e)}")

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
                        log.info(f"LLM KEY NOT IN CATALOG: key={_key!r}, trying smart_search")
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
                                log.info(f"LLM KEY MISS → SMART_SEARCH: {_search_name2!r} → {_fb2['item'].get('name')!r}")
                            elif _fb2 and _fb2.get("candidates"):
                                missing.append({"qty": _qty, "raw": _search_name2,
                                                "candidates": _fb2["candidates"]})
                            else:
                                missing.append({"qty": _qty, "raw": _matched or _name, "candidates": []})
                        except Exception as _fb2e:
                            log.error(f"LLM KEY MISS FALLBACK ERROR: {repr(_fb2e)}")
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
                                log.info(f"LLM FALLBACK SMART_SEARCH: {_search_name!r} → {_fb_result['item'].get('name')!r}")
                            elif _fb_result and _fb_result.get("candidates"):
                                # Ambiguo: pasar candidatos para que el bot pregunte al cliente
                                missing.append({"qty": _qty, "raw": _search_name,
                                                "candidates": _fb_result["candidates"]})
                                _fallback_found = True
                                log.info(f"LLM FALLBACK AMBIGUOUS: {_search_name!r} → {len(_fb_result['candidates'])} candidates")
                        except Exception as _fbe:
                            log.error(f"LLM FALLBACK ERROR: {repr(_fbe)}")
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
                                log.error("LLM ESCALATION NOTIFY ERROR:", repr(_ne))
                        state["escalated_proactive"] = True
                        if wa_from:
                            upsert_quote_state(company_id, wa_from, state)
                        log.info(f"LLM ESCALATION: low={_should_escalate_low} retry={_should_escalate_retry} miss={_miss_count}/{_total_items}")
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
                        log.error("LLM ESCALATION ERROR:", repr(_e))
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
        log.warning("SHADOW: skipped:", repr(_e))
    if multi:
        log.info(f"MULTI ITEMS ({_parser_used}): {[(q, p) for q, p, *_ in multi]}")
        conn = get_conn()
        try:
            state = get_quote_state(company_id, wa_from) if wa_from else None
            if not state:
                state = {}
            state.pop("pending_specs", None)
            state.pop("pending", None)  # Clear stale pending — new product list takes priority
            missing = []
            _pedido_raw = ", ".join(p for _, p, *_ in multi if p.strip() != "???")
            # Detect if the list is rejacero-context (rejas, abrazaderas, bases para poste, etc.)
            _pedido_lower = _pedido_raw.lower()
            _is_rejacero_list = any(w in _pedido_lower for w in (
                "reja", "rejacero", "malla", "abrazadera", "base para poste",
                "bases para poste", "deacero", "ciclonica", "ciclónica",
                "poste-malla", "sujecion poste", "sujeción poste",
            ))
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
                # If list is rejacero-context and product is "poste" without rejacero qualifier,
                # add "rejacero" hint so smart_search finds the right type of poste
                _search_query = prod_raw
                if _is_rejacero_list:
                    _pr_low = prod_raw.lower()
                    if ("poste" in _pr_low or "postes" in _pr_low) and "rejacero" not in _pr_low:
                        _search_query = prod_raw + " rejacero"
                        log.info(f"REJACERO CONTEXT: '{prod_raw}' → '{_search_query}'")
                    elif ("tornillo" in _pr_low or "pija" in _pr_low) and "rejacero" not in _pr_low and "base" not in _pr_low:
                        _search_query = prod_raw + " rejacero"
                        log.info(f"REJACERO CONTEXT: '{prod_raw}' → '{_search_query}'")
                try:
                    result = smart_search(conn, company_id, _search_query, qty,
                                          cart_context=_pedido_raw)
                except Exception as e:
                    log.error("SMART SEARCH ERROR:", repr(e))
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
                                log.error("PROACTIVE ESCALATION NOTIFY ERROR:", repr(_ne))
                        # Mark escalated so we don't spam
                        state["escalated_proactive"] = True
                        if wa_from:
                            upsert_quote_state(company_id, wa_from, state)
                        log.info(f"PROACTIVE ESCALATION: low={_should_escalate_low} retry={_should_escalate_retry} miss={_miss_count}/{_total_items}")
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
                        log.error("PROACTIVE ESCALATION ERROR:", repr(_e))
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
                log.error("SMART SEARCH ERROR:", repr(e))
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
                        log.error("SMART SEARCH ERROR:", repr(e))
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
                        log.error("SMART SEARCH ERROR:", repr(e))
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

        # ── Follow-up question: "y el calibre 20?", "y en 3/8?", "pero en cal 22?" ──
        # If user has a cart and asks about a variant of the last product
        _followup_match = re.match(
            r"^(?:y\s+|pero\s+)?(?:el|la|los|las|en|de|del)?\s*(?:de\s+|el\s+|la\s+)?\s*"
            r"(cal(?:ibre)?\s*\d+|\d+\s*/\s*\d+(?:\s*\")?|"
            r"\d+(?:\.\d+)(?:\s*x\s*\d+(?:\.\d+))?|"
            r"\d+\s*(?:m(?:ts?|etros?)?|pulgadas?|\")?)\s*\??$",
            tnorm, re.IGNORECASE
        )
        if not _followup_match:
            # Also catch "y el calibre 20?" style
            _followup_match = re.match(
                r"^(?:y\s+|pero\s+)?(?:el\s+|la\s+|en\s+|de\s+)?"
                r"(?:calibre|cal|diametro|diámetro|medida|largo|tamaño)\s+"
                r"(\S+(?:\s+\S+)?)\s*\??$",
                tnorm, re.IGNORECASE
            )
        if _followup_match and wa_from:
            _fu_spec = _followup_match.group(1).strip().rstrip("?")
            _fu_state = get_quote_state(company_id, wa_from) or {}
            _fu_cart = _fu_state.get("cart") or []
            if _fu_cart:
                # Get last product added to cart as context
                _fu_last = _fu_cart[-1]
                _fu_last_name = (_fu_last.get("name") or "").strip()
                if _fu_last_name:
                    # Extract base product name (without specs like cal 26, 3.05, etc.)
                    _fu_base = re.sub(
                        r"\s+(?:cal(?:ibre)?\s*\d+|\d+\.\d+(?:\s*x\s*\d+\.\d+)?)\s*",
                        " ", _fu_last_name, flags=re.IGNORECASE
                    ).strip()
                    # Build new query: base product + new spec
                    _fu_query = f"{_fu_base} {_fu_spec}"
                    log.info(f"FOLLOWUP: last='{_fu_last_name}' base='{_fu_base}' new_spec='{_fu_spec}' query='{_fu_query}'")
                    try:
                        conn_fu = get_conn()
                        _fu_result = smart_search(conn_fu, company_id, _fu_query, 1, cart_context=_fu_last_name)
                        conn_fu.close()
                    except Exception as e:
                        log.error(f"FOLLOWUP SEARCH ERROR: {repr(e)}")
                        _fu_result = {"status": "not_found", "item": None, "candidates": []}
                    if _fu_result["status"] == "found":
                        _fu_state = cart_add_item(_fu_state, {
                            "sku": _fu_result["item"].get("sku"),
                            "name": _fu_result["item"].get("name"),
                            "unit": _fu_result["item"].get("unit") or "unidad",
                            "price": float(_fu_result["item"].get("price") or 0.0),
                            "vat_rate": _fu_result["item"].get("vat_rate"),
                            "qty": 1,
                        })
                        _fu_state.pop("pending", None)
                        upsert_quote_state(company_id, wa_from, _fu_state)
                        return _build_reply_with_pending(_fu_state, company_id=company_id, wa_from=wa_from)
                    elif _fu_result.get("candidates"):
                        _fu_state["pending"] = [{"qty": 1, "raw": _fu_query, "candidates": _fu_result["candidates"]}]
                        upsert_quote_state(company_id, wa_from, _fu_state)
                        return _build_reply_with_pending(_fu_state, company_id=company_id, wa_from=wa_from)

        # ── Detect casual acknowledgment / thanks before escalating ──
        _ack_phrases = {
            "ok", "okey", "okay", "va", "vale", "sale", "listo", "entendido",
            "ah ok", "ah okey", "ah ya", "ya", "ya vi", "ah bueno",
            "gracias", "muchas gracias", "mil gracias", "grax", "thanks",
            "perfecto", "perfecto gracias", "excelente", "de acuerdo",
            "está bien", "esta bien", "muy bien",
        }
        _t_ack = re.sub(r"[¡!¿?,.\s]+", " ", tnorm).strip()
        if _t_ack in _ack_phrases or (len(_t_ack) < 25 and any(_t_ack.startswith(p) for p in ("gracias", "muchas gracias", "ok gracias", "perfecto", "ah ok"))):
            return "¡Con gusto! 😊 Si necesitas cotizar algo más, mándame tu lista con cantidades."

        # ── Pre-check: "manejan X?", "tienen X?", "venden X?" → treat as product ──
        _avail_match = re.match(
            r"^(?:disculpe?\s*,?\s*)?(?:ustedes\s+)?(?:manejan|tienen|venden|hay|cuentan\s+con|trabajan)\s+(.+)",
            tnorm, re.IGNORECASE
        )
        # ── Price inquiry: "cuanto vale X", "cuando vale X" (typo), "a como X" ──
        _price_match = re.match(
            r"^(?:cuando|cuanto|cuánto|quanto)\s+(?:vale|cuesta|sale|esta|está|es)\s+(?:el|la|los|las|un|una|unos|unas)?\s*(.+)",
            tnorm, re.IGNORECASE
        ) or re.match(
            r"^(?:a\s+como|a\s+cómo|a\s+cuanto|a\s+cuánto)\s+(?:esta|está|sale|dan|tienen|manejan)?\s*(?:el|la|los|las|un|una)?\s*(.+)",
            tnorm, re.IGNORECASE
        ) or re.match(
            r"^(?:que|qué)\s+precio\s+(?:tiene|tienen|maneja|manejan)?\s*(?:el|la|los|las|un|una)?\s*(.+)",
            tnorm, re.IGNORECASE
        ) or re.match(
            r"^(?:precio|costo)\s+(?:de(?:l)?|de\s+la|de\s+los|de\s+las)?\s*(.+)",
            tnorm, re.IGNORECASE
        )
        if _avail_match or _price_match:
            intent = "product"
            # Extract the product name from the match for better search
            _prod_from_question = (_avail_match or _price_match).group(1).strip().rstrip("?.,!¿")
            if _prod_from_question and len(_prod_from_question) >= 2:
                user_text = _prod_from_question  # Use clean product name for search
        else:
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
                    log.error("SMART SEARCH ERROR:", repr(e))
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
        elif intent == "browse":
            # Catalog browsing — search for related products
            _browse_query = user_text.strip()
            # Try to extract a useful search term from the message
            _browse_extract = re.sub(
                r"(?:tienes?|tienen|manejan|venden|hay|puedes?|pueden|mencion\w*|mostrar\w*|decir\w*|"
                r"catalogo|catálogo|productos?|materiales?|disponibles?|en\s+stock|para|de|del|la|el|"
                r"los|las|un|una|que|qué|me|nos|algo)\s*",
                " ", _browse_query, flags=re.IGNORECASE
            ).strip().rstrip("?.,!¿")
            _browse_term = _browse_extract if len(_browse_extract) >= 2 else _browse_query
            try:
                conn_br = get_conn()
                _br_results = _search_pricebook_candidates(conn_br, company_id, _browse_term, limit=15)
                conn_br.close()
            except Exception:
                _br_results = []
            if _br_results:
                _br_lines = []
                for i, it in enumerate(_br_results[:10], 1):
                    _p = float(it.get("price") or 0)
                    _u = it.get("unit") or "pza"
                    if _p > 0:
                        _br_lines.append(f"{i}. {it['name']} — ${_p:,.2f}/{_u}")
                    else:
                        _br_lines.append(f"{i}. {it['name']}")
                return (
                    f"Estos son los productos que encontré relacionados con tu búsqueda:\n\n"
                    + "\n".join(_br_lines)
                    + "\n\nPara cotizar, mándame la cantidad y el producto.\nEj: 10 " + (_br_results[0].get("name") or "producto")
                )
            else:
                return (
                    "No encontré productos relacionados en nuestro catálogo.\n\n"
                    "Dime más específicamente qué buscas, o mándame tu lista de materiales con cantidades."
                )
        else:
            # No es producto ni browse → escalar a humano
            return _escalate_non_quote(company_id, wa_from, user_text)

    # Último fallback: clasificar con GPT
    intent = _classify_intent(user_text)
    if intent == "product":
        return (
            "¡Claro! Con gusto te cotizo 😊\n\n"
            "Mándame los productos con cantidades, por ejemplo:\n"
            "• 10 bultos de cemento\n"
            "• 5 varillas 3/8\n"
            "• 20 blocks\n\n"
            "¿Qué necesitas?"
        )
    elif intent == "browse":
        # Same browse logic as above
        _browse_query = user_text.strip()
        _browse_extract = re.sub(
            r"(?:tienes?|tienen|manejan|venden|hay|puedes?|pueden|mencion\w*|mostrar\w*|decir\w*|"
            r"catalogo|catálogo|productos?|materiales?|disponibles?|en\s+stock|para|de|del|la|el|"
            r"los|las|un|una|que|qué|me|nos|algo)\s*",
            " ", _browse_query, flags=re.IGNORECASE
        ).strip().rstrip("?.,!¿")
        _browse_term = _browse_extract if len(_browse_extract) >= 2 else _browse_query
        try:
            conn_br2 = get_conn()
            _br_results2 = _search_pricebook_candidates(conn_br2, company_id, _browse_term, limit=15)
            conn_br2.close()
        except Exception:
            _br_results2 = []
        if _br_results2:
            _br_lines2 = []
            for i, it in enumerate(_br_results2[:10], 1):
                _p = float(it.get("price") or 0)
                _u = it.get("unit") or "pza"
                if _p > 0:
                    _br_lines2.append(f"{i}. {it['name']} — ${_p:,.2f}/{_u}")
                else:
                    _br_lines2.append(f"{i}. {it['name']}")
            return (
                f"Estos son los productos que encontré relacionados con tu búsqueda:\n\n"
                + "\n".join(_br_lines2)
                + "\n\nPara cotizar, mándame la cantidad y el producto.\nEj: 10 " + (_br_results2[0].get("name") or "producto")
            )
        return (
            "No encontré productos relacionados en nuestro catálogo.\n\n"
            "Dime más específicamente qué buscas, o mándame tu lista de materiales con cantidades."
        )
    else:
        return _escalate_non_quote(company_id, wa_from, user_text)



# ── Admin routes → routes/admin.py ──────────────────────────────────────────



# ── Company settings → routes/company.py ────────────────────────────────────
# ── Pagos routes → routes/pagos.py ──────────────────────────────────────────


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


# ── Pricebook routes → routes/pricebook.py ──────────────────────────────────


# ── Health / utility endpoints ──────────────────────────────────────────────

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
    log.debug("WA VERIFY: mode=%s match=%s has_challenge=%s",
              hub_mode, (hub_verify_token or "") == expected, bool(hub_challenge))
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


# ── Admin endpoints ─────────────────────────────────────────────────────────

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

        # Whitelisted tables for safe dynamic SQL
        _SAFE_TABLES = {
            "sessions", "item_embeddings", "pricebook_items", "wa_quote_state",
            "wa_conversation_windows", "wa_usage_monthly", "search_misses",
            "conversations", "api_keys",
        }
        _SAFE_COLS = {"user_id", "company_id"}

        # Delete in dependency order
        for table, col in [
            ("sessions", "user_id"),
        ]:
            assert table in _SAFE_TABLES and col in _SAFE_COLS
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
            assert table in _SAFE_TABLES and col in _SAFE_COLS
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
        log.error("DELETE TEST USER ERROR: %s", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()


# ── Auth endpoints ──────────────────────────────────────────────────────────

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
                        log.info("REGISTRO+PROMO: company=%s code=%s trial_end=%s", company_id, _promo, _trial_end)
            except Exception as pe:
                log.error("REGISTRO PROMO ERROR (non-fatal): %s", repr(pe))

        return {"ok": True, "user_id": user_id, "company_id": str(company_id), "api_key": token, "promo_applied": promo_applied}
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Email ya registrado")
    except HTTPException:
        raise
    except Exception as e:
        log.error("REGISTER ERROR: %s", repr(e))
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
        log.info("LOGIN email: %s", repr(email))
        log.info("LOGIN row found?: %s", bool(row))
        if not row:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        user_id, password_hash = row
        ok = verify_password(password, password_hash)
        log.info("LOGIN verify_password: %s", ok)
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
        log.error("LOGIN ERROR: %s", repr(e))
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
            log.error("AUTH ME ENRICH ERROR: %s", repr(e))
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
        log.error("CREATE COMPANY ERROR:", repr(e))
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
                    "CRÍTICO: NUNCA combines cantidades de productos diferentes. Cada producto es una línea separada. "
                    "'1 hoja de securock + 4 hojas blancas tablaroca' = DOS productos: 1x securock Y 4x tablaroca. "
                    "Securock, durock, tablaroca, tabla roca son productos DISTINTOS — nunca los mezcles. "
                    "'hojas blancas' o 'hojas blancas tablaroca' = tablaroca ultralight/blanca. "
                    "'hoja de securock' o 'hojas de securock' = securock (producto diferente). "
                    "CRÍTICO: Las medidas (4.10, 6.35, 3.05) y calibres (cal 20, cal 22, cal 26) "
                    "son PARTE DEL NOMBRE del producto. NUNCA los separes ni los omitas. "
                    "Interpreta cantidades en texto: 'una'=1, 'un'=1, 'dos'=2, 'media'=0.5. "
                    "Solo corrige errores ortográficos obvios: 'tabla roca'='tablaroca', "
                    "'redemix'='redimix', 'takete'='taquete', 'flamer'='framer', 'durok'='durock'. "
                    "Ejemplos correctos:\n"
                    "'4 postes 4.10 cal 20' → {\"qty\": 4, \"product\": \"postes 4.10 cal 20\"}\n"
                    "'2 canal 4.10 cal 22' → {\"qty\": 2, \"product\": \"canal 4.10 cal 22\"}\n"
                    "'10 poste 6.35 calibre 26' → {\"qty\": 10, \"product\": \"poste 6.35 calibre 26\"}\n"
                    "'1 hoja de securock + 4 hojas blancas tablaroca' → "
                    '[{"qty": 1, "product": "securock"}, {"qty": 4, "product": "tablaroca"}]\n'
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
        log.error("NER ERROR:", repr(e))
        return []

def extract_qty_items_robust(text: str):
    t = (text or "").strip()
    # Strip invisible unicode chars (word joiners, zero-width spaces, etc.)
    # WhatsApp bullet lists inject \u2060 (word joiner) and \u200b (zero-width space)
    t = re.sub(r"[\u2060\u200b\u200c\u200d\ufeff\u00a0]", " ", t)
    t = re.sub(r"[•;]", "\n", t)
    # Treat "+" as line separator: "1 hoja de securock + 4 hojas blancas tablaroca" → two lines
    t = re.sub(r"\s*\+\s*", "\n", t)
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
                            log.info(f"PARSER FILTER: dropping phantom preamble item '{_yp}'")
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
            log.info(f"DUPLICATE WEBHOOK ignored: {MessageSid}")
            return TWIML_OK
        _processed_sids.add(cache_key)
        if len(_processed_sids) > 1000:
            _processed_sids.clear()

    try:
        log.info("TWILIO IN:", {"from": From, "to": To, "body": Body})

        if not Body and int(NumMedia or 0) > 0 and MediaUrl0:
            if "image" in (MediaContentType0 or ""):
                cached = _image_cache.get(MessageSid)
                if cached:
                    Body = cached
                    log.info("TWILIO IMAGE FROM CACHE:", Body[:200])
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
                            log.info("TWILIO IMAGE EXTRACTED:", Body[:200])
                        else:
                            twilio_send_whatsapp(to_user_whatsapp=From, text="📷 Vi tu imagen pero no encontré una lista de productos.\n\nMándame el pedido así:\n10 cemento, 5 varilla 3/8")
                            return TWIML_OK
                    except Exception as e:
                        log.error("TWILIO IMAGE ERROR:", repr(e))
                        twilio_send_whatsapp(to_user_whatsapp=From, text="No pude leer la imagen 😔 Intenta enviarla más clara o escribe el pedido.")
                        return TWIML_OK

        if not Body:
            twilio_send_whatsapp(to_user_whatsapp=From, text="Solo proceso mensajes de texto por ahora 📝")
            return TWIML_OK

        company = get_company_by_twilio_number(To)
        log.info("TWILIO company:", company)

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

        log.info("WHATSAPP ENVIADO OK")
        return TWIML_OK

    except Exception as e:
        log.error("TWILIO WEBHOOK ERROR:", repr(e))
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


# ── Empresa/onboarding routes → routes/empresa.py ────────────────────────────

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



# ── Admin/jerga routes → routes/admin.py ────────────────────────────────────
