from prompts_cotizabot import COTIZABOT_SYSTEM_PROMPT

import os
import re
import hashlib
import secrets
import traceback
import requests
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Optional
from twilio.rest import Client

import bcrypt
import psycopg2
from psycopg2 import IntegrityError

from openai import OpenAI

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
from fastapi.responses import Response

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

def normalize_wa(addr: str) -> str:
    a = (addr or "").strip().replace(" ", "")
    if a and not a.startswith("whatsapp:"):
        a = "whatsapp:" + a
    return a

def norm_name(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def twilio_client():
    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    if not sid or not token:
        raise RuntimeError("Falta TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN en Render")
    return Client(sid, token)

def extract_qty_items_robust(text: str):
    t = (text or "").lower()

    # 🔥 FIX crítico: 6 x 1 -> 6x1
    t = re.sub(r"\b(\d+)\s*x\s*(\d+)\b", r"\1x\2", t)

    # normaliza separadores
    t = t.replace(",", " , ")
    t = re.sub(r"\s+y\s+", " , ", t)

    pattern = r"(\d+)\s+([^,]+)"
    matches = re.findall(pattern, t)

    items = []
    for qty, prod in matches:
        prod = prod.strip()
        if prod:
            items.append((int(qty), prod))

    return items

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
    t = (text or "").lower().strip()
    t = re.sub(r"[^a-z0-9áéíóúñü\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    words = [w for w in t.split() if w]
    cleaned = [
        w
        for w in words
        if w
        not in {
            "precio", "precios", "cuánto", "cuanto", "cuesta", "vale", "costo", "cost",
            "cotiza", "cotización", "cotizacion", "presupuesto", "lista", "de", "del",
            "la", "el", "un", "una", "por", "favor", "me", "dime", "oye", "quiero",
            "saber",
        }
    ]
    return " ".join(cleaned).strip() or t


def extract_qty_and_product(text: str):
    t = (text or "").strip().lower()
    m = re.match(r"^\s*(\d+)\s+(.+?)\s*$", t)
    if not m:
        return None, None
    qty = int(m.group(1))
    product = m.group(2).strip()
    return qty, product

def split_clarifications(text: str):
    t = (text or "").lower().strip()
    t = t.replace("+", " ")
    t = re.sub(r"\s+", " ", t)
    parts = [p.strip() for p in t.split(",") if p.strip()]
    out = []
    for p in parts:
        out.extend([s.strip() for s in re.split(r"\s+y\s+", p) if s.strip()])
    return [x for x in out if x]


def looks_like_price_question(text: str) -> bool:
    t = (text or "").lower()
    triggers = [
        "precio", "cuánto", "cuanto", "vale", "costo", "cost",
        "$", "cotiza", "cotización", "cotizacion", "presupuesto",
        "lista de precios", "price", "cuesta",
    ]
    return any(x in t for x in triggers)


# -------------------------
# API keys (bearer legacy for upload/chat)
# -------------------------
def generate_api_key() -> str:
    return secrets.token_urlsafe(32)


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

import json

# -------------------------
# Quote state helpers (tabla quote_states)
# -------------------------
def get_quote_state(company_id: str, wa_from: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT state_json
            FROM quote_states
            WHERE company_id=%s AND wa_from=%s
            LIMIT 1
            """,
            (company_id, wa_from),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


def upsert_quote_state(company_id: str, wa_from: str, state: dict):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO quote_states (company_id, wa_from, state_json, updated_at)
            VALUES (%s, %s, %s::jsonb, now())
            ON CONFLICT (company_id, wa_from)
            DO UPDATE SET
                state_json = EXCLUDED.state_json,
                updated_at = now()
            """,
            (company_id, wa_from, json.dumps(state)),
        )
    finally:
        cur.close()
        conn.close()


def clear_quote_state(company_id: str, wa_from: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM quote_states WHERE company_id=%s AND wa_from=%s",
            (company_id, wa_from),
        )
    finally:
        cur.close()
        conn.close()


# -------------------------
# Clarifications splitter
# -------------------------
def split_clarifications(text: str):
    t = (text or "").lower()
    parts = re.split(r",|\sy\s", t)
    return [p.strip() for p in parts if p.strip()]

def upsert_quote_state(company_id: str, wa_from: str, state: dict):
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
    finally:
        cur.close()
        conn.close()

def clear_quote_state(company_id: str, wa_from: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "delete from wa_quote_state where company_id=%s and wa_from=%s",
            (company_id, wa_from),
        )
    finally:
        cur.close()
        conn.close()

# -------------------------
# WhatsApp tenant lookup
# -------------------------
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

    reply = build_reply_for_company(company["company_id"], text)

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
              AND ({where_sql} OR sku ILIKE %s)
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (*params, f"%{q_clean}%", limit),
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
        return items
    finally:
        cur.close()

# -------------------------
# WhatsApp reply builder
# -------------------------

def extract_qty_items_robust(text: str):
    """
    Extrae (qty, producto) de frases tipo:
    'cotiza 10 tablarocas, 5 postes 4.10, 5 redimix 10 perfacinta y 1000 pijas...'
    """
    t = (text or "").lower()
    t = t.replace("+", " ")
    t = re.sub(r"[•;]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # quita palabras ruido
    t = re.sub(r"\b(cotiza|cotización|cotizacion|precio|precios|por favor|porfa|pls)\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # separa por comas primero
    parts = [p.strip() for p in t.split(",") if p.strip()]
    items = []

    for part in parts:
        # dentro de cada parte, puede venir " ... y 10 ..."
        subparts = [s.strip() for s in re.split(r"\s+y\s+", part) if s.strip()]

        for s in subparts:
            # extrae TODOS los pares (qty, texto hasta antes del siguiente qty) dentro del chunk
            pattern = r"(\d+)\s+(.+?)(?=\s+\d+\s+|$)"
            matches = re.findall(pattern, s)
            for qty_s, prod in matches:
                prod = prod.strip()
                if not prod:
                    continue
                items.append((int(qty_s), prod))

    return items

def build_reply_for_company(company_id: str, user_text: str, wa_from: str) -> str:
    user_text = (user_text or "").strip()

    # =========================================================
    # 1) MULTI-ITEMS (PRIORIDAD MÁXIMA)
    # =========================================================
    multi = extract_qty_items_robust(user_text)
    if multi:
        conn = get_conn()
        try:
            lines = []
            missing_pairs = []
            subtotal = 0.0
            found_any = False

            for qty, prod_raw in multi:
                prod_query = extract_product_query(prod_raw)
                items = search_pricebook(conn, company_id, prod_query, limit=1)

                if not items:
                    missing_pairs.append((qty, prod_raw))
                    continue

                found_any = True
                it = items[0]
                unit = it.get("unit") or "unidad"
                price = float(it.get("price") or 0)
                imp = qty * price
                subtotal += imp
                lines.append(f"- {qty} {unit} de {it['name']} x ${price:,.2f} = ${imp:,.2f}")

            if found_any:
                iva = subtotal * 0.16
                total = subtotal + iva
                msg = (
                    "Cotización rápida:\n"
                    + "\n".join(lines)
                    + f"\n\nSubtotal: ${subtotal:,.2f}"
                    + f"\nIVA (16%): ${iva:,.2f}"
                    + f"\nTotal: ${total:,.2f}"
                )
            else:
                msg = "Veo cantidades, pero no encontré esos productos en el catálogo."

            if missing_pairs:
                msg += (
                    "\n\nNo encontrados (escríbelos más exacto o con SKU):\n"
                    + "\n".join([f"- {q} x {r}" for (q, r) in missing_pairs[:12]])
                )

                upsert_quote_state(company_id, wa_from, {
                    "pending": [{"qty": q, "raw": r} for (q, r) in missing_pairs]
                })
            else:
                clear_quote_state(company_id, wa_from)

            msg += "\n\n¿Agregamos algo más?"
            return msg
        finally:
            conn.close()

    # =========================================================
    # 2) SINGLE ITEM
    # =========================================================
    qty, prod_query = extract_qty_and_product(user_text)

    if qty and prod_query:
        conn = get_conn()
        try:
            items = search_pricebook(conn, company_id, prod_query, limit=1)
        finally:
            conn.close()

        if items:
            it = items[0]
            unit = it.get("unit") or "unidad"
            price = float(it.get("price") or 0)
            subtotal = qty * price
            iva = subtotal * 0.16
            total = subtotal + iva
            return (
                "Cotización rápida:\n"
                f"- {qty} {unit} de {it['name']} x ${price:,.2f} = ${subtotal:,.2f}\n"
                f"IVA (16%): ${iva:,.2f}\n"
                f"Total: ${total:,.2f}\n\n"
                "¿Agregamos otro producto?"
            )

    # =========================================================
    # 3) PRICE QUESTION
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
                lines.append(f"- {it['name']}: ${it['price']:,.2f}{unit}")
            return (
                "Encontré estos precios:\n"
                + "\n".join(lines)
                + "\n\nDime cantidades para cotizar (ej: 10 tablaroca ultralight)."
            )

    # =========================================================
    # 3.5) CONTEXTO DE ACLARACIONES
    # =========================================================
    state = get_quote_state(company_id, wa_from)
    if state and state.get("pending"):
        clarifs = split_clarifications(user_text)

        if clarifs:
            pend = state["pending"]
            mapped = []

            for i, p in enumerate(pend):
                qty = int(p.get("qty") or 0)
                raw = (p.get("raw") or "").strip()
                new_prod = clarifs[i] if i < len(clarifs) else raw
                mapped.append((qty, new_prod))

            conn = get_conn()
            try:
                lines = []
                still_missing = []
                subtotal = 0.0

                for qty, prod_raw in mapped:
                    prod_query = extract_product_query(prod_raw)
                    found = search_pricebook(conn, company_id, prod_query, limit=1)

                    if not found:
                        still_missing.append((qty, prod_raw))
                        continue

                    it = found[0]
                    unit = it.get("unit") or "unidad"
                    price = float(it.get("price") or 0)
                    imp = qty * price
                    subtotal += imp
                    lines.append(f"- {qty} {unit} de {it['name']} x ${price:,.2f} = ${imp:,.2f}")

                if lines:
                    iva = subtotal * 0.16
                    total = subtotal + iva
                    msg = (
                        "Cotización (con tus aclaraciones):\n"
                        + "\n".join(lines)
                        + f"\n\nSubtotal: ${subtotal:,.2f}"
                        + f"\nIVA (16%): ${iva:,.2f}"
                        + f"\nTotal: ${total:,.2f}"
                    )
                else:
                    msg = "Gracias. Aún no pude encontrar esos productos en el catálogo."

                if still_missing:
                    upsert_quote_state(company_id, wa_from, {
                        "pending": [{"qty": q, "raw": r} for (q, r) in still_missing]
                    })
                    msg += "\n\nAún pendientes:\n" + "\n".join(
                        [f"- {q} x {r}" for (q, r) in still_missing[:12]]
                    )
                    msg += "\n\nMándame el nombre exacto o SKU de esos pendientes."
                else:
                    clear_quote_state(company_id, wa_from)
                    msg += "\n\n✅ Listo. ¿Agregamos algo más?"

                return msg
            finally:
                conn.close()

    # =========================================================
    # 4) GUARD ANTI-ALUCINACIÓN
    # =========================================================
    if re.search(r"\b\d+\b", user_text):
        return (
            "Veo cantidades en tu mensaje, pero no pude encontrar esos productos en el catálogo.\n\n"
            "👉 Escríbelos con nombre más exacto o SKU.\n"
            "Ejemplo: '10 tablaroca ultralight usg' o '1000 pija tablaroca 6x1'."
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
                    "reply":
                        "Cotización rápida:\n"
                        f"- {qty} {unit} de {it['name']} x ${price:,.2f} = ${subtotal:,.2f}\n"
                        f"IVA (16%): ${iva:,.2f}\n"
                        f"Total: ${total:,.2f}\n\n"
                        "¿Agregamos otro producto?"
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
                        + "\n\nDime cantidades para armar cotización (ej: 10 tablaroca ultralight)."
                    )
                    return {"reply": reply}
            except Exception:
                pass

    if not openai_client:
        return {"reply": "Falta configurar OPENAI_API_KEY en Render."}

    if app_id == "cotizabot":
        system_prompt = COTIZABOT_SYSTEM_PROMPT
    elif app_id == "dondever":
        system_prompt = DONDEVER_SYSTEM_PROMPT
    elif app_id == "entiendeusa":
        system_prompt = ENTIENDEUSA_SYSTEM_PROMPT
    else:
        system_prompt = "Eres un asistente útil. Responde claro y directo."

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text},
    ]

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.3,
    )

    reply = response.choices[0].message.content or ""
    return {"reply": reply}

from xml.sax.saxutils import escape

@app.post("/webhook/twilio")
async def twilio_webhook(
    From: str = Form(...),
    To: str = Form(...),
    Body: str = Form(...)
):
    From = normalize_wa(From)
    To = normalize_wa(To)
    Body = (Body or "").strip()

    TWIML_OK = Response(
        content='<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
        media_type="text/xml"
    )

    try:
        print("TWILIO IN:", {"from": From, "to": To, "body": Body})

        company = get_company_by_twilio_number(To)
        print("TWILIO company:", company)

        if not company:
            twilio_send_whatsapp(
                to_user_whatsapp=From,
                text="Hola 👋 Este número aún no está ligado a una empresa."
            )
            return TWIML_OK

        reply_text = build_reply_for_company(company["company_id"], Body, wa_from=From)
        print("REPLY TEXT:", repr(reply_text))

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
