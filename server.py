from prompts_cotizabot import COTIZABOT_SYSTEM_PROMPT
import os
from openai import OpenAI
import psycopg2
from psycopg2 import IntegrityError

import bcrypt
from fastapi import HTTPException

COTIZABOT_SYSTEM_PROMPT = """
Eres CotizaBot (CotizaExpress).
Ayudas a cotizar materiales como tablaroca, durock, perfiles y pijas.
Reglas:
- No inventes precios ni existencias.
- Si faltan datos, pide SOLO lo mínimo: ciudad, producto exacto, cantidades/medidas, ¿con IVA?
- Responde claro y en bullets. Si puedes, incluye desglose y total.
"""

DONDEVER_SYSTEM_PROMPT = """
Eres DóndeVer.
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

def _pw_bytes(password: str) -> bytes:
    # Siempre sanitiza igual en register y login
    return (password or "").strip().encode("utf-8")


def hash_password(password: str) -> str:
    pw = _pw_bytes(password)
    # bcrypt límite real: 72 BYTES
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


from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI(title="Clawdbot Server", version="1.0")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

@app.get("/")
def root():
    return {"ok": True, "service": "clawdbot-server"}

@app.get("/health")
def health():
    return {"ok": True}


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://cotizaexpress.com",
        "https://www.cotizaexpress.com",
        "https://buildquote-12.preview.emergentagent.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    db_url = (DATABASE_URL or "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url, sslmode="require", connect_timeout=5)


class RegisterBody(BaseModel):
    email: str
    password: str


class LoginBody(BaseModel):
    email: str
    password: str


class ChatRequest(BaseModel):
    app: str = "cotizabot"
    message: str
    user_id: str = None
    source: str = "web"
    country: str = "MX"


@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/db/ping")
def db_ping():
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)
        cur = conn.cursor()
        cur.execute("select 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/db/ping")
def db_ping():
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)
        cur = conn.cursor()
        cur.execute("select 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from fastapi import HTTPException

# Asegúrate de tener esto ya:

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
        conn.commit()
        return {"ok": True, "user_id": user_id}

    except IntegrityError:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=409, detail="Email ya registrado")

    except HTTPException:
        if conn:
            conn.rollback()
        raise

    except Exception:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail="Error interno")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.post("/api/auth/login")
def login(body: LoginBody):
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

        if not row:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        user_id, password_hash = row

        if not verify_password(password, password_hash):
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        return {"ok": True, "user_id": user_id}

    except HTTPException:
        raise

    except Exception:
        raise HTTPException(status_code=500, detail="Error interno")
  
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



@app.get("/api/health")
def api_health():
    return {"ok": True}

from fastapi import Header

@app.post("/api/chat")
async def chat(req: ChatRequest, authorization: str = Header(default="")):
    app_id = (req.app or "cotizabot").lower().strip()
    user_text = (req.message or "").strip()

    if not user_text:
        return {"reply": "Escribe un mensaje para poder ayudarte."}

    if not openai_client:
        return {"reply": "Falta configurar OPENAI_API_KEY en Render."}

    # Elegir system prompt según app (router)
    if app_id == "cotizabot":
        system_prompt = COTIZABOT_SYSTEM_PROMPT
    elif app_id == "dondever":
        system_prompt = DONDEVER_SYSTEM_PROMPT
    elif app_id == "entiendeusa":
        system_prompt = ENTIENDEUSA_SYSTEM_PROMPT
    else:
        system_prompt = "Eres un asistente útil. Responde claro y directo."

    # (Debug temporal) confirma qué prompt se usa
    print("USING_APP", app_id, "PROMPT_HEAD", system_prompt[:80])

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text}
    ]

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.3,
    )

    reply = response.choices[0].message.content or ""
    return {"reply": reply}
