import os
import psycopg2
from psycopg2 import IntegrityError
from passlib.context import CryptContext


from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI(title="Clawdbot Server", version="1.0")
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

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)


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
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


@app.post("/api/auth/register")
def register(body: RegisterBody):
    email = (body.email or "").strip().lower()
    password = (body.password or "").strip()

    # Validaciones básicas
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")
    if not password:
        raise HTTPException(status_code=400, detail="Password requerido")

    # bcrypt: límite real es 72 BYTES en UTF-8
    pw_bytes = password.encode("utf-8")
    if len(pw_bytes) > 72:
        raise HTTPException(status_code=400, detail="Password demasiado largo (máx 72 bytes)")

    # ✅ UN SOLO HASH
    password_hash = pwd_context.hash(password)

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "insert into users (email, password_hash) values (%s, %s) returning id",
            (email, password_hash),
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        return {"ok": True, "user_id": user_id}

    except IntegrityError:
        if conn:
            conn.rollback()
        # Mejor 409 (conflict) para email duplicado
        raise HTTPException(status_code=409, detail="Email ya registrado")

    except HTTPException:
        if conn:
            conn.rollback()
        raise

    except Exception:
        if conn:
            conn.rollback()
        # No expongas el error crudo en prod
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

    # Evitar user enumeration: mismo mensaje para todo
    if not email or not password:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    # bcrypt límite 72 bytes
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

        # Verificación (passlib)
        if not pwd_context.verify(password, password_hash):
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


@app.post("/api/chat")
def chat(req: ChatRequest, authorization: str = Header(default="")):


    app_id = (req.app or "cotizabot").lower().strip()
    msg = (req.message or "").lower().strip()



    # --- CotizaBot ---
    if app_id == "cotizabot":
        quote_kw = [
            "cotiza", "cotización", "cotizacion", "precio", "cuánto", "cuanto",
            "costo", "m2", "metros", "tablaroca", "durock", "pijas", "panel", "perfil"
        ]
        if any(k in msg for k in quote_kw):
            return {"reply": "📦 *CotizaBot*: Dime 1) ciudad 2) producto y cantidades (o m²) 3) ¿con IVA?"}
        return {"reply": "📦 *CotizaBot*: ¿Qué quieres cotizar? (ej: 'tablaroca 20 hojas en MTY con IVA')"}

    # --- DóndeVer ---
    if app_id == "dondever":
        sports_kw = [
            "america", "américa", "chivas", "tigres", "rayados",
            "liga mx", "champions", "nba", "nfl", "donde ver", "canal", "stream"
        ]
        if any(k in msg for k in sports_kw):
            return {"reply": "⚽ *DóndeVer*: Dime el partido y el país (MX/USA) y te digo canales/plataformas."}
        return {"reply": "⚽ *DóndeVer*: ¿Qué partido buscas?"}

    # --- EntiendeUSA ---
    if app_id == "entiendeusa":
        if not msg:
            return {"reply": "🇺🇸 *EntiendeUSA*: mándame el texto a traducir o explicar."}
        return {"reply": f"🇺🇸 *EntiendeUSA* (demo): recibí '{req.message}'."}

    return {"reply": f"App '{app_id}' no existe. Usa: cotizabot | dondever | entiendeusa"}
