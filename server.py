from prompts_cotizabot import COTIZABOT_SYSTEM_PROMPT
import os
from openai import OpenAI
import psycopg2
from psycopg2 import IntegrityError
from fastapi import UploadFile, File
from openpyxl import load_workbook
from io import BytesIO
import bcrypt
from fastapi import HTTPException

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

def norm_name(s: str) -> str:
    return " ".join((s or "").strip().lower().split())
    
def verify_password(password: str, password_hash: str) -> bool:
    pw = _pw_bytes(password)
    if len(pw) > 72:
        return False
    if not password_hash:
        return False
    return bcrypt.checkpw(pw, password_hash.encode("utf-8"))

import hashlib
import secrets

API_KEY_PREFIX_LEN = 10

def generate_api_key() -> str:
    return secrets.token_urlsafe(32)

def api_key_prefix(token: str) -> str:
    return token[:API_KEY_PREFIX_LEN]

def api_key_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI(title="Clawdbot Server", version="1.0")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    db_url = (DATABASE_URL or "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url, sslmode="require", connect_timeout=5)
    
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
        cur.execute("""
            SELECT id, company_id
            FROM api_keys
            WHERE prefix = %s
              AND key_hash = %s
              AND revoked_at IS NULL
            LIMIT 1
        """, (prefix, key_hash))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid or revoked token")

        api_key_id, company_id = row

        cur.execute("UPDATE api_keys SET last_used_at = now() WHERE id = %s", (api_key_id,))
        conn.commit()

        return {"company_id": str(company_id), "api_key_id": str(api_key_id)}

    finally:
        if cur: cur.close()
        if conn: conn.close()


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
    
class CompanyCreateBody(BaseModel):
    name: str
    slug: str | None = None
    key_name: str = "default"


@app.get("/")
def root():
    return {"ok": True, "service": "clawdbot-server"}

@app.get("/api/whoami")
def whoami(authorization: str = Header(default="")):
    tenant = get_company_from_bearer(authorization)
    return {"ok": True, **tenant}

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

        # Log del upload
        cur.execute(
            """
            INSERT INTO pricebook_uploads (company_id, filename, status)
            VALUES (%s, %s, 'processing')
            RETURNING id
            """,
            (company_id, file.filename),
        )
        upload_id = cur.fetchone()[0]
        conn.commit()

        # Leer Excel
        content = file.file.read()
        wb = load_workbook(BytesIO(content))
        ws = wb.active

        # --- Headers (fila 1) ---
        header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        headers_raw = [str(h or "") for h in header_row]
        headers_norm = [h.strip().lower() for h in headers_raw]

        # --- Aliases ES -> EN (y variantes comunes) ---
        alias = {
            # name
            "nombre": "name",
            "producto": "name",
            "product": "name",

            # price
            "precio": "price",
            "precio_base": "price",
            "precio unitario": "price",
            "costo": "price",
            "cost": "price",

            # unit
            "unidad": "unit",
            "uom": "unit",

            # category
            "categoria": "category",
            "categoría": "category",

            # description
            "descripcion": "description",
            "descripción": "description",

            # stock
            "existencia": "stock",
            "inventario": "stock",
        }

        headers_mapped = [alias.get(h, h) for h in headers_norm]
        idx = {h: i for i, h in enumerate(headers_mapped)}

        # Requerimos solo name y price
        required = {"name", "price"}
        missing = required - set(headers_mapped)
        if missing:
            # debug útil (opcional): muestra qué headers vio el server
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

        conn.commit()

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
        conn.commit()

        return {
            "ok": True,
            "company_id": company_id,
            "upload_id": str(upload_id),
            "rows_total": rows_total,
            "rows_upserted": rows_upserted,
            "rows_skipped": rows_skipped,
        }

    except HTTPException as e:
        if conn and cur and upload_id:
            cur.execute(
                """
                UPDATE pricebook_uploads
                SET status='failed', error=%s, finished_at=now()
                WHERE id=%s
                """,
                (str(e.detail), upload_id),
            )
            conn.commit()
        raise

    except Exception as e:
        if conn and cur and upload_id:
            cur.execute(
                """
                UPDATE pricebook_uploads
                SET status='failed', error=%s, finished_at=now()
                WHERE id=%s
                """,
                (str(e), upload_id),
            )
            conn.commit()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.get("/api/db/test")
def db_test():
    conn = None
    try:
        db_url = os.getenv("DATABASE_URL", "").strip()
        if not db_url:
            return {"db_ok": False, "error": "DATABASE_URL no está configurada en Render."}

        conn = psycopg2.connect(db_url, connect_timeout=5)
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

        cur.execute("""
            INSERT INTO companies (name, slug)
            VALUES (%s, %s)
            RETURNING id
        """, (name, slug))
        company_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO api_keys (company_id, name, prefix, key_hash)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (company_id, key_name, prefix, key_hash))
        api_key_id = cur.fetchone()[0]

        conn.commit()

        return {
            "ok": True,
            "company_id": str(company_id),
            "api_key_id": str(api_key_id),
            "api_key": token,          # GUARDA ESTO: se muestra una sola vez
            "api_key_prefix": prefix
        }

    except IntegrityError:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=409, detail="Slug ya existe o conflicto")

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cur: cur.close()
        if conn: conn.close()


@app.get("/api/health")
def api_health():
    return {"ok": True}

from fastapi import Header

@app.post("/api/chat")
async def chat(req: ChatRequest, authorization: str = Header(default="")):
    app_id = (getattr(req, "app", None) or "cotizabot").lower().strip()
    user_text = (getattr(req, "message", None) or "").strip()

    if not user_text:
        return {"reply": "Escribe un mensaje para poder ayudarte."}

    if not openai_client:
        return {"reply": "Falta configurar OPENAI_API_KEY en Render."}

    # Router por app
    if app_id == "cotizabot":
        system_prompt = COTIZABOT_SYSTEM_PROMPT
    elif app_id == "dondever":
        system_prompt = DONDEVER_SYSTEM_PROMPT
    elif app_id == "entiendeusa":
        system_prompt = ENTIENDEUSA_SYSTEM_PROMPT
    else:
        system_prompt = "Eres un asistente útil. Responde claro y directo."

    # Debug temporal (verifica que está usando el prompt correcto)
    print("USING_APP", app_id, "PROMPT_HEAD", system_prompt[:120])

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
