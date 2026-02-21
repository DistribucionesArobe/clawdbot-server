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
from fastapi.responses import StreamingResponse
from openpyxl import Workbook


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

import re

def extract_product_query(text: str) -> str:
    t = (text or "").lower().strip()
    t = re.sub(r"[^a-z0-9áéíóúñü\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    words = [w for w in t.split() if w]

    cleaned = [w for w in words if w not in {
        "precio","precios","cuánto","cuanto","cuesta","vale","costo","cost",
        "cotiza","cotización","cotizacion","presupuesto","lista","de","del","la","el",
        "un","una","por","favor","me","dime","oye","quiero","saber"
    }]

    return " ".join(cleaned).strip() or t


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
SESSION_COOKIE_NAME = "session"
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "14"))


def generate_api_key() -> str:
    return secrets.token_urlsafe(32)

def api_key_prefix(token: str) -> str:
    return token[:API_KEY_PREFIX_LEN]

def api_key_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone


app = FastAPI(title="Clawdbot Server", version="1.0")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

DATABASE_URL = os.getenv("DATABASE_URL")

# -------------------------
# Version check
# -------------------------
@app.get("/api/_version")
def _version():
    return {"version": "pricebook-v2-2026-02-12"}

# -------------------------
# DB
# -------------------------
def get_conn():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    conn.autocommit = True
    return conn

def create_session(conn, user_id: int) -> str:
    sid = secrets.token_urlsafe(32)
    exp = datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sessions (id, user_id, expires_at) VALUES (%s, %s, %s)",
        (sid, user_id, exp),
    )
    return sid


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
            SELECT u.id, u.email
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
        user_id, email = row
        return {"id": int(user_id), "email": email}
    finally:
        if cur: cur.close()
        if conn: conn.close()


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
        conn.commit()

        return {"company_id": str(company_id), "api_key_id": str(api_key_id)}

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



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

from fastapi import Request, HTTPException, Query
import os
import psycopg2

from fastapi import Query, HTTPException
from starlette.requests import Request
import os

from fastapi import Query, HTTPException
from starlette.requests import Request
import os

@app.get("/api/pricebook/items")
def pricebook_items(
    request: Request,
    q: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
):
    conn = None
    cur = None
    try:
        user = get_user_from_session(request)
        user_id = int(user["id"])  # ok (aunque aquí no lo uses aún)

        conn = get_conn()
        cur = conn.cursor()

        company_id = os.getenv("DEFAULT_COMPANY_ID")
        if not company_id:
            raise HTTPException(status_code=500, detail="DEFAULT_COMPANY_ID missing en Render")

        if q:
            like = f"%{q.strip()}%"
            cur.execute(
                """
                select id, company_id, sku, name, unit, price, vat_rate, source, updated_at, created_at
                from pricebook_items
                where company_id = %s
                  and (
                       sku ilike %s
                    or name ilike %s
                    or name_norm ilike %s
                  )
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
                    "name": r[3],
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
        # Limpia transacción si autocommit no está (o si en tu get_conn no lo pusiste)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=f"pricebook_items failed: {type(e).__name__}: {e}")
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

@app.get("/api/db/test")
def db_test():
    conn = None
    try:
        db_url = os.getenv("DATABASE_URL", "").strip()
        if not db_url:
            return {"db_ok": False, "error": "DATABASE_URL no está configurada en Render."}

        conn = psycopg2.connect(db_url, sslmode="require", connect_timeout=5)
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
        "https://ferreteria-whatsapp.emergent.host",
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

from fastapi import HTTPException, Response

@app.post("/api/auth/login")
def login(body: LoginBody, response: Response):
    email = (body.email or "").strip().lower()
    password = (body.password or "").strip()

    if not email or not password:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    # bcrypt solo usa los primeros 72 bytes
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

        # 🔍 DEBUG TEMPORAL
        print("LOGIN email:", repr(email))
        print("LOGIN row found?:", bool(row))
        if row:
            print("LOGIN user_id:", row[0])
            print("LOGIN hash prefix:", str(row[1])[:10])

        if not row:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        user_id, password_hash = row

        ok = verify_password(password, password_hash)
        print("LOGIN verify_password:", ok)
        if not ok:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        sid = create_session(conn, int(user_id))
        conn.commit()

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

    except Exception:
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
            conn.commit()
        finally:
            if cur: cur.close()
            if conn: conn.close()

    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        domain=".cotizaexpress.com",
)

@app.get("/api/web/pricebook/template")
def download_template(request: Request):
    _ = get_user_from_session(request)  # exige sesión por cookie

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

def looks_like_price_question(text: str) -> bool:
    t = (text or "").lower()
    triggers = [
        "precio", "cuánto", "cuanto", "vale", "costo", "cost",
        "$", "cotiza", "cotización", "cotizacion", "presupuesto",
        "lista de precios", "price", "cuesta"
    ]
    return any(x in t for x in triggers)

import re

def extract_qty_and_product(text: str):
    """
    Ejemplos que entiende:
    - "10 tablaroca ultralight"
    - "2 basecoat gris"
    - "15 pija tablaroca"
    """
    t = (text or "").strip().lower()
    m = re.match(r"^\s*(\d+)\s+(.+?)\s*$", t)
    if not m:
        return None, None
    qty = int(m.group(1))
    product = m.group(2).strip()
    return qty, product


@app.get("/api/health")
def api_health():
    return {"ok": True}

from fastapi import Header

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
            items.append({
                "sku": sku,
                "name": name,
                "unit": unit,
                "price": float(price) if price is not None else None,
                "vat_rate": float(vat_rate) if vat_rate is not None else None,
                "updated_at": updated_at.isoformat() if updated_at else None,
            })
        return items
    finally:
        cur.close()


@app.post("/api/chat")
async def chat(req: ChatRequest, authorization: str = Header(default="")):
    app_id = (getattr(req, "app", None) or "cotizabot").lower().strip()
    user_text = (getattr(req, "message", None) or "").strip()

    if not user_text:
        return {"reply": "Escribe un mensaje para poder ayudarte."}

    # --- SOLO CotizaBot usa catálogo ---
    if app_id == "cotizabot":

        # -----------------------------
        # 1️⃣ Cantidad + producto
        # -----------------------------
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

                return {"reply":
                    "Cotización rápida:\n"
                    f"- {qty} {unit} de {it['name']} x ${price:,.2f} = ${subtotal:,.2f}\n"
                    f"IVA (16%): ${iva:,.2f}\n"
                    f"Total: ${total:,.2f}\n\n"
                    "¿Agregamos otro producto?"
                }

            except Exception:
                pass

        # -----------------------------
        # 2️⃣ Consulta de precio simple
        # -----------------------------
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

                    reply = "Encontré estos precios en tu catálogo:\n" + "\n".join(lines) + \
                            "\n\nDime cantidades para armar cotización (ej: 10 tablaroca ultralight)."
                    return {"reply": reply}
            except Exception:
                pass

    # -----------------------------
    # 3️⃣ Fallback a OpenAI
    # -----------------------------
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
        {"role": "user", "content": user_text}
    ]

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.3,
    )

    reply = response.choices[0].message.content or ""
    return {"reply": reply}
