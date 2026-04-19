"""
auth.py — Password hashing, API key helpers, and session management.
"""

import hashlib
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
from fastapi import HTTPException, Request

from db import get_conn

log = logging.getLogger("cotizaexpress.auth")

SESSION_COOKIE_NAME = "session"
SESSION_TTL_DAYS = int((os.getenv("SESSION_TTL_DAYS") or "14").strip())


# ── Password helpers ──────────────────────────────────────────────────────

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


# ── API key helpers ───────────────────────────────────────────────────────

def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


API_KEY_PREFIX_LEN = 10

def api_key_prefix(raw_key: str) -> str:
    return raw_key[:API_KEY_PREFIX_LEN] if raw_key else ""


# ── Session management ───────────────────────────────────────────────────

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
    key_hash = hash_api_key(token)
    log.debug("BEARER: prefix=%s", prefix)
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
