"""
auth.py — Password hashing, API key helpers, and session management.
"""

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
from fastapi import HTTPException

log = logging.getLogger("cotizaexpress.auth")


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
