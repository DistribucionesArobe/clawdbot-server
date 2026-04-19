"""
middleware.py — Rate limiting and request middleware for CotizaExpress.
"""

import asyncio
import logging
import time
from collections import defaultdict

from fastapi import FastAPI, Request, Response

log = logging.getLogger("cotizaexpress.middleware")

# ── Rate limiting (in-memory, per-IP, sliding window) ──────────────────────
RATE_LIMITS = {
    "/webhook/whatsapp": {"max_requests": 120, "window_seconds": 60},
    "/api/auth/login": {"max_requests": 10, "window_seconds": 60},
    "/api/auth/register": {"max_requests": 5, "window_seconds": 60},
    "_default": {"max_requests": 60, "window_seconds": 60},
}

_rate_store: dict[str, list[float]] = defaultdict(list)


def register_middleware(app: FastAPI):
    """Attach rate limiting middleware and cleanup task to the FastAPI app."""

    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        client_ip = request.headers.get(
            "x-forwarded-for",
            request.client.host if request.client else "unknown",
        )
        client_ip = client_ip.split(",")[0].strip()
        path = request.url.path

        limit_cfg = RATE_LIMITS.get(path, RATE_LIMITS["_default"])
        max_req = limit_cfg["max_requests"]
        window = limit_cfg["window_seconds"]

        bucket = path if path in RATE_LIMITS else "_default"
        rate_key = f"{client_ip}:{bucket}"

        now = time.time()
        _rate_store[rate_key] = [t for t in _rate_store[rate_key] if now - t < window]

        if len(_rate_store[rate_key]) >= max_req:
            log.warning("RATE LIMIT: %s hit %d/%ds on %s", client_ip, max_req, window, path)
            return Response(
                content='{"detail":"Too many requests"}',
                status_code=429,
                media_type="application/json",
            )

        _rate_store[rate_key].append(now)
        return await call_next(request)

    async def _cleanup_rate_store():
        while True:
            await asyncio.sleep(600)
            now = time.time()
            stale = [k for k, v in _rate_store.items() if not v or now - v[-1] > 120]
            for k in stale:
                del _rate_store[k]

    @app.on_event("startup")
    async def _start_rate_cleanup():
        asyncio.create_task(_cleanup_rate_store())
