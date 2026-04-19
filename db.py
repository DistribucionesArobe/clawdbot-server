"""
db.py — Database connection pool for CotizaExpress.

Uses psycopg2 ThreadedConnectionPool with a wrapper that makes
conn.close() return connections to the pool instead of closing them.
"""

import logging
import os

import psycopg2
from psycopg2 import pool as _pg_pool

log = logging.getLogger("cotizaexpress.db")

_connection_pool: _pg_pool.ThreadedConnectionPool | None = None


def _get_pool() -> _pg_pool.ThreadedConnectionPool:
    """Inicializa el pool de conexiones (lazy, thread-safe)."""
    global _connection_pool
    if _connection_pool is None or _connection_pool.closed:
        dsn = (os.getenv("DATABASE_URL") or "").strip()
        if not dsn:
            raise RuntimeError("DATABASE_URL missing")
        if "sslmode=" not in dsn:
            dsn = dsn + ("&" if "?" in dsn else "?") + "sslmode=require"
        _connection_pool = _pg_pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=dsn,
            connect_timeout=5,
        )
        log.info("DB POOL: initialized (min=2, max=20)")
    return _connection_pool


class _PooledConnection:
    """Wrapper que intercepta .close() para devolver al pool en vez de cerrar."""

    def __init__(self, real_conn, pool):
        self._conn = real_conn
        self._pool = pool

    def close(self):
        try:
            if self._pool and not self._pool.closed:
                self._pool.putconn(self._conn)
            else:
                self._conn.close()
        except Exception:
            try:
                self._conn.close()
            except Exception:
                pass

    def cursor(self, *a, **kw):
        return self._conn.cursor(*a, **kw)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    @property
    def autocommit(self):
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, val):
        self._conn.autocommit = val

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def get_conn():
    """Obtiene una conexión del pool. conn.close() la devuelve al pool automáticamente."""
    pool = _get_pool()
    real_conn = pool.getconn()
    real_conn.autocommit = True
    return _PooledConnection(real_conn, pool)


def print_db_fingerprint():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("select inet_server_addr(), inet_server_port(), current_database()")
        log.debug("DB FINGERPRINT: %s", cur.fetchone())
        conn.close()
    except Exception as e:
        log.error("DB FINGERPRINT ERROR: %s", repr(e))
