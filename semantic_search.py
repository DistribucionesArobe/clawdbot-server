"""
semantic_search.py — Búsqueda semántica para CotizaBot
=======================================================
Reemplaza la búsqueda con LIKE + fuzzy por embeddings OpenAI + pgvector.

PASOS PARA ACTIVAR:
1. Corre la migración SQL (abajo)
2. pip install pgvector
3. Llama a rebuild_embeddings_for_company() una vez por empresa al subir catálogo
4. Reemplaza search_pricebook_best() con semantic_search_best()
"""

import re
import json
import os
from typing import Optional
from openai import OpenAI

# ── cliente OpenAI (reutiliza el que ya tienes) ──────────────────────────────
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

EMBED_MODEL = "text-embedding-3-small"   # barato: $0.02 / millón de tokens
EMBED_DIM   = 1536


# ============================================================
# MIGRACIÓN SQL — ejecuta esto UNA VEZ en tu DB de Render
# ============================================================
MIGRATION_SQL = """
-- 1) Activa la extensión pgvector (ya viene en Render Postgres)
CREATE EXTENSION IF NOT EXISTS vector;

-- 2) Agrega columna de embedding a pricebook_items
ALTER TABLE pricebook_items
  ADD COLUMN IF NOT EXISTS embedding vector(1536);

-- 3) Índice HNSW para búsqueda rápida por cosine similarity
CREATE INDEX IF NOT EXISTS pricebook_items_embedding_idx
  ON pricebook_items
  USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
"""


# ============================================================
# HELPERS DE TEXTO
# ============================================================

def build_product_text(name: str, sku: str = "", unit: str = "") -> str:
    """
    Construye el texto que se embeddea por cada producto.
    Incluimos nombre, SKU y unidad para que el modelo entienda contexto.
    
    Ejemplo: "Poste 4.10 x 2.44 Cal 26 | SKU: ABC123 | Pieza"
    """
    parts = [(name or "").strip()]
    if sku:
        parts.append(f"SKU: {sku.strip()}")
    if unit:
        parts.append(unit.strip())
    return " | ".join(p for p in parts if p)


def build_query_text(user_input: str) -> str:
    """
    Limpia el mensaje del usuario antes de embeddear.
    NO removemos demasiado — el modelo semántico entiende plurales,
    abreviaciones y typos mejor que el regex.
    """
    t = (user_input or "").lower().strip()
    # Solo quitamos ruido obvio de chat
    noise = r"\b(cotiza|cotizame|dame|quiero|necesito|por favor|porfa|pls|precio|precios)\b"
    t = re.sub(noise, " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ============================================================
# GENERAR EMBEDDINGS
# ============================================================

def get_embedding(text: str) -> list[float]:
    """Llama a OpenAI y devuelve el vector."""
    if not openai_client:
        raise RuntimeError("OpenAI client no inicializado")
    
    resp = openai_client.embeddings.create(
        model=EMBED_MODEL,
        input=text,
        encoding_format="float",
    )
    return resp.data[0].embedding


def get_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """
    Embeddings en lote — mucho más eficiente al subir el catálogo.
    OpenAI acepta hasta 2048 inputs por llamada.
    """
    if not openai_client:
        raise RuntimeError("OpenAI client no inicializado")
    if not texts:
        return []

    # Chunks de 500 para no exceder límites
    all_vectors = []
    chunk_size = 500
    for i in range(0, len(texts), chunk_size):
        chunk = texts[i : i + chunk_size]
        resp = openai_client.embeddings.create(
            model=EMBED_MODEL,
            input=chunk,
            encoding_format="float",
        )
        # Orden garantizado por OpenAI
        resp.data.sort(key=lambda x: x.index)
        all_vectors.extend([d.embedding for d in resp.data])
    
    return all_vectors


# ============================================================
# RECONSTRUIR EMBEDDINGS DE TODO EL CATÁLOGO
# Llama esto después de cada carga masiva de productos
# ============================================================

def rebuild_embeddings_for_company(conn, company_id: str) -> dict:
    """
    Regenera embeddings para todos los productos de una empresa.
    
    Retorna: {"updated": int, "skipped": int, "errors": int}
    """
    cur = conn.cursor()
    try:
        # Trae todos los productos (con o sin embedding — los regenera todos)
        cur.execute(
            """
            SELECT id, name, sku, unit
            FROM pricebook_items
            WHERE company_id = %s
            ORDER BY id
            """,
            (company_id,),
        )
        rows = cur.fetchall()

        if not rows:
            return {"updated": 0, "skipped": 0, "errors": 0}

        ids    = [r[0] for r in rows]
        texts  = [build_product_text(r[1], r[2] or "", r[3] or "") for r in rows]

        # Genera todos los embeddings en lote
        vectors = get_embeddings_batch(texts)

        # Actualiza en DB
        updated = 0
        errors  = 0
        for item_id, vector in zip(ids, vectors):
            try:
                cur.execute(
                    """
                    UPDATE pricebook_items
                    SET embedding = %s::vector
                    WHERE id = %s AND company_id = %s
                    """,
                    (json.dumps(vector), item_id, company_id),
                )
                updated += 1
            except Exception as e:
                print(f"ERROR embedding item {item_id}: {e}")
                errors += 1

        print(f"Embeddings rebuilt: company={company_id} updated={updated} errors={errors}")
        return {"updated": updated, "skipped": 0, "errors": errors}

    finally:
        cur.close()


def upsert_single_embedding(conn, company_id: str, item_id: int,
                             name: str, sku: str = "", unit: str = ""):
    """
    Genera y guarda el embedding de UN solo producto.
    Llama esto en pricebook_item_create() después del INSERT.
    """
    text   = build_product_text(name, sku, unit)
    vector = get_embedding(text)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE pricebook_items
            SET embedding = %s::vector
            WHERE id = %s AND company_id = %s
            """,
            (json.dumps(vector), item_id, company_id),
        )
    finally:
        cur.close()


# ============================================================
# BÚSQUEDA SEMÁNTICA — reemplaza search_pricebook_best()
# ============================================================

def semantic_search_best(
    conn,
    company_id: str,
    user_query: str,
    threshold: float = 0.75,   # similitud mínima (0-1). Ajusta según pruebas.
    limit: int = 5,
) -> Optional[dict]:
    """
    Busca el mejor producto semánticamente.
    
    - Si hay 1 resultado con similitud >= threshold → lo devuelve directo
    - Si hay varios con similitud similar → devuelve None (pide al caller que muestre opciones)
    - Si no hay nada → devuelve None
    
    Uso: reemplaza search_pricebook_best() en build_reply_for_company()
    """
    query_text   = build_query_text(user_query)
    query_vector = get_embedding(query_text)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                sku, name, unit, price, vat_rate,
                1 - (embedding <=> %s::vector) AS similarity
            FROM pricebook_items
            WHERE company_id = %s
              AND embedding IS NOT NULL
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (json.dumps(query_vector), company_id,
             json.dumps(query_vector), limit),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        return None

    results = [
        {
            "sku":        r[0],
            "name":       r[1],
            "unit":       r[2],
            "price":      float(r[3]) if r[3] is not None else None,
            "vat_rate":   float(r[4]) if r[4] is not None else None,
            "similarity": float(r[5]),
        }
        for r in rows
    ]

    best = results[0]

    # Si no supera el umbral mínimo → sin resultado
    if best["similarity"] < threshold:
        return None

    # Si el primero es claramente mejor que el segundo → devuelve directo
    if len(results) < 2:
        return best

    second = results[1]
    gap = best["similarity"] - second["similarity"]

    # Gap grande (>0.08) → resultado claro, no preguntes
    if gap >= 0.08:
        return best

    # Gap pequeño → ambigüedad, el caller debe mostrar opciones
    # Retornamos None con candidatos embebidos para que el caller los use
    # (ver semantic_search_candidates)
    return None


def semantic_search_candidates(
    conn,
    company_id: str,
    user_query: str,
    threshold: float = 0.65,
    limit: int = 5,
) -> list[dict]:
    """
    Devuelve hasta `limit` candidatos ordenados por similitud semántica.
    Úsalo cuando semantic_search_best() devuelve None.
    """
    query_text   = build_query_text(user_query)
    query_vector = get_embedding(query_text)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                sku, name, unit, price, vat_rate,
                1 - (embedding <=> %s::vector) AS similarity
            FROM pricebook_items
            WHERE company_id = %s
              AND embedding IS NOT NULL
              AND 1 - (embedding <=> %s::vector) >= %s
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (
                json.dumps(query_vector), company_id,
                json.dumps(query_vector), threshold,
                json.dumps(query_vector), limit,
            ),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    return [
        {
            "sku":        r[0],
            "name":       r[1],
            "unit":       r[2],
            "price":      float(r[3]) if r[3] is not None else None,
            "vat_rate":   float(r[4]) if r[4] is not None else None,
            "similarity": float(r[5]),
        }
        for r in rows
    ]


# ============================================================
# FUNCIÓN UNIFICADA — úsala directamente en build_reply_for_company
# ============================================================

def smart_search(
    conn,
    company_id: str,
    user_query: str,
    qty: int = 0,
) -> dict:
    """
    Función principal. Devuelve un dict con:
    
    {
      "status": "found" | "ambiguous" | "not_found",
      "item":   {...} | None,         # si status=="found"
      "candidates": [...],            # si status=="ambiguous"
    }
    
    Ejemplo de uso en build_reply_for_company():
    
        result = smart_search(conn, company_id, prod_raw, qty)
        
        if result["status"] == "found":
            state = cart_add_item(state, {...result["item"], "qty": qty})
        
        elif result["status"] == "ambiguous":
            missing.append({
                "qty": qty,
                "raw": prod_raw,
                "candidates": result["candidates"],
            })
        
        else:  # not_found
            missing.append({
                "qty": qty,
                "raw": prod_raw,
                "candidates": [],
            })
    """
    # 1) Intenta match directo
    best = semantic_search_best(conn, company_id, user_query)
    if best:
        return {"status": "found", "item": best, "candidates": []}

    # 2) Sin match claro → busca candidatos para mostrar opciones
    candidates = semantic_search_candidates(conn, company_id, user_query, limit=5)
    if candidates:
        return {"status": "ambiguous", "item": None, "candidates": candidates}

    # 3) Nada relevante
    return {"status": "not_found", "item": None, "candidates": []}


# ============================================================
# ENDPOINT FASTAPI — agrega esto a main.py
# POST /api/pricebook/rebuild-embeddings
# ============================================================
"""
@app.post("/api/pricebook/rebuild-embeddings")
def rebuild_embeddings(request: Request):
    company_id = require_company_id(request)
    conn = get_conn()
    try:
        result = rebuild_embeddings_for_company(conn, company_id)
        return {"ok": True, **result}
    finally:
        conn.close()
"""


# ============================================================
# PATCH para pricebook_upload — agrega al final del upload exitoso
# ============================================================
"""
# Al final de pricebook_upload(), antes del return, agrega:
try:
    from semantic_search import rebuild_embeddings_for_company
    rebuild_embeddings_for_company(conn, company_id)
except Exception as e:
    print("EMBEDDINGS REBUILD ERROR (non-fatal):", repr(e))
"""


# ============================================================
# PATCH para pricebook_item_create — agrega embedding al crear 1 producto
# ============================================================
"""
# Al final de pricebook_item_create(), antes del return, agrega:
try:
    from semantic_search import upsert_single_embedding
    upsert_single_embedding(conn, company_id, new_id, name, sku or "", unit or "")
except Exception as e:
    print("SINGLE EMBEDDING ERROR (non-fatal):", repr(e))
"""


# ============================================================
# REEMPLAZO EN build_reply_for_company
# ============================================================
"""
# ANTES (en el bloque multi-items):
best = search_pricebook_best(conn, company_id, prod_query, limit=12)

# DESPUÉS:
from semantic_search import smart_search
result = smart_search(conn, company_id, prod_raw, qty)

if result["status"] == "found":
    best = result["item"]
elif result["status"] == "ambiguous":
    missing.append({"qty": qty, "raw": prod_raw, "candidates": result["candidates"]})
    continue
else:
    missing.append({"qty": qty, "raw": prod_raw, "candidates": []})
    continue
"""
