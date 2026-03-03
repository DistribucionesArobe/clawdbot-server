"""
semantic_search.py — Búsqueda semántica para CotizaBot v3
"""

import re
import os
from typing import Optional
from openai import OpenAI

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

EMBED_MODEL = "text-embedding-3-small"


def build_product_text(name: str, sku: str = "", unit: str = "", synonyms: str = "") -> str:
    parts = [(name or "").strip()]
    if sku:
        parts.append(f"SKU: {sku.strip()}")
    if unit:
        parts.append(unit.strip())
    if synonyms:
        for s in synonyms.split(","):
            s = s.strip()
            if s:
                parts.append(s)
    return " | ".join(p for p in parts if p)


def build_query_text(user_input: str) -> str:
    t = (user_input or "").lower().strip()
    noise = r"\b(cotiza|cotizame|dame|quiero|necesito|por favor|porfa|pls|precio|precios)\b"
    t = re.sub(noise, " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        t = (user_input or "").lower().strip()
    return t


def get_embedding(text: str) -> list:
    if not openai_client:
        raise RuntimeError("OpenAI client no inicializado")
    text = (text or "").strip()
    if not text:
        raise ValueError("texto vacio para embedding")
    resp = openai_client.embeddings.create(
        model=EMBED_MODEL,
        input=text,
        encoding_format="float",
    )
    return resp.data[0].embedding


def get_embeddings_batch(texts: list) -> list:
    if not openai_client:
        raise RuntimeError("OpenAI client no inicializado")
    if not texts:
        return []
    all_vectors = []
    chunk_size = 500
    for i in range(0, len(texts), chunk_size):
        chunk = texts[i : i + chunk_size]
        resp = openai_client.embeddings.create(
            model=EMBED_MODEL,
            input=chunk,
            encoding_format="float",
        )
        resp.data.sort(key=lambda x: x.index)
        all_vectors.extend([d.embedding for d in resp.data])
    return all_vectors


def rebuild_embeddings_for_company(conn, company_id: str) -> dict:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, sku, unit, synonyms FROM pricebook_items WHERE company_id = %s ORDER BY id",
            (company_id,),
        )
        rows = cur.fetchall()
        if not rows:
            return {"updated": 0, "skipped": 0, "errors": 0}

        ids = [r[0] for r in rows]
        texts = [build_product_text(r[1], r[2] or "", r[3] or "", r[4] or "") for r in rows]
        vectors = get_embeddings_batch(texts)

        updated = 0
        errors = 0
        for item_id, vector in zip(ids, vectors):
            try:
                vector_str = "[" + ",".join(str(x) for x in vector) + "]"
                cur.execute(
                    "UPDATE pricebook_items SET embedding = %s::vector WHERE id = %s AND company_id = %s",
                    (vector_str, item_id, company_id),
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
                             name: str, sku: str = "", unit: str = "", synonyms: str = ""):
    text = build_product_text(name, sku, unit, synonyms)
    vector = get_embedding(text)
    vector_str = "[" + ",".join(str(x) for x in vector) + "]"
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE pricebook_items SET embedding = %s::vector WHERE id = %s AND company_id = %s",
            (vector_str, item_id, company_id),
        )
    finally:
        cur.close()


def semantic_search_best(conn, company_id: str, user_query: str,
                          threshold: float = 0.78, limit: int = 5) -> Optional[dict]:
    query_text = build_query_text(user_query)
    query_vector = get_embedding(query_text)
    vector_str = "[" + ",".join(str(x) for x in query_vector) + "]"
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT sku, name, unit, price, vat_rate,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM pricebook_items
            WHERE company_id = %s AND embedding IS NOT NULL
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (vector_str, company_id, vector_str, limit),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
    if not rows:
        return None
    results = [
        {
            "sku": r[0], "name": r[1], "unit": r[2],
            "price": float(r[3]) if r[3] is not None else None,
            "vat_rate": float(r[4]) if r[4] is not None else None,
            "similarity": float(r[5]),
        }
        for r in rows
    ]
    best = results[0]
    print(f"SEMANTIC TOP: query='{user_query}' best='{best['name']}' score={best['similarity']:.3f}")
    if best["similarity"] < threshold:
        print(f"SEMANTIC: below threshold {threshold}, returning None")
        return None
    if len(results) < 2:
        return best
    second = results[1]
    gap = best["similarity"] - second["similarity"]
    print(f"SEMANTIC: gap={gap:.3f} second='{second['name']}' score={second['similarity']:.3f}")
    if gap >= 0.06:
        return best
    return None


def semantic_search_candidates(conn, company_id: str, user_query: str,
                                threshold: float = 0.45, limit: int = 5) -> list:
    query_text = build_query_text(user_query)
    query_vector = get_embedding(query_text)
    vector_str = "[" + ",".join(str(x) for x in query_vector) + "]"

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT sku, name, unit, price, vat_rate,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM pricebook_items
            WHERE company_id = %s AND embedding IS NOT NULL
              AND 1 - (embedding <=> %s::vector) >= %s
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (vector_str, company_id, vector_str, threshold, vector_str, limit),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    candidates = [
        {
            "sku": r[0], "name": r[1], "unit": r[2],
            "price": float(r[3]) if r[3] is not None else None,
            "vat_rate": float(r[4]) if r[4] is not None else None,
            "similarity": float(r[5]),
        }
        for r in rows
    ]
    print(f"SEMANTIC CANDIDATES: query='{user_query}' found={len(candidates)}")
    return candidates


def fuzzy_search_best(conn, company_id: str, user_query: str, threshold: int = 95) -> Optional[dict]:
    from rapidfuzz import fuzz

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT sku, name, unit, price, vat_rate, synonyms
            FROM pricebook_items
            WHERE company_id = %s AND embedding IS NOT NULL
            """,
            (company_id,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        return None

    q = user_query.lower().strip()
    best_score = 0
    best_item = None

    for r in rows:
        name = (r[1] or "").lower()
        syns = [s.strip().lower() for s in (r[5] or "").split(",") if s.strip()]
        all_terms = [name] + syns
        score = max(
            max(fuzz.token_set_ratio(q, t), fuzz.partial_ratio(q, t))
            for t in all_terms
        )
        if score > best_score:
            best_score = score
            best_item = r

    if best_score < threshold or not best_item:
        return None

    print(f"FUZZY TOP: query='{user_query}' best='{best_item[1]}' score={best_score}")
    return {
        "sku": best_item[0],
        "name": best_item[1],
        "unit": best_item[2],
        "price": float(best_item[3]) if best_item[3] is not None else None,
        "vat_rate": float(best_item[4]) if best_item[4] is not None else None,
    }



def resolve_global_synonym(conn, q: str) -> str:
    """
    Busca q en global_synonyms y devuelve termino_busqueda.
    Ejemplo: 'drywall' → 'tablaroca', 'perico' → 'llave ajustable'
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT termino_busqueda FROM global_synonyms WHERE sinonimo = %s LIMIT 1",
            (q.lower().strip(),),
        )
        row = cur.fetchone()
        if row:
            print(f"GLOBAL SYNONYM: '{q}' → '{row[0]}'")
            return row[0]
        return q
    except Exception:
        return q
    finally:
        cur.close()


def smart_search(conn, company_id: str, user_query: str, qty: int = 0) -> dict:  # noqa
    try:
        from rapidfuzz import fuzz

        q = user_query.lower().strip()

        # Limpiar stopwords de construcción para mejorar búsqueda
        # "pija para tablaroca" → "pija tablaroca", "tornillo de madera" → "tornillo madera"
        _stopwords = {"para", "de", "del", "la", "el", "un", "una", "con", "sin", "los", "las"}
        q_tokens = [t for t in q.split() if t not in _stopwords]
        q = " ".join(q_tokens).strip() or q

        # =========================================================
        # PASO -1: Resolver sinónimo global (drywall→tablaroca, perico→llave ajustable)
        # =========================================================
        q_resolved = resolve_global_synonym(conn, q)
        if q_resolved != q:
            return smart_search(conn, company_id, q_resolved, qty)

        # =========================================================
        # PASO 0: Búsqueda directa por sinónimo exacto en DB
        # Resuelve: "permabase" → "Durock USG"
        # =========================================================
        cur0 = conn.cursor()
        try:
            cur0.execute(
                """
                SELECT sku, name, unit, price, vat_rate
                FROM pricebook_items
                WHERE company_id = %s
                  AND synonyms ILIKE %s
                LIMIT 5
                """,
                (company_id, f"%{q}%"),
            )
            syn_rows = cur0.fetchall()
        finally:
            cur0.close()

        if len(syn_rows) == 1:
            r = syn_rows[0]
            print(f"SYNONYM DIRECT HIT: query='{user_query}' match='{r[1]}'")
            return {"status": "found", "item": {
                "sku": r[0], "name": r[1], "unit": r[2],
                "price": float(r[3]) if r[3] is not None else None,
                "vat_rate": float(r[4]) if r[4] is not None else None,
            }, "candidates": []}
        elif len(syn_rows) > 1:
            print(f"SYNONYM AMBIGUOUS: query='{user_query}' found={len(syn_rows)}")
            return {"status": "ambiguous", "item": None, "candidates": [
                {"sku": r[0], "name": r[1], "unit": r[2],
                 "price": float(r[3]) if r[3] is not None else None,
                 "vat_rate": float(r[4]) if r[4] is not None else None}
                for r in syn_rows
            ]}

        # =========================================================
        # PASO 1: Búsqueda directa por nombre ILIKE
        # Resuelve: "pijas" → productos con "pija" en el nombre
        # También intenta singular si no hay resultados
        # =========================================================
        def _name_search(term):
            c = conn.cursor()
            try:
                c.execute(
                    """
                    SELECT sku, name, unit, price, vat_rate
                    FROM pricebook_items
                    WHERE company_id = %s AND name ILIKE %s
                    LIMIT 10
                    """,
                    (company_id, f"%{term}%"),
                )
                return c.fetchall()
            finally:
                c.close()

        pool_rows = _name_search(q)
        if not pool_rows and q.endswith("s") and len(q) > 3:
            pool_rows = _name_search(q[:-1])  # intenta singular

        if pool_rows:
            if len(pool_rows) == 1:
                r = pool_rows[0]
                print(f"NAME DIRECT HIT: query='{user_query}' match='{r[1]}'")
                return {"status": "found", "item": {
                    "sku": r[0], "name": r[1], "unit": r[2],
                    "price": float(r[3]) if r[3] is not None else None,
                    "vat_rate": float(r[4]) if r[4] is not None else None,
                }, "candidates": []}
            else:
                print(f"NAME AMBIGUOUS: query='{user_query}' found={len(pool_rows)}")
                return {"status": "ambiguous", "item": None, "candidates": [
                    {"sku": r[0], "name": r[1], "unit": r[2],
                     "price": float(r[3]) if r[3] is not None else None,
                     "vat_rate": float(r[4]) if r[4] is not None else None}
                    for r in pool_rows[:5]
                ]}

        # =========================================================
        # PASO 2: Fuzzy con sinónimos (threshold 80, con desempate)
        # =========================================================
        cur2 = conn.cursor()
        try:
            cur2.execute(
                """
                SELECT sku, name, unit, price, vat_rate, synonyms
                FROM pricebook_items
                WHERE company_id = %s AND embedding IS NOT NULL
                """,
                (company_id,),
            )
            rows = cur2.fetchall()
        finally:
            cur2.close()

        scored = []
        for r in rows:
            name = (r[1] or "").lower()
            syns = [s.strip().lower() for s in (r[5] or "").split(",") if s.strip()]
            all_terms = [name] + syns
            score = max(
                max(fuzz.token_set_ratio(q, t), fuzz.partial_ratio(q, t))
                for t in all_terms
            )
            if score >= 80:
                scored.append((score, {
                    "sku": r[0], "name": r[1], "unit": r[2],
                    "price": float(r[3]) if r[3] is not None else None,
                    "vat_rate": float(r[4]) if r[4] is not None else None,
                }))

        scored.sort(key=lambda x: x[0], reverse=True)

        if len(scored) == 1:
            print(f"FUZZY UNIQUE: query='{user_query}' match='{scored[0][1]['name']}' score={scored[0][0]}")
            return {"status": "found", "item": scored[0][1], "candidates": []}

        if len(scored) > 1:
            top_score = scored[0][0]
            second_score = scored[1][0]
            # Si hay un ganador claro (>= 95 y gap >= 10), agrega directo
            if top_score >= 95 and (top_score - second_score) >= 10:
                print(f"FUZZY CLEAR WIN: query='{user_query}' match='{scored[0][1]['name']}' score={top_score}")
                return {"status": "found", "item": scored[0][1], "candidates": []}
            print(f"FUZZY AMBIGUOUS: query='{user_query}' found={len(scored)}")
            return {"status": "ambiguous", "item": None, "candidates": [s[1] for s in scored[:5]]}

        # =========================================================
        # PASO 3: Semántico (threshold más bajo para queries cortas)
        # =========================================================
        words = user_query.strip().split()
        cand_threshold = 0.20 if len(words) == 1 else 0.30 if len(words) == 2 else 0.40

        candidates = semantic_search_candidates(conn, company_id, user_query,
                                                threshold=cand_threshold, limit=5)
        if candidates:
            print(f"SEMANTIC CANDIDATES: query='{user_query}' found={len(candidates)}")
            return {"status": "ambiguous", "item": None, "candidates": candidates}

        return {"status": "not_found", "item": None, "candidates": []}

    except Exception as e:
        print(f"SMART SEARCH EXCEPTION: query='{user_query}' error={repr(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "not_found", "item": None, "candidates": []}
