"""
semantic_search.py — Búsqueda semántica para CotizaBot v7
Novedades vs v5:
  - smart_search acepta cart_context
  - _gpt_catalog_fallback recibe cart_context y lo pasa al prompt
  - GPT entiende "placas" como plafón cuando el pedido tiene "Tee 61"
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
    t = re.sub(r"(\d+)\s*/\s*(\d+)", r"\1/\2", t)
    noise_intent = r"\b(cotiza|cotizame|dame|quiero|necesito|por favor|porfa|pls|precio|precios)\b"
    t = re.sub(noise_intent, " ", t)
    noise_units = r"\b(cubeta|cubetas|bulto|bultos|bolsa|bolsas|rollo|rollos|pieza|piezas|metro|metros|kilo|kilos|kilogramo|kilogramos|litro|litros|par|pares|juego|juegos|caja|cajas|saco|sacos|bote|botes|lata|latas|tubo|tubos|tira|tiras|hoja|hojas)\b"
    t = re.sub(noise_units, " ", t)
    t = re.sub(r"^\s*\d+\s+", "", t)
    t = re.sub(r"(?<![/\d])(\b\d\b)(?![/\d])", " ", t)
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
    print(f"SEMANTIC CANDS DETAIL: {[(c['name'], round(c['similarity'],3)) for c in candidates]}")
    return candidates


def fuzzy_search_best(conn, company_id: str, user_query: str, threshold: int = 95) -> Optional[dict]:
    from rapidfuzz import fuzz
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT sku, name, unit, price, vat_rate, synonyms FROM pricebook_items WHERE company_id = %s AND embedding IS NOT NULL",
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
        score = max(max(fuzz.token_set_ratio(q, t), fuzz.partial_ratio(q, t)) for t in all_terms)
        if score > best_score:
            best_score = score
            best_item = r
    if best_score < threshold or not best_item:
        return None
    print(f"FUZZY TOP: query='{user_query}' best='{best_item[1]}' score={best_score}")
    return {
        "sku": best_item[0], "name": best_item[1], "unit": best_item[2],
        "price": float(best_item[3]) if best_item[3] is not None else None,
        "vat_rate": float(best_item[4]) if best_item[4] is not None else None,
    }


def resolve_global_synonym(conn, q: str) -> str:
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


def _auto_save_synonym(conn, company_id: str, user_query: str, resolved_name: str):
    query_clean = (user_query or "").strip().lower()
    if len(query_clean) < 3:
        return
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, synonyms FROM pricebook_items WHERE company_id = %s AND lower(name) = lower(%s) LIMIT 1",
            (company_id, resolved_name),
        )
        row = cur.fetchone()
        if not row:
            return
        item_id, existing_synonyms = row
        existing_set = {s.strip().lower() for s in (existing_synonyms or "").split(",") if s.strip()}
        if query_clean in existing_set:
            print(f"AUTO SYNONYM: '{query_clean}' ya existe en '{resolved_name}', skip")
            return
        new_synonyms = ((existing_synonyms or "").rstrip(", ") + f", {query_clean}").lstrip(", ")
        cur.execute(
            "UPDATE pricebook_items SET synonyms = %s, updated_at = now() WHERE id = %s AND company_id = %s",
            (new_synonyms, item_id, company_id),
        )
        print(f"AUTO SYNONYM SAVED: '{query_clean}' → '{resolved_name}'")
    except Exception as e:
        print(f"AUTO SYNONYM ERROR: {repr(e)}")
    finally:
        cur.close()


def _gpt_catalog_fallback(conn, company_id: str, user_query: str,
                           cart_context: str = "") -> list:
    """
    Fallback GPT v3: recibe cart_context para inferir categoría del producto.
    - 1 resultado  → found directo + auto-save sinónimo
    - 2-5 resultados → ambiguous, cliente elige A1/B2
    - lista vacía  → not_found limpio
    """
    if not openai_client:
        print("GPT FALLBACK: openai_client no disponible")
        return []

    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT name, sku, unit, price, vat_rate FROM pricebook_items WHERE company_id = %s ORDER BY name ASC LIMIT 500",
            (company_id,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        return []

    catalog_lines = []
    for i, (name, sku, unit, price, vat_rate) in enumerate(rows):
        sku_txt = f" [SKU:{sku}]" if sku else ""
        unit_txt = f" ({unit})" if unit else ""
        catalog_lines.append(f"{i + 1}. {name}{sku_txt}{unit_txt}")
    catalog_str = "\n".join(catalog_lines)

    context_block = ""
    if cart_context:
        context_block = (
            f"CONTEXTO: Este producto es parte de un pedido que también incluye: {cart_context}. "
            f"Usa ese contexto para inferir la categoría del producto buscado.\n\n"
        )

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Eres asistente de ferretería mexicana con amplio conocimiento de materiales "
                        "de construcción, plomería, electricidad, herrería y acabados. "
                        "El cliente busca un producto usando lenguaje coloquial, abreviado o con errores. "
                        "Usa el contexto del pedido completo para entender a qué categoría pertenece "
                        "el producto buscado — igual que lo haría un ferretero experimentado.\n\n"
                        "Ejemplos:\n"
                        "- Pedido con 'tee principal, tee 61' → 'placas' = plafón reticulado\n"
                        "- Pedido con 'tablaroca, poste, canal' → 'pasta' = redimix/basecoat\n"
                        "- Pedido con 'tubo conduit, clavija' → 'cinta' = cinta aislante\n\n"
                        "Reglas:\n"
                        "- UNA coincidencia clara → responde SOLO ese número (ej: '42')\n"
                        "- VARIAS opciones del MISMO tipo → números por coma (ej: '42,43,44'). Máximo 5.\n"
                        "- Nada relevante → responde exactamente: NO\n"
                        "- NUNCA incluyas productos de tipo diferente al inferido\n"
                        "- NUNCA expliques. Solo números o NO."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"{context_block}"
                        f"Catálogo:\n{catalog_str}\n\n"
                        f"El cliente busca: \"{user_query}\"\n\n"
                        "¿Qué números corresponden? (números separados por coma, o NO)"
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=20,
        )
        raw = (resp.choices[0].message.content or "").strip().upper()
        print(f"GPT FALLBACK RAW: query='{user_query}' context='{cart_context[:50]}' response='{raw}'")

        if raw == "NO" or not raw:
            return []

        parts = [p.strip() for p in raw.split(",") if p.strip()]
        results = []
        for part in parts:
            if not part.isdigit():
                continue
            idx = int(part) - 1
            if idx < 0 or idx >= len(rows):
                print(f"GPT FALLBACK: índice {part} fuera de rango ({len(rows)} items)")
                continue
            name, sku, unit, price, vat_rate = rows[idx]
            results.append({
                "sku": sku, "name": name, "unit": unit,
                "price": float(price) if price is not None else None,
                "vat_rate": float(vat_rate) if vat_rate is not None else None,
            })

        print(f"GPT FALLBACK PARSED: query='{user_query}' items={[r['name'] for r in results]}")
        return results

    except Exception as e:
        print(f"GPT FALLBACK ERROR: query='{user_query}' error={repr(e)}")
        return []


def smart_search(conn, company_id: str, user_query: str, qty: int = 0,
                 cart_context: str = "") -> dict:
    try:
        from rapidfuzz import fuzz
        import re as _re

        q = user_query.lower().strip()
        q = build_query_text(q)

        _stopwords = {"para", "de", "del", "la", "el", "un", "una", "con", "sin", "los", "las"}
        q_tokens = [t for t in q.split() if t not in _stopwords]
        # Si quitar stopwords destruyó el query (ej: "t de 61" → solo "t"), preservar más
        if not q_tokens or (len(q_tokens) == 1 and len(q_tokens[0]) <= 2 and not re.search(r"\d", q_tokens[0])):
            _soft_stopwords = {"para", "del", "la", "el", "un", "una", "con", "sin", "los", "las"}
            q_tokens = [t for t in q.split() if t not in _soft_stopwords]
        q = " ".join(q_tokens).strip() or q

        def _name_search(term):
            c = conn.cursor()
            try:
                c.execute("SELECT count(*) FROM pricebook_items WHERE company_id = %s", (company_id,))
                cnt = c.fetchone()[0]
                print(f">>> DEBUG: company_id='{company_id}' total_rows={cnt}")
                c.execute(
                    "SELECT sku, name, unit, price, vat_rate FROM pricebook_items WHERE company_id = %s AND lower(name) LIKE lower(%s) LIMIT 10",
                    (company_id, f"%{term}%"),
                )
                rows = c.fetchall()
                print(f">>> _name_search('{term}') → {len(rows)} rows: {[r[1] for r in rows]}")
                return rows
            except Exception as e:
                print(f">>> _name_search ERROR: {repr(e)}")
                return []
            finally:
                c.close()

        def _extract_specs(text):
            t = text.lower()
            cal = None
            medida = None
            m_cal = _re.search(r"\bcal(?:ibre)?\s*(\d+)\b", t)
            if m_cal:
                cal = m_cal.group(1)
            m_med = _re.search(r"\b(\d+\.\d+)\b", t)
            if m_med:
                medida = m_med.group(1)
            return medida, cal

        def _spec_bonus(item_name, medida, cal):
            n = item_name.lower()
            bonus = 0
            if medida and medida in n:
                bonus += 30
            if cal and (_re.search(rf"\bcal\s*{cal}\b", n)):
                bonus += 50
            q_tokens = [t for t in q.split() if len(t) >= 4]
            for tok in q_tokens:
                if tok in n:
                    bonus += 15
            return bonus

        def _make_item(r):
            return {
                "sku": r[0], "name": r[1], "unit": r[2],
                "price": float(r[3]) if r[3] is not None else None,
                "vat_rate": float(r[4]) if r[4] is not None else None,
            }

        q_medida, q_cal = _extract_specs(q)

        # ── PASO -1: Sinónimo global ──────────────────────────────────────────
        print(f"SMART SEARCH q='{q}' original='{user_query}' context='{cart_context[:50]}'")
        q_resolved = resolve_global_synonym(conn, q)
        print(f"RESOLVED: q='{q}' → q_resolved='{q_resolved}'")
        if q_resolved != q:
            rows = _name_search(q_resolved)
            if rows:
                scored = [(fuzz.token_set_ratio(q_resolved, (r[1] or "").lower()), r) for r in rows]
                scored.sort(key=lambda x: x[0], reverse=True)
                if scored[0][0] >= 70:
                    print(f"GLOBAL SYNONYM ILIKE HIT: '{q_resolved}' → '{scored[0][1][1]}'")
                    return {"status": "found", "item": _make_item(scored[0][1]), "candidates": []}
            first_token = q_resolved.split()[0] if q_resolved.split() else q_resolved
            if first_token != q_resolved:
                rows_token = _name_search(first_token)
                if rows_token:
                    scored = [(fuzz.token_set_ratio(q_resolved, (r[1] or "").lower()), r) for r in rows_token]
                    scored.sort(key=lambda x: x[0], reverse=True)
                    if scored[0][0] >= 65:
                        print(f"GLOBAL SYNONYM FIRST TOKEN HIT: '{first_token}' → '{scored[0][1][1]}'")
                        return {"status": "found", "item": _make_item(scored[0][1]), "candidates": []}
            print(f"GLOBAL SYNONYM NO HIT: '{q_resolved}' → continuando con q original='{q}'")

        # ── PASO 0: Sinónimo exacto en pricebook ─────────────────────────────
        cur0 = conn.cursor()
        try:
            cur0.execute(
                "SELECT sku, name, unit, price, vat_rate FROM pricebook_items WHERE company_id = %s AND lower(synonyms) LIKE lower(%s) LIMIT 5",
                (company_id, f"%{q}%"),
            )
            syn_rows = cur0.fetchall()
        finally:
            cur0.close()

        if len(syn_rows) == 1:
            name_score = fuzz.token_set_ratio(q, (syn_rows[0][1] or "").lower())
            if name_score >= 60:
                print(f"SYNONYM DIRECT HIT: query='{user_query}' match='{syn_rows[0][1]}'")
                return {"status": "found", "item": _make_item(syn_rows[0]), "candidates": []}
        elif len(syn_rows) > 1:
            name_matches = [r for r in syn_rows if q in (r[1] or "").lower()]
            if not name_matches:
                print(f"SYNONYM AMBIGUOUS: query='{user_query}' found={len(syn_rows)}")
                return {"status": "ambiguous", "item": None, "candidates": [_make_item(r) for r in syn_rows]}

        # ── PASO 1: ILIKE directo + ranking con bonus de specs ────────────────
        pool_rows = _name_search(q)
        if not pool_rows and q.endswith("s") and len(q) > 3:
            pool_rows = _name_search(q[:-1])
        if not pool_rows and q_medida:
            pool_rows = _name_search(q_medida)

        if pool_rows:
            scored = []
            for r in pool_rows:
                name = (r[1] or "").lower()
                base = max(fuzz.token_set_ratio(q, name), fuzz.partial_ratio(q, name))
                bonus = _spec_bonus(r[1], q_medida, q_cal)
                scored.append((base + bonus, r))
            scored.sort(key=lambda x: x[0], reverse=True)
            top = scored[0][0]
            second = scored[1][0] if len(scored) > 1 else 0
            gap = top - second
            print(f"ILIKE SCORED: {[(s, r[1]) for s, r in scored[:3]]}")
            min_score = 80 if (q_medida or q_cal) else 85
            min_gap = 15 if (q_medida or q_cal) else 8
            if top >= min_score and gap >= min_gap:
                r = scored[0][1]
                print(f"ILIKE RESOLVED: query='{user_query}' match='{r[1]}'")
                return {"status": "found", "item": _make_item(r), "candidates": []}
            q_first_token = q.split()[0] if q.split() else q
            filtered = [(s, r) for s, r in scored[:5] if (r[1] or "").lower().startswith(q_first_token)]
            candidates = filtered if filtered else scored[:5]
            return {"status": "ambiguous", "item": None, "candidates": [_make_item(r) for _, r in candidates]}

        # ── PASO 2: tsvector + fuzzy sobre candidatos ─────────────────────────
        _tokens = [t for t in q.split() if len(t) >= 3]
        _tsquery = " | ".join(_tokens) if _tokens else q

        cur2 = conn.cursor()
        try:
            cur2.execute(
                """
                SELECT sku, name, unit, price, vat_rate, synonyms
                FROM pricebook_items
                WHERE company_id = %s
                  AND (
                      search_vector @@ to_tsquery('spanish', %s)
                      OR lower(name) LIKE lower(%s)
                      OR lower(synonyms) LIKE lower(%s)
                  )
                LIMIT 30
                """,
                (company_id, _tsquery, f"%{q}%", f"%{q}%"),
            )
            rows = cur2.fetchall()
        except Exception as e:
            print(f"TSVECTOR FALLBACK: {repr(e)}")
            cur2_b = conn.cursor()
            try:
                cur2_b.execute(
                    "SELECT sku, name, unit, price, vat_rate, synonyms FROM pricebook_items WHERE company_id = %s AND (lower(name) LIKE lower(%s) OR lower(synonyms) LIKE lower(%s)) LIMIT 30",
                    (company_id, f"%{q}%", f"%{q}%"),
                )
                rows = cur2_b.fetchall()
            finally:
                cur2_b.close()
        finally:
            cur2.close()

        scored = []
        for r in rows:
            name = (r[1] or "").lower()
            syns = [s.strip().lower() for s in (r[5] or "").split(",") if s.strip()]
            all_terms = [name] + syns
            base = max(max(fuzz.token_set_ratio(q, t), fuzz.partial_ratio(q, t)) for t in all_terms)
            name_score = max(fuzz.token_set_ratio(q, name), fuzz.partial_ratio(q, name))
            if name_score < base - 20:
                base = name_score
            bonus = _spec_bonus(r[1], q_medida, q_cal)
            total = base + bonus
            if total >= 80:
                scored.append((total, {
                    "sku": r[0], "name": r[1], "unit": r[2],
                    "price": float(r[3]) if r[3] is not None else None,
                    "vat_rate": float(r[4]) if r[4] is not None else None,
                }))

        scored.sort(key=lambda x: x[0], reverse=True)

        if len(scored) == 1:
            if scored[0][0] >= 92:
                print(f"FUZZY UNIQUE: query='{user_query}' match='{scored[0][1]['name']}' score={scored[0][0]}")
                return {"status": "found", "item": scored[0][1], "candidates": []}
            else:
                print(f"FUZZY UNIQUE LOW SCORE: query='{user_query}' match='{scored[0][1]['name']}' score={scored[0][0]} → semántico")

        if len(scored) > 1:
            top_score = scored[0][0]
            second_score = scored[1][0]
            gap = top_score - second_score
            min_score = 85 if (q_medida or q_cal) else 95
            if top_score >= min_score and gap >= 10:
                print(f"FUZZY CLEAR WIN: query='{user_query}' match='{scored[0][1]['name']}' score={top_score}")
                return {"status": "found", "item": scored[0][1], "candidates": []}
            print(f"FUZZY AMBIGUOUS: query='{user_query}' found={len(scored)}")
            q_first_token = q.split()[0] if q.split() else q
            filtered = [(s, item) for s, item in scored[:5] if (item.get("name") or "").lower().startswith(q_first_token)]
            candidates = filtered if filtered else scored[:5]
            return {"status": "ambiguous", "item": None, "candidates": [item for _, item in candidates]}

        # ── PASO 3: Semántico ─────────────────────────────────────────────────
        words = user_query.strip().split()
        cand_threshold = 0.55 if len(words) == 1 else 0.60 if len(words) == 2 else 0.65

        candidates = semantic_search_candidates(conn, company_id, user_query,
                                                threshold=cand_threshold, limit=5)
        if candidates and candidates[0].get("similarity", 0) >= 0.60:
            print(f"SEMANTIC CANDIDATES: query='{user_query}' found={len(candidates)}")
            return {"status": "ambiguous", "item": None, "candidates": candidates}

        candidates_low = semantic_search_candidates(conn, company_id, user_query,
                                                    threshold=0.50, limit=3)
        if candidates_low:
            print(f"SEMANTIC LOW THRESHOLD: query='{user_query}' found={len(candidates_low)}")
            return {"status": "ambiguous", "item": None, "candidates": candidates_low}

        # ── PASO 4: Fallback GPT con catálogo completo + contexto del pedido ──
        gpt_results = _gpt_catalog_fallback(conn, company_id, user_query,
                                             cart_context=cart_context)

        if len(gpt_results) == 1:
            print(f"GPT FALLBACK FOUND: query='{user_query}' → '{gpt_results[0]['name']}'")
            try:
                _auto_save_synonym(conn, company_id, user_query, gpt_results[0]["name"])
            except Exception as e:
                print(f"AUTO SYNONYM SAVE ERROR: {repr(e)}")
            return {"status": "found", "item": gpt_results[0], "candidates": []}

        if len(gpt_results) > 1:
            print(f"GPT FALLBACK AMBIGUOUS: query='{user_query}' options={[r['name'] for r in gpt_results]}")
            return {"status": "ambiguous", "item": None, "candidates": gpt_results}

        print(f"GPT FALLBACK NO: query='{user_query}' → not_found")
        return {"status": "not_found", "item": None, "candidates": []}

    except Exception as e:
        print(f"SMART SEARCH EXCEPTION: query='{user_query}' error={repr(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "not_found", "item": None, "candidates": []}
