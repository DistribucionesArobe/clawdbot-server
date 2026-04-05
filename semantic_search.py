"""
semantic_search.py — Búsqueda semántica para CotizaBot v9
Novedades vs v8:
  - PASO 0.5: Normalización LLM de jerga (llm_normalize_query)
  - Diccionario de jerga global y local con caché en DB
  - tenant_context por empresa para mejorar normalización
"""

import re
import os
import unicodedata
from typing import Optional
from openai import OpenAI

def _strip_accents(s: str) -> str:
    """Remove accents: 'listón' → 'liston'"""
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


# ── Phonetic normalization for Mexican Spanish typos ──────────────
# b↔v and s↔z sound identical in Mexican Spanish, causing constant
# misspellings: abrasadera/abrazadera, barilla/varilla, tencion/tension
_SQL_TRANSLATE_FROM = "áéíóúÁÉÍÓÚñÑbBzZ"
_SQL_TRANSLATE_TO   = "aeiouAEIOUnNvVsS"

def _phonetic(s: str) -> str:
    """Accent-strip + phonetic normalize: 'abrasadera' → 'avrasadera'"""
    out = _strip_accents(s)
    return out.replace("b", "v").replace("B", "V").replace("z", "s").replace("Z", "S")


def _sql_translate(col: str) -> str:
    """Return SQL translate() expression for accent+phonetic normalization."""
    return f"translate(COALESCE({col},''), '{_SQL_TRANSLATE_FROM}', '{_SQL_TRANSLATE_TO}')"


def _singulars_es(word: str):
    """Return possible singular forms of a Spanish word.
    rollos→rollo, tubos→tubo, abrazaderas→abrazadera,
    conectores→conector, soleras→solera.
    Returns list of candidates (may include original)."""
    w = word.lower().strip()
    forms = []
    if w.endswith("es") and len(w) > 4:
        base = w[:-2]       # conectores → conector
        base_s = w[:-1]     # soleras   → solera  (vowel+s case)
        if base and base[-1] not in "aeiou":
            forms.append(base)       # consonant+es → remove "es"
        forms.append(base_s)         # always try just removing "s"
    elif w.endswith("s") and len(w) > 3:
        forms.append(w[:-1])         # rollos → rollo
    return forms


OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

EMBED_MODEL = "text-embedding-3-large"

# Packaging/unit words to skip when selecting the primary token for matching
_PACKAGING_WORDS = {"rollos", "rollo", "sacos", "saco", "bultos", "bulto",
                    "piezas", "pieza", "hojas", "hoja", "tiras", "tira",
                    "metros", "metro", "kilos", "kilo", "cajas", "caja",
                    "cubetas", "cubeta", "botes", "bote", "bolsas", "bolsa",
                    "paquetes", "paquete", "costales", "costal", "atados", "atado"}


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
    """
    Limpia la query del usuario para búsqueda en catálogo.
    Estrategia: solo quitar la unidad de empaque cuando va pegada a la cantidad
    inicial (ej "10 sacos cemento" → "cemento"). Preservar todo lo demás
    (specs de medida, unidades dentro del nombre del producto, etc.).
    """
    t = (user_input or "").lower().strip()
    # Normalizar fracciones: "1 / 2" → "1/2"
    t = re.sub(r"(\d+)\s*/\s*(\d+)", r"\1/\2", t)
    # Quitar palabras de intención (cotiza, dame, quiero, etc.)
    noise_intent = r"\b(cotiza|cotizame|dame|quiero|necesito|por favor|porfa|pls|precio|precios)\b"
    t = re.sub(noise_intent, " ", t)

    # Solo quitar la cantidad inicial + su unidad de empaque directa.
    # Ej: "10 sacos cemento" → "cemento", "5 bolsas arena" → "arena"
    # Pero NO tocar: "tubo de 2 metros", "rejacero 2.50 m", "clavo de 3 pulgadas"
    _packaging_units = (
        r"(?:cubetas?|bultos?|bolsas?|rollos?|piezas?|kilos?|kilogramos?|kg|"
        r"litros?|lts?|pares?|juegos?|cajas?|sacos?|botes?|latas?|tiras?|hojas?|"
        r"paquetes?|cientos?|millares?|costales?)"
    )
    # Quitar "CANTIDAD [de] UNIDAD_EMPAQUE [de]" al inicio
    t = re.sub(rf"^\s*\d+\s+(?:de\s+)?{_packaging_units}\s+(?:de\s+)?", "", t)
    # Si solo era "CANTIDAD PRODUCTO" sin unidad de empaque, quitar la cantidad inicial
    t = re.sub(r"^\s*\d+\s+", "", t)

    # Normalizar "de" suelto al inicio (residual)
    t = re.sub(r"^\s*de\s+", "", t)

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
        dimensions=1536,
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
            dimensions=1536,
            encoding_format="float",
        )
        resp.data.sort(key=lambda x: x.index)
        all_vectors.extend([d.embedding for d in resp.data])
    return all_vectors


def auto_generate_context_groups(conn, company_id: str) -> dict:
    """Auto-generate product context groups using LLM clustering.
    Groups products that are typically bought together (e.g. tablaroca vs rejacero).
    Stores result in companies.context_groups JSONB field."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT name FROM pricebook_items WHERE company_id = %s AND name IS NOT NULL ORDER BY name",
            (company_id,),
        )
        products = [r[0] for r in cur.fetchall()]
        if len(products) < 5:
            return {"status": "skip", "reason": "too few products", "count": len(products)}

        product_list = "\n".join(f"- {p}" for p in products[:500])  # cap at 500

        prompt = f"""Eres un experto en ferreterías y tiendas de materiales de construcción en México.

Analiza estos productos y agrúpalos en CATEGORÍAS DE PROYECTO — es decir, productos que un cliente típicamente compra JUNTOS en un mismo pedido porque son del mismo tipo de obra/proyecto.

Productos del catálogo:
{product_list}

IMPORTANTE: El objetivo es que si un cliente está comprando productos del grupo "tablaroca" (como canal, plafón, poste), el sistema entienda que productos del grupo "rejacero" (como malla ciclónica, concertina) NO son relevantes, y viceversa.

Responde SOLO con JSON válido, sin explicaciones. Formato:
{{"nombre_grupo": ["keyword1", "keyword2", ...], ...}}

Reglas ESTRICTAS:
1. Los keywords deben ser palabras DISTINTIVAS que aparecen en los nombres de los productos — NO uses palabras genéricas como "sin", "con", "simple", "tipo", "para", "grande", "chico"
2. Cada keyword debe tener mínimo 4 letras
3. Incluye las palabras clave del nombre del producto: si un producto se llama "Poste para tablaroca", los keywords son "poste" y "tablaroca"
4. Grupos típicos en ferreterías mexicanas: tablaroca (canal, poste, plafon, angulo, durock, pija), rejacero (malla, ciclonica, concertina, poste, espada, tension), plomeria (tubo, codo, valvula, llave, pvc, cpvc), pintura (pintura, esmalte, brocha, rodillo, thinner), electricidad (cable, contacto, apagador, caja), herrajes/puertas (cerradura, bisagra, marco, puerta, chapa), laminas (lamina, acanalada, aceroteja, caballete), techos/impermeabilizacion (impermeabilizante, sellador, membrana), tornilleria (pija, tornillo, taquete, clavo, remache), herramienta (martillo, desarmador, pinza, llave)
5. Un keyword puede estar en MÁXIMO 2 grupos si genuinamente es ambiguo (ej: "poste" en tablaroca Y rejacero)
6. Mínimo 5 keywords por grupo, máximo 20
7. Máximo 15 grupos
8. Todos los keywords en minúsculas, sin acentos"""

        client = OpenAI()
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un experto en materiales de construcción y ferretería en México. Conoces perfectamente qué productos se compran juntos en cada tipo de obra. Respondes solo con JSON válido."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=2000,
        )
        import json
        raw = (resp.choices[0].message.content or "").strip()
        # Extract JSON from possible markdown code block
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        groups = json.loads(raw)

        # Validate structure
        if not isinstance(groups, dict):
            return {"status": "error", "reason": "LLM returned non-dict"}
        # Normalize: ensure all keywords are lowercase strings, filter junk
        _generic_words = {"para", "tipo", "con", "sin", "grande", "chico", "simple",
                          "doble", "triple", "nuevo", "viejo", "bueno", "malo",
                          "pieza", "piezas", "metro", "metros", "rollo", "rollos",
                          "caja", "cajas", "bulto", "bultos"}
        clean_groups = {}
        for name, keywords in groups.items():
            if isinstance(keywords, list):
                cleaned_kws = []
                for k in keywords:
                    kw = _strip_accents(str(k).lower().strip())
                    # Filter: min 4 chars, not generic, not purely numeric
                    if kw and len(kw) >= 4 and kw not in _generic_words and not kw.replace(".", "").isdigit():
                        cleaned_kws.append(kw)
                if len(cleaned_kws) >= 3:  # only keep groups with enough keywords
                    clean_groups[_strip_accents(name.lower())] = cleaned_kws
        if not clean_groups:
            return {"status": "error", "reason": "no valid groups extracted"}

        # Save to DB
        cur.execute(
            "UPDATE companies SET context_groups = %s WHERE id = %s",
            (json.dumps(clean_groups, ensure_ascii=False), company_id),
        )
        conn.commit()
        print(f"CONTEXT GROUPS GENERATED: company={company_id} groups={list(clean_groups.keys())} total_keywords={sum(len(v) for v in clean_groups.values())}")
        return {"status": "ok", "groups": clean_groups}

    except Exception as e:
        print(f"CONTEXT GROUPS ERROR: {repr(e)}")
        conn.rollback()
        return {"status": "error", "reason": str(e)}
    finally:
        cur.close()


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
                cur.execute("SAVEPOINT embed_sp")
                cur.execute(
                    "UPDATE pricebook_items SET embedding = %s::vector WHERE id = %s AND company_id = %s",
                    (vector_str, item_id, company_id),
                )
                cur.execute("RELEASE SAVEPOINT embed_sp")
                updated += 1
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT embed_sp")
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
            SELECT sku, name, unit, price, vat_rate, bundle_size,
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
            "bundle_size": r[5],
            "similarity": float(r[6]),
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
            SELECT sku, name, unit, price, vat_rate, bundle_size,
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
            "bundle_size": r[5],
            "similarity": float(r[6]),
        }
        for r in rows
    ]
    print(f"SEMANTIC CANDIDATES: query='{user_query}' found={len(candidates)}")
    print(f"SEMANTIC CANDS DETAIL: {[(c['name'], round(c['similarity'],3)) for c in candidates]}")
    return candidates


# ─── HYBRID SEARCH v2: keyword + vector + RRF + LLM reranker ────────────────

def _vector_candidates(conn, company_id: str, query_text: str, limit: int = 20) -> list:
    """Get top-N candidates from pgvector cosine similarity."""
    try:
        query_vector = get_embedding(query_text)
    except Exception as e:
        print(f"VECTOR CANDIDATES ERROR (embedding): {repr(e)}")
        return []
    vector_str = "[" + ",".join(str(x) for x in query_vector) + "]"
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT sku, name, unit, price, vat_rate, bundle_size,
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
    results = []
    for r in rows:
        results.append({
            "sku": r[0], "name": r[1], "unit": r[2],
            "price": float(r[3]) if r[3] is not None else None,
            "vat_rate": float(r[4]) if r[4] is not None else None,
            "bundle_size": r[5],
            "similarity": float(r[6]),
        })
    return results


def _keyword_candidates(conn, company_id: str, query_phonetic: str,
                         q_tokens: list, limit: int = 20) -> list:
    """Get top-N candidates from ILIKE multi-token search."""
    from rapidfuzz import fuzz
    _NS_SQL = f"""SELECT sku, name, unit, price, vat_rate, bundle_size FROM pricebook_items
                  WHERE company_id = %s
                    AND lower({_sql_translate('name')}) LIKE lower(%s)
                  LIMIT 10"""
    seen = set()
    merged = []
    cur = conn.cursor()
    try:
        for tok in q_tokens:
            if len(tok) < 3 or tok.replace(".", "").isdigit():
                continue
            tok_phon = _phonetic(tok)
            cur.execute(_NS_SQL, (company_id, f"%{tok_phon}%"))
            for r in cur.fetchall():
                key = (r[0] or "", r[1] or "")
                if key not in seen:
                    seen.add(key)
                    merged.append(r)
            # Also try singular
            for sg in _singulars_es(tok_phon):
                cur.execute(_NS_SQL, (company_id, f"%{sg}%"))
                for r in cur.fetchall():
                    key = (r[0] or "", r[1] or "")
                    if key not in seen:
                        seen.add(key)
                        merged.append(r)
    finally:
        cur.close()
    # Score with fuzzy + build dicts
    results = []
    for r in merged:
        name_phon = _phonetic((r[1] or "").lower())
        score = max(fuzz.token_set_ratio(query_phonetic, name_phon),
                    fuzz.partial_ratio(query_phonetic, name_phon))
        results.append({
            "sku": r[0], "name": r[1], "unit": r[2],
            "price": float(r[3]) if r[3] is not None else None,
            "vat_rate": float(r[4]) if r[4] is not None else None,
            "keyword_score": score / 100.0,
        })
    results.sort(key=lambda x: x["keyword_score"], reverse=True)
    return results[:limit]


def _rrf_merge(keyword_results: list, vector_results: list, k: int = 60) -> list:
    """Reciprocal Rank Fusion: combine keyword + vector rankings."""
    scores = {}  # name → {rrf_score, item}
    for rank, item in enumerate(keyword_results):
        name = item["name"]
        scores[name] = {
            "rrf": 1.0 / (k + rank + 1),
            "item": item,
            "kw_rank": rank + 1,
            "vec_rank": None,
            "vec_sim": 0,
        }
    for rank, item in enumerate(vector_results):
        name = item["name"]
        vec_score = 1.0 / (k + rank + 1)
        if name in scores:
            scores[name]["rrf"] += vec_score
            scores[name]["vec_rank"] = rank + 1
            scores[name]["vec_sim"] = item.get("similarity", 0)
        else:
            scores[name] = {
                "rrf": vec_score,
                "item": item,
                "kw_rank": None,
                "vec_rank": rank + 1,
                "vec_sim": item.get("similarity", 0),
            }
    # Sort by RRF score descending
    ranked = sorted(scores.values(), key=lambda x: x["rrf"], reverse=True)
    return ranked


def _llm_rerank(conn, company_id: str, user_query: str, candidates: list,
                cart_context: str = "", max_candidates: int = 10) -> dict:
    """
    LLM reranker: send top candidates to GPT-4o-mini to pick the best match.
    Returns {"status": "found"/"ambiguous"/"not_found", "item": ..., "candidates": [...]}
    """
    if not openai_client or not candidates:
        return {"status": "not_found", "item": None, "candidates": []}

    top_n = candidates[:max_candidates]
    catalog_lines = []
    for i, c in enumerate(top_n):
        item = c["item"]
        name = item.get("name", "")
        unit = item.get("unit", "")
        unit_txt = f" ({unit})" if unit else ""
        kw = f"kw#{c['kw_rank']}" if c.get("kw_rank") else ""
        vec = f"vec#{c['vec_rank']}:{c['vec_sim']:.2f}" if c.get("vec_rank") else ""
        catalog_lines.append(f"{i + 1}. {name}{unit_txt} [{kw} {vec}]")
    catalog_str = "\n".join(catalog_lines)

    context_block = ""
    if cart_context:
        context_block = (
            f"CONTEXTO DEL PEDIDO: {cart_context}\n"
            f"Usa este contexto para inferir la categoría del producto buscado.\n\n"
        )

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Eres un ferretero mexicano experto. El cliente busca un producto. "
                        "Te doy los mejores candidatos del catálogo ordenados por relevancia.\n\n"
                        "REGLAS:\n"
                        "- Si HAY una coincidencia clara → responde SOLO ese número (ej: '3')\n"
                        "- Si hay 2-3 opciones del MISMO producto en diferentes medidas → números por coma (ej: '3,5,7')\n"
                        "- Si NINGUNO corresponde → responde: NO\n"
                        "- NUNCA expliques. Solo número(s) o NO.\n\n"
                        "IMPORTANTE: Conoces la jerga mexicana de construcción:\n"
                        "- 'pasta para durock/tablaroca' = basecoat/compuesto para juntas\n"
                        "- 'malla para durock/tablaroca' = cinta de fibra de vidrio\n"
                        "- 'tanque' puede ser 'tinaco'\n"
                        "- 'RH'/'HR' = resistente a humedad = anti-moho\n"
                        "- Usa el contexto del pedido para desambiguar"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"{context_block}"
                        f"Candidatos:\n{catalog_str}\n\n"
                        f"El cliente busca: \"{user_query}\"\n"
                        "¿Cuál número? (número, números por coma, o NO)"
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=20,
        )
        raw = (resp.choices[0].message.content or "").strip().upper()
        print(f"LLM RERANK: query='{user_query}' response='{raw}' candidates={[c['item']['name'] for c in top_n[:5]]}")

        if raw == "NO" or not raw:
            return {"status": "not_found", "item": None, "candidates": []}

        parts = [p.strip() for p in raw.split(",") if p.strip().isdigit()]
        picked = []
        for p in parts:
            idx = int(p) - 1
            if 0 <= idx < len(top_n):
                picked.append(top_n[idx]["item"])

        if len(picked) == 1:
            return {"status": "found", "item": picked[0], "candidates": []}
        elif picked:
            return {"status": "ambiguous", "item": None, "candidates": picked}
        else:
            return {"status": "not_found", "item": None, "candidates": []}

    except Exception as e:
        print(f"LLM RERANK ERROR: {repr(e)}")
        return {"status": "not_found", "item": None, "candidates": []}


def _auto_learn_synonym(conn, company_id: str, query_raw: str, product_name: str):
    """Auto-save successful match to jerga_local for instant future lookups."""
    try:
        q = _phonetic(query_raw.lower().strip())
        if not q or len(q) < 3:
            return
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO diccionario_jerga_local (company_id, termino_original, termino_normalizado, source, usage_count)
                   VALUES (%s, %s, %s, 'auto_learn', 1)
                   ON CONFLICT (company_id, termino_original)
                   DO UPDATE SET usage_count = diccionario_jerga_local.usage_count + 1,
                                 termino_normalizado = EXCLUDED.termino_normalizado""",
                (company_id, q, product_name),
            )
            print(f"AUTO-LEARN: '{q}' → '{product_name}' (tenant={company_id[:8]})")
        finally:
            cur.close()
    except Exception as e:
        print(f"AUTO-LEARN ERROR: {repr(e)}")


def hybrid_search(conn, company_id: str, user_query: str, q_normalized: str,
                   q_tokens: list, q_medida: str = None, q_cal: str = None,
                   cart_context: str = "") -> dict:
    """
    Hybrid search v2: keyword + vector + RRF + LLM reranker.
    Returns {"status": "found"/"ambiguous"/"not_found", "item": ..., "candidates": [...]}
    """
    import re as _re
    from rapidfuzz import fuzz

    q_phonetic = _phonetic(q_normalized)

    # ── Step 1: Get candidates from both sources in parallel-ish ──────────
    kw_cands = _keyword_candidates(conn, company_id, q_phonetic, q_tokens, limit=20)
    vec_cands = _vector_candidates(conn, company_id, q_normalized, limit=20)

    print(f"HYBRID: keyword={len(kw_cands)} vector={len(vec_cands)} query='{user_query}'")
    if vec_cands:
        print(f"HYBRID VEC TOP-3: {[(c['name'], round(c['similarity'],3)) for c in vec_cands[:3]]}")
    if kw_cands:
        print(f"HYBRID KW TOP-3: {[(c['name'], round(c['keyword_score'],3)) for c in kw_cands[:3]]}")

    # ── Step 2: RRF merge ─────────────────────────────────────────────────
    merged = _rrf_merge(kw_cands, vec_cands)
    if not merged:
        return {"status": "not_found", "item": None, "candidates": []}

    # ── Step 3: Apply spec bonus to RRF scores ────────────────────────────
    for entry in merged:
        name = (entry["item"].get("name") or "").lower()
        bonus = 0
        if q_medida:
            if " " in q_medida or "/" in q_medida:
                if q_medida in name:
                    bonus += 0.01
                else:
                    bonus -= 0.005
            else:
                if _re.search(rf"\b{_re.escape(q_medida)}\b", name):
                    bonus += 0.01
                else:
                    bonus -= 0.005
        if q_cal and _re.search(rf"\bcal\s*{q_cal}\b", name):
            bonus += 0.015
        entry["rrf"] += bonus

    merged.sort(key=lambda x: x["rrf"], reverse=True)
    top = merged[0]
    second = merged[1] if len(merged) > 1 else None

    print(f"HYBRID MERGED TOP-3: {[(e['item']['name'], round(e['rrf'],4)) for e in merged[:3]]}")

    # ── Step 4: Auto-resolve if clear winner ──────────────────────────────
    # High vector similarity + clear gap = obvious match
    vec_sim = top.get("vec_sim", 0)
    rrf_gap = (top["rrf"] - second["rrf"]) if second else 999

    # Obvious: vector similarity > 0.85 and big RRF gap
    if vec_sim >= 0.85 and rrf_gap >= 0.005:
        print(f"HYBRID RESOLVED (high vec): query='{user_query}' match='{top['item']['name']}' vec={vec_sim:.3f} gap={rrf_gap:.4f}")
        return {"status": "found", "item": top["item"], "candidates": []}

    # Obvious: both keyword and vector agree on #1, decent scores
    if top.get("kw_rank") and top.get("vec_rank"):
        if top["kw_rank"] <= 2 and top["vec_rank"] <= 2 and vec_sim >= 0.70 and rrf_gap >= 0.003:
            print(f"HYBRID RESOLVED (kw+vec agree): query='{user_query}' match='{top['item']['name']}' kw#{top['kw_rank']} vec#{top['vec_rank']} sim={vec_sim:.3f}")
            return {"status": "found", "item": top["item"], "candidates": []}

    # Obvious: only one candidate has both keyword AND vector match
    dual_match = [e for e in merged[:5] if e.get("kw_rank") and e.get("vec_rank")]
    if len(dual_match) == 1 and dual_match[0]["vec_sim"] >= 0.60:
        print(f"HYBRID RESOLVED (only dual): query='{user_query}' match='{dual_match[0]['item']['name']}'")
        return {"status": "found", "item": dual_match[0]["item"], "candidates": []}

    # ── Step 5: LLM reranker for ambiguous cases ──────────────────────────
    print(f"HYBRID → LLM RERANK: query='{user_query}' top_rrf={top['rrf']:.4f} gap={rrf_gap:.4f}")
    result = _llm_rerank(conn, company_id, user_query, merged[:10], cart_context)

    # ── Step 6: Auto-learn from successful LLM picks ──────────────────────
    if result["status"] == "found" and result.get("item"):
        _auto_learn_synonym(conn, company_id, user_query, result["item"]["name"])

    return result


def fuzzy_search_best(conn, company_id: str, user_query: str, threshold: int = 95) -> Optional[dict]:
    from rapidfuzz import fuzz
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT sku, name, unit, price, vat_rate, synonyms, bundle_size FROM pricebook_items WHERE company_id = %s AND embedding IS NOT NULL",
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


def _run_migrations(conn):
    """
    Ejecuta migraciones necesarias de forma idempotente.
    Se llama una vez al inicio del servidor.
    """
    cur = conn.cursor()
    try:
        # 1. Agregar columna is_protected si no existe
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'diccionario_jerga_global'
                    AND column_name = 'is_protected'
                ) THEN
                    ALTER TABLE diccionario_jerga_global
                    ADD COLUMN is_protected BOOLEAN NOT NULL DEFAULT FALSE;
                END IF;
            END $$;
        """)
        print("MIGRATION: is_protected column ensured on diccionario_jerga_global")

        # 2. Nuevas columnas para jerga escalable: tracking de uso y confianza
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'diccionario_jerga_global'
                    AND column_name = 'usage_count'
                ) THEN
                    ALTER TABLE diccionario_jerga_global
                    ADD COLUMN usage_count INTEGER NOT NULL DEFAULT 0,
                    ADD COLUMN success_count INTEGER NOT NULL DEFAULT 0,
                    ADD COLUMN industry VARCHAR(100) DEFAULT NULL,
                    ADD COLUMN source VARCHAR(50) NOT NULL DEFAULT 'seed';
                END IF;
            END $$;
        """)
        print("MIGRATION: usage_count, success_count, industry, source columns ensured on diccionario_jerga_global")

        # 3. Crear tabla query_events — log de cada búsqueda para aprendizaje
        cur.execute("""
            CREATE TABLE IF NOT EXISTS query_events (
                id BIGSERIAL PRIMARY KEY,
                company_id UUID NOT NULL,
                original_text VARCHAR(500) NOT NULL,
                cleaned_text VARCHAR(500),
                normalized_text VARCHAR(500),
                normalization_source VARCHAR(50),
                matched_item_name VARCHAR(500),
                matched_item_sku VARCHAR(100),
                search_status VARCHAR(30) NOT NULL,
                search_paso VARCHAR(50),
                confidence_score REAL,
                was_correct BOOLEAN DEFAULT NULL,
                industry VARCHAR(100),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        print("MIGRATION: query_events table ensured")

        # 4. Índices para query_events
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_query_events_company
            ON query_events (company_id, created_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_query_events_status
            ON query_events (search_status, created_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_query_events_normalized
            ON query_events (normalized_text, search_status);
        """)
        print("MIGRATION: query_events indexes ensured")

    except Exception as e:
        print(f"MIGRATION ERROR: {repr(e)}")
    finally:
        cur.close()


def _log_query_event(conn, company_id: str, original_text: str, cleaned_text: str,
                     normalized_text: str, normalization_source: str,
                     matched_item_name: str, matched_item_sku: str,
                     search_status: str, search_paso: str,
                     confidence_score: float = None, industry: str = None):
    """
    Registra un evento de búsqueda en query_events.
    Se llama al final de smart_search() con el resultado.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO query_events "
            "(company_id, original_text, cleaned_text, normalized_text, "
            "normalization_source, matched_item_name, matched_item_sku, "
            "search_status, search_paso, confidence_score, industry) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (company_id, original_text[:500], (cleaned_text or "")[:500],
             (normalized_text or "")[:500], normalization_source,
             matched_item_name, matched_item_sku,
             search_status, search_paso, confidence_score, industry),
        )
        cur.close()
    except Exception as e:
        print(f"LOG QUERY EVENT ERROR: {repr(e)}")


def _increment_jerga_usage(conn, termino_original: str, success: bool = False):
    """
    Incrementa usage_count (y success_count si aplica) en diccionario_jerga_global.
    """
    try:
        cur = conn.cursor()
        if success:
            cur.execute(
                "UPDATE diccionario_jerga_global "
                "SET usage_count = usage_count + 1, success_count = success_count + 1 "
                "WHERE termino_original = %s",
                (termino_original.lower().strip(),),
            )
        else:
            cur.execute(
                "UPDATE diccionario_jerga_global "
                "SET usage_count = usage_count + 1 "
                "WHERE termino_original = %s",
                (termino_original.lower().strip(),),
            )
        cur.close()
    except Exception as e:
        print(f"INCREMENT JERGA USAGE ERROR: {repr(e)}")


def _check_auto_promote(conn, termino_original: str):
    """
    Auto-promueve un término de jerga a protegido si:
    - usage_count >= 5
    - confidence (success_count / usage_count) >= 0.8
    - No es ya protegido
    """
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT usage_count, success_count, is_protected "
            "FROM diccionario_jerga_global WHERE termino_original = %s LIMIT 1",
            (termino_original.lower().strip(),),
        )
        row = cur.fetchone()
        if row and not row[2]:  # not already protected
            usage, success, _ = row
            if usage >= 5 and success / max(usage, 1) >= 0.8:
                cur.execute(
                    "UPDATE diccionario_jerga_global SET is_protected = TRUE "
                    "WHERE termino_original = %s AND NOT is_protected",
                    (termino_original.lower().strip(),),
                )
                confidence = success / max(usage, 1)
                print(f"AUTO-PROMOTE: '{termino_original}' → protected (usage={usage}, success={success}, confidence={confidence:.2f})")
        cur.close()
    except Exception as e:
        print(f"AUTO-PROMOTE ERROR: {repr(e)}")


def seed_jerga_global(conn):
    """
    Inserta/actualiza términos críticos en diccionario_jerga_global con is_protected=TRUE.
    Se llama una vez al inicio del servidor.
    Entradas protegidas no pueden ser sobreescritas por el LLM.
    Los datos semilla vienen de la DB — esto es solo bootstrap inicial.
    """
    _run_migrations(conn)

    # Bootstrap: entradas protegidas iniciales.
    # En el futuro estas se administran desde el panel admin, no desde código.
    _SEED_ENTRIES = [
        ("framer", "framer"),
        ("pija framer", "pija framer"),
        ("pija durock", "pija durock"),
        ("pija para durock", "pija para durock"),
        ("pija para tablaroca", "pija para tablaroca"),
        ("pija pada tablaroca", "pija para tablaroca"),
        ("durock", "durock"),
        ("basecoat", "basecoat"),
        ("redimix", "redimix"),
        ("tablaroca rh", "tablaroca anti-moho"),
        ("tr rh", "tablaroca anti-moho"),
        ("tablaroca hr", "tablaroca anti-moho"),
        ("tr hr", "tablaroca anti-moho"),
        ("tablaroca anti moho", "tablaroca anti-moho"),
        ("tornillos pa taquete", "pija para taquete"),
        ("tornillo para taquete", "pija para taquete"),
        ("tornillo pa taquete", "pija para taquete"),
        ("tornillos para taquete", "pija para taquete"),
        # Tablaroca / hojas
        ("hojas", "tablaroca"),
        ("hoja", "tablaroca"),
        ("hojas tablaroca", "tablaroca"),
        ("hojas de tablaroca", "tablaroca"),
        ("laminas tablaroca", "tablaroca"),
        # Canaletas / cargadoras
        ("cargadoras", "canaleta de carga"),
        ("cargadora", "canaleta de carga"),
        ("canaleta carga", "canaleta de carga"),
        ("canaletas de carga", "canaleta de carga"),
        ("canaletas carga", "canaleta de carga"),
        # Rejas / rejacero
        ("reja", "rejacero"),
        ("rejas", "rejacero"),
        ("reja blanca", "rejacero blanca"),
        ("rejas blancas", "rejacero blanca"),
        ("reja negra", "rejacero negra"),
        ("rejas negras", "rejacero negra"),
        # Pasta / compuesto para juntas
        ("pasta para durock", "basecoat"),
        ("pasta durock", "basecoat"),
        ("pasta para tablaroca", "basecoat"),
        ("pasta tablaroca", "basecoat"),
        ("pasta para juntas", "basecoat"),
        ("compuesto para juntas", "basecoat"),
        # Malla / cinta para juntas
        ("malla para durock", "cinta fibra de vidrio"),
        ("malla durock", "cinta fibra de vidrio"),
        ("malla para tablaroca", "cinta fibra de vidrio"),
        ("malla tablaroca", "cinta fibra de vidrio"),
        ("cinta para juntas", "cinta fibra de vidrio"),
        ("cinta para durock", "cinta fibra de vidrio"),
        ("cinta para tablaroca", "cinta fibra de vidrio"),
        # Concertina shorthand
        ("rollos concertina", "concertina"),
        ("rollo concertina", "concertina"),
        # Pilas = pijas (very common Mexican construction slang)
        ("pilas", "pija"),
        ("pila", "pija"),
        ("pilas framer", "pija framer"),
        ("pilas para durock", "pija para durock"),
        ("pilas para tablaroca", "pija para tablaroca"),
        ("pilas pata de broca", "pija punta de broca"),
        ("pija pata de broca", "pija punta de broca"),
        ("pijas pata de broca", "pija punta de broca"),
        # Pija y taquete — product name, don't let LLM change "y" to "para"
        ("pija y taquete", "pija y taquete"),
        ("pijas y taquete", "pija y taquete"),
        ("pija y taquetes", "pija y taquete"),
        ("pijas y taquetes", "pija y taquete"),
        ("tornillo y taquete", "pija y taquete"),
        ("tornillos y taquetes", "pija y taquete"),
        ("tornillos y taquete", "pija y taquete"),
        # Pija tablaroca = Pija 6 x 1 (the standard tablaroca screw)
        ("pija tablaroca", "pija 6 x 1"),
        ("pijas tablaroca", "pija 6 x 1"),
        ("pija para tablaroca", "pija 6 x 1"),
        ("pijas para tablaroca", "pija 6 x 1"),
    ]
    try:
        cur = conn.cursor()
        for orig, norm in _SEED_ENTRIES:
            cur.execute(
                "INSERT INTO diccionario_jerga_global (termino_original, termino_normalizado, is_protected, source) "
                "VALUES (%s, %s, TRUE, 'seed') "
                "ON CONFLICT (termino_original) DO UPDATE "
                "SET termino_normalizado = EXCLUDED.termino_normalizado, is_protected = TRUE, source = 'seed'",
                (orig, norm),
            )
        cur.close()
        print(f"SEED JERGA GLOBAL: {len(_SEED_ENTRIES)} protected entries upserted")
    except Exception as e:
        print(f"SEED JERGA GLOBAL ERROR: {repr(e)}")


def _validate_term_in_catalog(conn, normalized: str) -> bool:
    """
    Verifica que el término normalizado exista como nombre o sinónimo
    en al menos un producto de cualquier tenant.
    Evita que el LLM guarde basura como 'framer' → 'estructura'.
    """
    n = _strip_accents((normalized or "").strip().lower())
    if not n or len(n) < 2:
        return False
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM pricebook_items "
            f"WHERE lower({_sql_translate('name')}) LIKE lower(%s) "
            f"   OR lower({_sql_translate('synonyms')}) LIKE lower(%s) "
            "LIMIT 1",
            (f"%{n}%", f"%{n}%"),
        )
        found = cur.fetchone() is not None
        return found
    except Exception:
        return True  # En caso de error, permitir el guardado
    finally:
        cur.close()


def llm_normalize_query(conn, company_id: str, user_query: str, tenant_context: str = ""):
    """
    Normaliza jerga del usuario. Retorna tupla (normalized_text, source).
    source: 'local', 'global', 'llm', 'none'
    """
    q = _phonetic((user_query or "").strip().lower())
    if not q or len(q) < 2:
        return user_query, "none"

    _JERGA_TRANSLATE = _sql_translate("termino_original")

    # 1. Buscar en diccionario local (per-tenant, máxima prioridad)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT termino_normalizado FROM diccionario_jerga_local WHERE company_id = %s AND lower({_JERGA_TRANSLATE}) = %s LIMIT 1",
            (company_id, q),
        )
        row = cur.fetchone()
        cur.close()
        if row:
            print(f"JERGA LOCAL HIT: '{q}' → '{row[0]}'")
            return _strip_accents(row[0]), "local"
    except Exception as e:
        print(f"JERGA LOCAL ERROR: {repr(e)}")

    # 2. Buscar en diccionario global (compartido, incluye entradas protegidas)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT termino_normalizado, is_protected FROM diccionario_jerga_global WHERE lower({_JERGA_TRANSLATE}) = %s LIMIT 1",
            (q,),
        )
        row = cur.fetchone()
        cur.close()
        if row:
            label = "JERGA GLOBAL PROTECTED" if row[1] else "JERGA GLOBAL HIT"
            print(f"{label}: '{q}' → '{row[0]}'")
            # Track usage
            _increment_jerga_usage(conn, q, success=False)  # success se marca después en smart_search
            return _strip_accents(row[0]), "global"
    except Exception as e:
        print(f"JERGA GLOBAL ERROR: {repr(e)}")

    # 2.5. Reemplazo parcial: si la query tiene specs (ej "rejas blancas 2 metro"),
    # buscar sub-frases en la jerga global y reemplazarlas, conservando el resto.
    try:
        words = q.split()
        if len(words) >= 2:
            cur = conn.cursor()
            best_match = None
            best_len = 0
            # Probar sub-frases de más larga a más corta (greedy)
            for size in range(min(len(words), 4), 0, -1):
                for start in range(len(words) - size + 1):
                    sub = " ".join(words[start:start + size])
                    cur.execute(
                        f"SELECT termino_normalizado FROM diccionario_jerga_global WHERE lower({_JERGA_TRANSLATE}) = %s LIMIT 1",
                        (sub,),
                    )
                    row = cur.fetchone()
                    if row and len(sub) > best_len:
                        best_match = (sub, row[0], start, size)
                        best_len = len(sub)
                if best_match:
                    break  # Encontramos la sub-frase más larga que matchea
            cur.close()
            if best_match:
                orig_sub, norm_sub, start, size = best_match
                norm_sub = _strip_accents(norm_sub)
                new_words = words[:start] + norm_sub.split() + words[start + size:]
                result = " ".join(new_words)
                print(f"JERGA GLOBAL PARTIAL: '{q}' → '{result}' (replaced '{orig_sub}' → '{norm_sub}')")
                _increment_jerga_usage(conn, orig_sub, success=False)
                return result, "global"
    except Exception as e:
        print(f"JERGA GLOBAL PARTIAL ERROR: {repr(e)}")

    # 3. Llamar LLM y guardar en global (solo si validado contra catálogo)
    if not openai_client:
        return user_query, "none"
    try:
        system = (
            "Eres un normalizador de lenguaje para materiales de construcción en México. "
            "Convierte jerga, abreviaciones o errores a términos técnicos estándar de la industria. "
            "NO inventes productos. NO expliques. NO traduzcas al español. "
            "Responde SOLO con el término normalizado en minúsculas, sin puntuación extra.\n\n"
            "REGLA IMPORTANTE: Si el término es una marca, nombre comercial o tipo de producto "
            "específico (framer, durock, usg, sheetrock, tablaroca, redimix, basecoat, etc.), "
            "NO lo traduzcas ni lo cambies. Los nombres de productos se mantienen tal cual.\n\n"
            "Ejemplos:\n"
            "- 'tr hr' → 'tablaroca resistente a humedad'\n"
            "- 'tr rh' → 'tablaroca anti-moho'\n"
            "- 'tablaroca rh' → 'tablaroca anti-moho'\n"
            "- 'tr std' → 'tablaroca estandar'\n"
            "- 'tablarock' → 'tablaroca'\n"
            "- 'durok' → 'durock'\n"
            "- 'pste 6.35' → 'poste 6.35'\n"
            "- 'base coat' → 'basecoat'\n"
            "- 'cancel' → 'canal'\n"
            "- 'redimix' → 'redimix'\n"
            "- 'framer' → 'framer'\n"
            "- 'pija framer' → 'pija framer'\n"
            "- 'pija durock' → 'pija durock'\n"
            "- 'tornillos pa taquete' → 'tornillo para taquete'\n"
            "- 'pija pada tablaroca' → 'pija para tablaroca'\n"
            "Si ya es un término estándar, regrésalo igual."
        )
        if tenant_context:
            system += f"\n\nContexto del negocio: {tenant_context}"

        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": q},
            ],
            temperature=0.0,
            max_tokens=30,
        )
        normalized = _strip_accents((resp.choices[0].message.content or "").strip().lower())
        if not normalized or normalized == q:
            return user_query, "none"

        print(f"LLM NORMALIZE: '{q}' → '{normalized}'")

        # Validar que el término normalizado exista en algún catálogo
        # antes de guardarlo — evita guardar basura como "framer" → "estructura"
        if _validate_term_in_catalog(conn, normalized):
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO diccionario_jerga_global "
                    "(termino_original, termino_normalizado, is_protected, source, usage_count, success_count) "
                    "VALUES (%s, %s, FALSE, 'llm', 1, 0) "
                    "ON CONFLICT (termino_original) DO UPDATE "
                    "SET termino_normalizado = EXCLUDED.termino_normalizado, "
                    "    usage_count = diccionario_jerga_global.usage_count + 1 "
                    "WHERE NOT diccionario_jerga_global.is_protected",
                    (q, normalized),
                )
                cur.close()
                print(f"JERGA GLOBAL SAVED: '{q}' → '{normalized}' (validated, source=llm)")
            except Exception as e:
                print(f"JERGA GLOBAL SAVE ERROR: {repr(e)}")
        else:
            print(f"JERGA GLOBAL REJECTED: '{q}' → '{normalized}' (not found in any catalog)")

        return normalized, "llm"

    except Exception as e:
        print(f"LLM NORMALIZE ERROR: {repr(e)}")
        return user_query, "none"


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


def _cache_local_mapping(conn, company_id: str, term_original: str, product_name: str):
    """
    Guarda en diccionario_jerga_local para que la próxima vez
    llm_normalize_query() devuelva el nombre del producto directamente.
    """
    t_orig = (term_original or "").strip().lower()
    t_norm = (product_name or "").strip().lower()
    if not t_orig or not t_norm or t_orig == t_norm:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO diccionario_jerga_local "
            "(company_id, termino_original, termino_normalizado) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (company_id, termino_original) DO NOTHING",
            (company_id, t_orig, t_norm),
        )
        cur.close()
        print(f"CACHE LOCAL SAVED: '{t_orig}' → '{t_norm}' (tenant={company_id[:8]})")
    except Exception as e:
        print(f"CACHE LOCAL ERROR: {repr(e)}")


def _gpt_catalog_fallback(conn, company_id: str, user_query: str,
                           cart_context: str = "") -> list:
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
                        "EQUIVALENCIAS IMPORTANTES que debes conocer:\n"
                        "- 'resistente a humedad' = 'anti-moho' = 'RH' = 'HR'\n"
                        "- 'framer' es un tipo de pija/tornillo (Pija framer)\n"
                        "- 'durock' y 'tablaroca' son tipos de panel diferentes\n"
                        "- 'pija para tablaroca' ≠ 'pija para durock' (son productos distintos)\n\n"
                        "Ejemplos de contexto:\n"
                        "- Pedido con 'tee principal, tee 61' → 'placas' = plafón reticulado\n"
                        "- Pedido con 'tablaroca, poste, canal' → 'pasta' = redimix/basecoat\n"
                        "- Pedido con 'tubo conduit, clavija' → 'cinta' = cinta aislante\n\n"
                        "Reglas:\n"
                        "- UNA coincidencia clara → responde SOLO ese número (ej: '42')\n"
                        "- VARIAS opciones del MISMO tipo → números por coma (ej: '42,43,44'). Máximo 5.\n"
                        "- Nada relevante → responde exactamente: NO\n"
                        "- NUNCA incluyas productos de tipo diferente al inferido\n"
                        "- Sé FLEXIBLE con sinónimos: si el cliente dice 'resistente a humedad' "
                        "y existe 'anti-moho', ESO es una coincidencia.\n"
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

        q = _phonetic(user_query.lower().strip())
        q = build_query_text(q)

        # Strip packaging words that cause false matches (cubeta→pintura, rollo→malla)
        _packaging_words_re = r"\b(hojas?|piezas?|rollos?|bultos?|sacos?|atados?|paquetes?|costales?|cubetas?|cuvetas?|bolsas?|botes?|latas?|tiras?|cajas?|cientos?|millares?)\b"
        q_stripped = re.sub(_packaging_words_re, "", q, flags=re.IGNORECASE).strip()
        q_stripped = re.sub(r"\bde\b", " ", q_stripped).strip()
        q_stripped = re.sub(r"\s+", " ", q_stripped).strip()
        if q_stripped and len(q_stripped) >= 3:
            q = q_stripped

        _stopwords = {"para", "de", "del", "la", "el", "un", "una", "con", "sin", "los", "las"}
        _soft_stopwords = {"para", "del", "la", "el", "un", "una", "con", "sin", "los", "las"}
        q_tokens = [t for t in q.split() if t not in _stopwords]
        if not q_tokens or (len(q_tokens) == 1 and len(q_tokens[0]) <= 2 and not re.search(r"\d", q_tokens[0])):
            q_tokens = [t for t in q.split() if t not in _soft_stopwords]
        q = " ".join(q_tokens).strip() or q

        _NS_SQL = f"""SELECT sku, name, unit, price, vat_rate, bundle_size FROM pricebook_items
                      WHERE company_id = %s
                        AND lower({_sql_translate('name')}) LIKE lower(%s)
                      LIMIT 25"""

        def _name_search(term):
            c = conn.cursor()
            term_phon = _phonetic(term)
            try:
                c.execute(_NS_SQL, (company_id, f"%{term_phon}%"))
                rows = c.fetchall()
                if rows:
                    print(f">>> _name_search('{term}') → {len(rows)} rows: {[r[1] for r in rows]}")
                    return rows
                # Try singular forms: rollos→rollo, conectores→conector, etc.
                for singular in _singulars_es(term_phon):
                    c.execute(_NS_SQL, (company_id, f"%{singular}%"))
                    rows = c.fetchall()
                    if rows:
                        print(f">>> _name_search('{term}') singular='{singular}' → {len(rows)} rows: {[r[1] for r in rows]}")
                        return rows
                print(f">>> _name_search('{term}') → 0 rows")
                return []
            except Exception as e:
                print(f">>> _name_search ERROR: {repr(e)}")
                return []
            finally:
                c.close()

        def _extract_specs(text):
            t = text.lower()
            cal = None
            medida = None
            m_cal = _re.search(r"\bcal(?:ibre)?\s*(\d+(?:\.\d+)?)\b", t)
            if m_cal:
                cal = m_cal.group(1)
            # Strip calibre portion so it doesn't interfere with medida extraction
            t_no_cal = _re.sub(r"\bcal(?:ibre)?\s*\d+(?:\.\d+)?\b", "", t)
            # Match fractions like "2 1/2", "1 3/8", "1 5/8" FIRST (most specific)
            m_frac = _re.search(r"\b(\d+\s+\d+/\d+)\b", t_no_cal)
            if m_frac:
                medida = m_frac.group(1)  # e.g. "2 1/2"
            else:
                # Match "N unit" patterns including altura, largo, ancho
                m_unit = _re.search(r"\b(\d+(?:\.\d+)?)\s*(?:metros?|mts?|m|cm|centimetros?|pulgadas?|pulg|mm|altura|alto|largo|ancho)\b", t_no_cal)
                if m_unit:
                    medida = m_unit.group(1)
                elif _re.search(r"(?:de\s+)?(\d+(?:\.\d+)?)\s*(?:metros?|mts?|m)\b", t_no_cal):
                    medida = _re.search(r"(?:de\s+)?(\d+(?:\.\d+)?)\s*(?:metros?|mts?|m)\b", t_no_cal).group(1)
                else:
                    # Fallback: any decimal number (e.g., "2.50" in product names)
                    m_med = _re.search(r"\b(\d+\.\d+)\b", t_no_cal)
                    if m_med:
                        medida = m_med.group(1)
                    else:
                        # Last resort: bare integer that's likely a size (e.g. "espadas 2 simples", "tubos 2 galvanizados")
                        # Only capture if it's a small number (1-99) and NOT already the calibre
                        m_bare = _re.search(r"\b(\d{1,2})\b", t_no_cal)
                        if m_bare and m_bare.group(1) != cal:
                            medida = m_bare.group(1)
            print(f">>> _extract_specs('{text[:60]}') → medida={medida}, cal={cal}")
            return medida, cal

        def _medida_matches(medida_val, product_name):
            """Check if a measurement value matches in a product name.
            Handles: '2' matches '2.00', '1.5' matches '1.50', '2 1/2' matches '2 1/2'"""
            n = product_name.lower()
            if not medida_val:
                return False
            # Fractions: exact substring
            if " " in medida_val or "/" in medida_val:
                return medida_val in n
            # Numeric: parse and compare against all numbers in name
            try:
                target = float(medida_val)
            except ValueError:
                return medida_val in n
            # Remove cal/calibre and abb sections to avoid matching caliber as measurement
            n_no_cal = _re.sub(r'\bcal(?:ibre)?\s*\d+(?:\.\d+)?\b', '', n)
            n_no_cal = _re.sub(r'\babb\s*\d+(?:\.\d+)?\b', '', n_no_cal)
            # Find all numbers, but skip those that are part of fractions (e.g. "2 1/2" = 2.5)
            for m in _re.finditer(r'\b(\d+(?:\.\d+)?)\b', n_no_cal):
                try:
                    val = float(m.group(1))
                    if val == target:
                        # Check if this number is followed by a fraction (making it e.g. "2 1/2")
                        after = n_no_cal[m.end():]
                        if _re.match(r'\s+\d+/\d+', after):
                            continue  # Skip: this "2" is part of "2 1/2"
                        # Check if this number is the denominator of a fraction (e.g. the "2" in "1/2")
                        before = n_no_cal[:m.start()]
                        if _re.search(r'\d+/$', before):
                            continue  # Skip: this "2" is denominator of "1/2"
                        # Check if this number is the numerator of a fraction (e.g. the "3" in "3/8")
                        if _re.match(r'/\d+', after):
                            continue  # Skip: this "3" is numerator of "3/8"
                        return True
                except ValueError:
                    pass
            return False

        def _spec_bonus(item_name, medida, cal):
            n = item_name.lower()
            bonus = 0
            if medida:
                if _medida_matches(medida, item_name):
                    bonus += 30
                else:
                    bonus -= 20  # Wrong size variant
            if cal and (_re.search(rf"\bcal\s*{cal}\b", n)):
                bonus += 50
            _bt = [t for t in q.split() if len(t) >= 4]
            for tok in _bt:
                if tok in n:
                    bonus += 15
                else:
                    # Try singular forms: "soleras"→"solera", "conectores"→"conector"
                    for sg in _singulars_es(tok):
                        if sg in n:
                            bonus += 15
                            break
            return bonus

        def _tiebreak(s, row_or_item):
            if isinstance(row_or_item, dict):
                name = (row_or_item.get("name") or "").lower()
            else:
                name = (row_or_item[1] or "").lower()
            q_first = q.split()[0] if q.split() else q
            if name.startswith(q + " ") or name == q:
                return s + 50
            if name.startswith(q_first + " ") or name.split()[0] == q_first:
                return s + 25
            return s

        def _make_item(r):
            # Rows can be:
            #   (sku, name, unit, price, vat_rate, bundle_size)           — from _NS_SQL
            #   (sku, name, unit, price, vat_rate, synonyms, bundle_size) — from synonym queries
            #   (sku, name, unit, price, vat_rate, bundle_size, similarity) — from vector queries
            # bundle_size is always an int or None; synonyms is always a string
            _bs = None
            for i in range(5, len(r)):
                if isinstance(r[i], int):
                    _bs = r[i]
                    break
            return {
                "sku": r[0], "name": r[1], "unit": r[2],
                "price": float(r[3]) if r[3] is not None else None,
                "vat_rate": float(r[4]) if r[4] is not None else None,
                "bundle_size": _bs,
            }

        # ── Context: load company context_groups from DB ─────────────────
        _ctx_groups = {}  # {group_name: [keyword1, keyword2, ...]}
        try:
            _cg_cur = conn.cursor()
            _cg_cur.execute("SELECT context_groups FROM companies WHERE id = %s LIMIT 1", (company_id,))
            _cg_row = _cg_cur.fetchone()
            _cg_cur.close()
            if _cg_row and _cg_row[0]:
                import json as _json_cg
                _cg_raw = _cg_row[0]
                if isinstance(_cg_raw, str):
                    _cg_raw = _json_cg.loads(_cg_raw)
                if isinstance(_cg_raw, dict):
                    _ctx_groups = _cg_raw
                    print(f"CONTEXT GROUPS LOADED: {list(_ctx_groups.keys())}")
        except Exception as _cge:
            print(f"CONTEXT GROUPS LOAD ERROR: {repr(_cge)}")
            _ctx_groups = {}

        # Build reverse index: keyword → set of group names
        _keyword_to_groups = {}
        for gname, keywords in _ctx_groups.items():
            for kw in keywords:
                kw_ph = _phonetic(kw.lower())
                _keyword_to_groups.setdefault(kw_ph, set()).add(gname)

        def _item_group_affinity(item_name: str) -> dict:
            """Return {group_name: keyword_count} — how many keywords from each group match."""
            name_ph = _phonetic(item_name.lower())
            affinity = {}  # group → count of matching keywords
            for kw, gnames in _keyword_to_groups.items():
                if kw in name_ph:
                    for g in gnames:
                        affinity[g] = affinity.get(g, 0) + 1
            return affinity

        # Determine which groups the cart context belongs to, weighted by keyword count
        _cart_group_weights = {}  # group → count of cart keywords that match
        _ctx_name_tokens = set()
        if cart_context:
            for w in _phonetic(cart_context.lower()).replace(",", " ").split():
                w = w.strip()
                if len(w) >= 3 and not w.replace(".", "").isdigit() and w not in {"para", "del", "con", "sin", "los", "las", "una", "metros", "metro", "pieza", "piezas"}:
                    _ctx_name_tokens.add(w)
            # Count how many cart tokens belong to each group
            for w in _ctx_name_tokens:
                if w in _keyword_to_groups:
                    for g in _keyword_to_groups[w]:
                        _cart_group_weights[g] = _cart_group_weights.get(g, 0) + 1
        # Pick dominant cart groups: those with the most keyword matches
        _cart_groups = set()
        if _cart_group_weights:
            max_weight = max(_cart_group_weights.values())
            # Include groups that have at least 40% of max weight (strong signal)
            threshold = max(max_weight * 0.4, 2)
            _cart_groups = {g for g, w in _cart_group_weights.items() if w >= threshold}
        if _cart_groups:
            print(f"CART GROUPS DETECTED: {_cart_groups} weights={_cart_group_weights}")

        def _context_bonus(item_name: str) -> int:
            """Bonus/penalty based on context group AFFINITY.
            Counts how many keywords match cart groups vs non-cart groups.
            'Poste para rejacero' → rejacero has 2 keywords (poste+rejacero), tablaroca only 1 (poste)
            So if cart is tablaroca, rejacero affinity is STRONGER → penalize."""
            if not _cart_groups or not _ctx_groups:
                # Fallback: simple token overlap if no groups configured
                if not _ctx_name_tokens:
                    return 0
                name_lower = _phonetic(item_name.lower())
                name_toks = set(t for t in name_lower.split() if len(t) >= 3
                               and not t.replace(".", "").isdigit()
                               and t not in {"para", "del", "con", "sin", "cal"})
                if not name_toks:
                    return 0
                shared = name_toks & _ctx_name_tokens
                unique = name_toks - _ctx_name_tokens
                bonus = len(shared) * 10
                for tok in unique:
                    if len(tok) >= 5:
                        bonus -= 15
                return max(bonus, -40)

            # Group-based approach with affinity weighting
            affinity = _item_group_affinity(item_name)
            if not affinity:
                return 0  # item not in any known group, neutral

            # Sum keyword matches for cart groups vs non-cart groups
            cart_score = sum(cnt for g, cnt in affinity.items() if g in _cart_groups)
            other_score = sum(cnt for g, cnt in affinity.items() if g not in _cart_groups)

            # "Poste para rejacero" with cart=tablaroca:
            #   cart_score=1 (poste in tablaroca), other_score=2 (poste+rejacero in rejacero)
            #   → other wins → penalize
            # "Poste 4.10 x 3.05 cal 20" with cart=tablaroca:
            #   cart_score=1 (poste in tablaroca), other_score=1 (poste in rejacero)
            #   → tie → neutral/slight boost

            if cart_score > other_score:
                return 25   # clearly belongs to cart domain
            elif other_score > cart_score:
                return -30  # clearly belongs to OTHER domain
            elif cart_score > 0:
                return 5    # ambiguous but at least partially matches cart
            return 0

        def _context_sort(candidates, context: str):
            """Sort AND FILTER ambiguous candidates by relevance to cart context.
            If context groups are available, remove candidates from wrong groups."""
            if not context or (not _cart_groups and not _ctx_name_tokens) or len(candidates) < 2:
                return candidates

            # Score all candidates
            scored = []
            for item in candidates:
                score, r = item if isinstance(item, tuple) else (0, item)
                name = (r[1] if isinstance(r, tuple) else r.get("name", "")) or ""
                bonus = _context_bonus(name)
                scored.append((bonus, score, r))

            # If we have context groups, FILTER out candidates with negative bonus
            # but only if there are candidates with positive/neutral bonus remaining
            if _cart_groups:
                good = [s for s in scored if s[0] >= 0]
                if good and len(good) < len(scored):
                    print(f"CONTEXT FILTER: {len(scored)} → {len(good)} (removed {len(scored)-len(good)} wrong-group candidates)")
                    scored = good

            # Sort by context bonus descending
            scored.sort(key=lambda x: -x[0])
            return [(s[1], s[2]) for s in scored]

        q_medida, q_cal = _extract_specs(q)

        # ── PASO -1 + 0.5: Normalización LLM de jerga ────────────────────────
        print(f"SMART SEARCH q='{q}' original='{user_query}' context='{cart_context[:50]}'")

        try:
            cur_ctx = conn.cursor()
            cur_ctx.execute("SELECT tenant_context FROM companies WHERE id = %s LIMIT 1", (company_id,))
            row_ctx = cur_ctx.fetchone()
            cur_ctx.close()
            tenant_context = (row_ctx[0] or "") if row_ctx else ""
        except Exception:
            tenant_context = ""

        q_pre_llm = q  # guardar query antes de normalizar para cachear después
        # Pass original (non-phonetic) query to LLM so it understands the words correctly
        # (phonetic transforms like b→v confuse the LLM: "brocha"→"vrocha"→LLM thinks "viga")
        _llm_input = _strip_accents(user_query.lower().strip())
        _llm_input = re.sub(_packaging_words_re, "", _llm_input, flags=re.IGNORECASE).strip()
        _llm_input = re.sub(r"\s+", " ", _llm_input).strip()
        q_llm, norm_source = llm_normalize_query(conn, company_id, _llm_input or q, tenant_context)
        # Apply phonetic to LLM result for consistency with rest of search
        q_llm = _phonetic(q_llm)
        if q_llm != q:
            q = q_llm
            # Recalculate q_tokens after LLM normalization so new tokens
            # (e.g. "malla" added to "ciclonica") are included in multi-token fallback
            q_tokens = [t for t in q.split() if t not in _stopwords]
            if not q_tokens or (len(q_tokens) == 1 and len(q_tokens[0]) <= 2 and not re.search(r"\d", q_tokens[0])):
                q_tokens = [t for t in q.split() if t not in _soft_stopwords]
            print(f"q_tokens UPDATED after LLM: {q_tokens}")
            # Recalculate specs from LLM-normalized query (e.g. "cal18" → "calibre 18")
            # But preserve original specs if LLM dropped them
            _prev_medida, _prev_cal = q_medida, q_cal
            q_medida, q_cal = _extract_specs(q)
            if not q_medida and _prev_medida:
                q_medida = _prev_medida
                print(f"SPEC PRESERVED: medida={q_medida} (LLM dropped it)")
            if not q_cal and _prev_cal:
                q_cal = _prev_cal
                print(f"SPEC PRESERVED: cal={q_cal} (LLM dropped it)")

        # Helper para logging de eventos al final de cada paso
        def _log_event(status, paso, item=None, confidence=None):
            _log_query_event(
                conn, company_id,
                original_text=user_query,
                cleaned_text=q_pre_llm,
                normalized_text=q if q != q_pre_llm else None,
                normalization_source=norm_source,
                matched_item_name=(item or {}).get("name"),
                matched_item_sku=(item or {}).get("sku"),
                search_status=status,
                search_paso=paso,
                confidence_score=confidence,
                industry=None,  # TODO: fill from company giro
            )
            # Si fue found y vino de jerga global, marcar success
            if status == "found" and norm_source == "global" and q != q_pre_llm:
                _increment_jerga_usage(conn, q_pre_llm, success=True)
                _check_auto_promote(conn, q_pre_llm)

        # ── PASO -1: Sinónimo global ──────────────────────────────────────────
        q_resolved = resolve_global_synonym(conn, q)
        print(f"RESOLVED: q='{q}' → q_resolved='{q_resolved}'")
        if q_resolved != q:
            rows = _name_search(q_resolved)
            if rows:
                scored = [(fuzz.token_set_ratio(q_resolved, (r[1] or "").lower()), r) for r in rows]
                scored.sort(key=lambda x: x[0], reverse=True)
                if scored[0][0] >= 70:
                    print(f"GLOBAL SYNONYM ILIKE HIT: '{q_resolved}' → '{scored[0][1][1]}'")
                    item = _make_item(scored[0][1])
                    _log_event("found", "global_synonym", item, scored[0][0] / 100.0)
                    return {"status": "found", "item": item, "candidates": []}
            first_token = q_resolved.split()[0] if q_resolved.split() else q_resolved
            if first_token != q_resolved:
                rows_token = _name_search(first_token)
                if rows_token:
                    scored = [(fuzz.token_set_ratio(q_resolved, (r[1] or "").lower()), r) for r in rows_token]
                    scored.sort(key=lambda x: x[0], reverse=True)
                    if scored[0][0] >= 65:
                        print(f"GLOBAL SYNONYM FIRST TOKEN HIT: '{first_token}' → '{scored[0][1][1]}'")
                        item = _make_item(scored[0][1])
                        _log_event("found", "global_synonym_token", item, scored[0][0] / 100.0)
                        return {"status": "found", "item": item, "candidates": []}
            print(f"GLOBAL SYNONYM NO HIT: '{q_resolved}' → continuando con q original='{q}'")

        # ── PASO 0: Sinónimo exacto en pricebook (accent-insensitive) ────────
        _SYN_TRANSLATE = _sql_translate("synonyms")
        _NAME_TRANSLATE = _sql_translate("name")
        cur0 = conn.cursor()
        try:
            cur0.execute(
                f"SELECT sku, name, unit, price, vat_rate, synonyms, bundle_size FROM pricebook_items WHERE company_id = %s AND (lower({_SYN_TRANSLATE}) LIKE lower(%s) OR lower({_NAME_TRANSLATE}) LIKE lower(%s)) LIMIT 10",
                (company_id, f"%{_phonetic(q)}%", f"%{_phonetic(q)}%"),
            )
            syn_rows = cur0.fetchall()

            if not syn_rows and q_tokens:
                first_token = _phonetic(q_tokens[0])
                cur0.execute(
                    f"SELECT sku, name, unit, price, vat_rate, synonyms, bundle_size FROM pricebook_items WHERE company_id = %s AND (lower({_SYN_TRANSLATE}) LIKE lower(%s) OR lower({_NAME_TRANSLATE}) LIKE lower(%s)) LIMIT 10",
                    (company_id, f"%{first_token}%", f"%{first_token}%"),
                )
                syn_rows = cur0.fetchall()
                if len(syn_rows) > 1 and len(q_tokens) > 1:
                    rest_tokens = [_phonetic(t) for t in q_tokens[1:]]
                    syn_rows = [
                        r for r in syn_rows
                        if any(t in _phonetic((r[1] or "").lower()) for t in rest_tokens)
                        or fuzz.token_set_ratio(q, _phonetic((r[1] or "").lower())) >= 50
                    ]
        finally:
            cur0.close()

        def _best_syn_score(row, query):
            """Score considerando nombre Y sinónimos del producto (phonetic-normalized)."""
            query_plain = _phonetic(query)
            name = _phonetic((row[1] or "").lower())
            name_score = max(fuzz.token_set_ratio(query_plain, name), fuzz.partial_ratio(query_plain, name))
            # Bonus: if product name STARTS with the query, boost score strongly
            # "durok" → "Durock USG" should win over "Pija para durock"
            if name.startswith(query_plain):
                name_score = min(name_score + 25, 100)
            # Extra bonus: query IS essentially the product name (first word matches)
            name_first_word = name.split()[0] if name.split() else ""
            query_first_word = query_plain.split()[0] if query_plain.split() else ""
            if name_first_word and query_first_word and name_first_word == query_first_word:
                name_score = min(name_score + 10, 100)
            # También comparar contra cada sinónimo individual
            syns_raw = row[5] if len(row) > 5 else ""
            if syns_raw:
                for s in (syns_raw or "").split(","):
                    s = _phonetic(s.strip().lower())
                    if s:
                        syn_score = max(fuzz.token_set_ratio(query_plain, s), fuzz.partial_ratio(query_plain, s))
                        if syn_score > name_score:
                            name_score = syn_score
            return name_score

        if len(syn_rows) == 1:
            best_score = _best_syn_score(syn_rows[0], q)
            # Sanity check: at least one key query token (>3 chars) must appear
            # in the product name or synonyms to avoid false positives
            _prod_text = _phonetic(((syn_rows[0][1] or "") + " " + (syn_rows[0][5] if len(syn_rows[0]) > 5 else "") or "").lower())
            _key_tokens = [t for t in q.split() if len(t) > 3 and not t.replace(".", "").isdigit()]
            _token_overlap = sum(1 for t in _key_tokens if t in _prod_text)
            _overlap_ratio = _token_overlap / max(len(_key_tokens), 1)
            # Check if any significant key token (>5 chars) is missing from the product
            _missing_significant = [t for t in _key_tokens if len(t) > 5 and t not in _prod_text
                                    and not any(_phonetic(sg) in _prod_text for sg in _singulars_es(t))]
            if best_score >= 60 and (not _key_tokens or _overlap_ratio >= 0.5) and not _missing_significant:
                print(f"SYNONYM DIRECT HIT: query='{user_query}' match='{syn_rows[0][1]}' score={best_score} overlap={_overlap_ratio:.0%}")
                item = _make_item(syn_rows[0])
                _log_event("found", "synonym_direct", item, best_score / 100.0)
                return {"status": "found", "item": item, "candidates": []}
            else:
                print(f"SYNONYM DIRECT REJECTED (low overlap): query='{user_query}' match='{syn_rows[0][1]}' score={best_score} overlap={_overlap_ratio:.0%} key_tokens={_key_tokens}")
        elif len(syn_rows) > 1:
            # 1) Narrow by name containment (accent-insensitive)
            _q_plain = _phonetic(q)
            name_matches = [r for r in syn_rows if _q_plain in _phonetic((r[1] or "").lower())]
            if name_matches and len(name_matches) < len(syn_rows):
                print(f"SYNONYM NAME FILTER: {len(syn_rows)} → {len(name_matches)} (query='{q}' in name)")
                syn_rows = name_matches

            # 2) Filter by specs if the user specified medida/calibre
            if q_medida or q_cal:
                spec_filtered = []
                for r in syn_rows:
                    n = (r[1] or "").lower()
                    if q_medida and q_medida not in n:
                        continue
                    if q_cal and not _re.search(rf"\bcal(?:ibre)?\s*{q_cal}\b", n):
                        continue
                    spec_filtered.append(r)
                if len(spec_filtered) == 1:
                    print(f"SYNONYM SPEC RESOLVED: query='{user_query}' match='{spec_filtered[0][1]}'")
                    item = _make_item(spec_filtered[0])
                    _log_event("found", "synonym_spec", item, 0.9)
                    return {"status": "found", "item": item, "candidates": []}
                elif spec_filtered:
                    syn_rows = spec_filtered
                else:
                    print(f"SYNONYM SPEC NO MATCH: query='{user_query}' specs=({q_medida},{q_cal}) → skipping synonym ambiguous")
                    syn_rows = []

            # 3) If narrowed to 1, resolve directly
            if len(syn_rows) == 1:
                best_score = _best_syn_score(syn_rows[0], q)
                if best_score >= 60:
                    print(f"SYNONYM NARROWED HIT: query='{user_query}' match='{syn_rows[0][1]}' score={best_score}")
                    item = _make_item(syn_rows[0])
                    _log_event("found", "synonym_narrowed", item, best_score / 100.0)
                    return {"status": "found", "item": item, "candidates": []}

            # 4) Fuzzy scoring to try to resolve a clear winner
            if len(syn_rows) >= 2:
                scored_syns = []
                for r in syn_rows:
                    base = _best_syn_score(r, q)
                    bonus = _spec_bonus(r[1], q_medida, q_cal)
                    ctx_b = _context_bonus((r[1] or ""))
                    scored_syns.append((base + bonus + ctx_b, r))
                scored_syns.sort(key=lambda x: x[0], reverse=True)
                top_s = scored_syns[0][0]
                second_s = scored_syns[1][0]
                if top_s >= 75 and (top_s - second_s) >= 10:
                    print(f"SYNONYM SCORED RESOLVED: query='{user_query}' match='{scored_syns[0][1][1]}' score={top_s} gap={top_s - second_s}")
                    item = _make_item(scored_syns[0][1])
                    _log_event("found", "synonym_scored", item, top_s / 100.0)
                    return {"status": "found", "item": item, "candidates": []}

            # 4b) Check if one candidate's name contains ALL query key tokens (exact name match)
            if len(syn_rows) >= 2:
                _q_key_tokens = [_phonetic(t) for t in q_tokens if len(t) > 2 and not t.replace(".", "").isdigit()]
                if _q_key_tokens:
                    _full_matches = []
                    for r in syn_rows:
                        _pname = _phonetic((r[1] or "").lower())
                        if all(tk in _pname for tk in _q_key_tokens):
                            _full_matches.append(r)
                    if len(_full_matches) == 1:
                        print(f"SYNONYM ALL-TOKEN MATCH: query='{user_query}' match='{_full_matches[0][1]}' tokens={_q_key_tokens}")
                        item = _make_item(_full_matches[0])
                        _log_event("found", "synonym_all_token", item, 0.88)
                        return {"status": "found", "item": item, "candidates": []}

            # 5) Still ambiguous — check if candidates are relevant (have primary token)
            if syn_rows:
                _syn_key = [t for t in q_tokens if len(t) > 3 and not t.replace(".", "").isdigit()]
                _syn_primary = _syn_key[0] if _syn_key else None
                if _syn_primary:
                    _syn_relevant = [r for r in syn_rows
                                     if _syn_primary in _phonetic((r[1] or "").lower())
                                     or any(sg in _phonetic((r[1] or "").lower()) for sg in _singulars_es(_syn_primary))]
                    if not _syn_relevant:
                        print(f"SYNONYM NO RELEVANT: query='{user_query}' primary='{_syn_primary}' not in any candidate → skipping to ILIKE")
                        syn_rows = []  # clear so we fall through
                    else:
                        print(f"SYNONYM AMBIGUOUS: query='{user_query}' found={len(_syn_relevant)}")
                        _syn_sorted = _context_sort([(0, r) for r in _syn_relevant], cart_context)
                        _log_event("ambiguous", "synonym_ambiguous")
                        return {"status": "ambiguous", "item": None, "candidates": [_make_item(r) for _, r in _syn_sorted]}
                else:
                    print(f"SYNONYM AMBIGUOUS: query='{user_query}' found={len(syn_rows)}")
                    _log_event("ambiguous", "synonym_ambiguous")
                    return {"status": "ambiguous", "item": None, "candidates": [_make_item(r) for r in syn_rows]}
            # syn_rows vacío (specs no coincidieron) → seguir a ILIKE/catalog fallback

        # ── PASO 1: ILIKE directo + ranking con bonus de specs + tiebreak ─────
        pool_rows = _name_search(q)
        if not pool_rows and q.endswith("s") and len(q) > 3:
            pool_rows = _name_search(q[:-1])
        # Fallback: search ALL significant tokens and merge results.
        # E.g. "rollos malla ciclonica" → try "rollos", "malla", "ciclonica"
        # This ensures "Malla ciclonica" is found even if "rollos" only matches láminas.
        if not pool_rows:
            _seen_keys = set()
            _merged = []
            for _tok in q_tokens:
                if len(_tok) >= 3 and not _tok.replace(".", "").isdigit():
                    _tok_rows = _name_search(_tok)
                    for r in _tok_rows:
                        # Dedup by (sku, name) to handle NULL SKUs correctly
                        _key = (r[0] or "", r[1] or "")
                        if _key not in _seen_keys:
                            _seen_keys.add(_key)
                            _merged.append(r)
            pool_rows = _merged
            if pool_rows:
                print(f">>> MULTI-TOKEN FALLBACK: {len(pool_rows)} rows from tokens {[t for t in q_tokens if len(t)>=3 and not t.replace('.','').isdigit()]}")

        if pool_rows:
            scored = []
            for r in pool_rows:
                name = _phonetic((r[1] or "").lower())
                base = max(fuzz.token_set_ratio(q, name), fuzz.partial_ratio(q, name))
                bonus = _spec_bonus(r[1], q_medida, q_cal) + _context_bonus(r[1] or "")
                scored.append((base + bonus, r))

            scored = [(_tiebreak(s, r), r) for s, r in scored]
            scored.sort(key=lambda x: x[0], reverse=True)
            top = scored[0][0]
            second = scored[1][0] if len(scored) > 1 else 0
            gap = top - second
            print(f"ILIKE SCORED: {[(s, r[1]) for s, r in scored[:3]]}")
            # Helper: check overlap including singular forms
            def _token_overlap_smart(name_lower, key_tokens):
                hits = 0
                for t in key_tokens:
                    tp = _phonetic(t)
                    if tp in name_lower:
                        hits += 1
                    else:
                        for sg in _singulars_es(t):
                            if _phonetic(sg) in name_lower:
                                hits += 1
                                break
                return hits / max(len(key_tokens), 1)

            min_score = 80 if (q_medida or q_cal) else 85
            min_gap = 15 if (q_medida or q_cal) else 8
            # Skip packaging/unit words when selecting key tokens for primary check
            _ilike_key = [t for t in q_tokens if len(t) > 3 and not t.replace(".", "").isdigit()
                          and t.lower() not in _PACKAGING_WORDS]
            # Fallback: if all tokens were packaging, use original filter
            if not _ilike_key:
                _ilike_key = [t for t in q_tokens if len(t) > 3 and not t.replace(".", "").isdigit()]

            # Helper: check if primary token (or its singular) is in a name
            # Uses phonetic normalization to match b↔v, z↔s
            def _has_primary(name_lower, primary_tok):
                tp = _phonetic(primary_tok)
                if tp in name_lower:
                    return True
                for sg in _singulars_es(primary_tok):
                    if _phonetic(sg) in name_lower:
                        return True
                return False

            if top >= min_score and gap >= min_gap:
                r = scored[0][1]
                _ilike_name = _phonetic((r[1] or "").lower())
                _ilike_overlap = _token_overlap_smart(_ilike_name, _ilike_key)
                # Check that at least one key token is present — avoid "pasta durock" → "Pija para durock"
                _primary_ok = (not _ilike_key) or any(_has_primary(_ilike_name, tk) for tk in _ilike_key)
                # Reject if any significant token (>=5 chars) is completely missing
                _missing_sig = [tk for tk in _ilike_key if len(tk) >= 5
                                and not _has_primary(_ilike_name, tk)]
                if _missing_sig:
                    _primary_ok = False
                if _primary_ok and (not _ilike_key or _ilike_overlap >= 0.3):
                    print(f"ILIKE RESOLVED: query='{user_query}' match='{r[1]}' overlap={_ilike_overlap:.0%}")
                    item = _make_item(r)
                    _log_event("found", "ilike", item, top / 100.0)
                    return {"status": "found", "item": item, "candidates": []}
                else:
                    print(f"ILIKE REJECTED (primary={_ilike_key[0] if _ilike_key else '?'} ok={_primary_ok} overlap={_ilike_overlap:.0%}): query='{user_query}' match='{r[1]}' score={top} key={_ilike_key}")

            # "Obvious winner" path: even if gap is small, if the #1 candidate
            # contains the PRIMARY query token (first significant word) and #2 does NOT,
            # resolve directly. E.g. "soleras tensión 2m" → "Solera" wins over "Abrazadera"
            if top >= 80 and gap < min_gap and len(scored) >= 2 and _ilike_key:
                _primary = _ilike_key[0]  # first significant token
                _name1 = _phonetic((scored[0][1][1] or "").lower())
                _name2 = _phonetic((scored[1][1][1] or "").lower())
                _pp = _phonetic(_primary)
                _p1 = _pp in _name1 or any(_phonetic(sg) in _name1 for sg in _singulars_es(_primary))
                _p2 = _pp in _name2 or any(_phonetic(sg) in _name2 for sg in _singulars_es(_primary))
                if _p1 and not _p2:
                    r = scored[0][1]
                    print(f"ILIKE OBVIOUS WINNER: query='{user_query}' match='{r[1]}' (primary='{_primary}' in #1 but not #2)")
                    item = _make_item(r)
                    _log_event("found", "ilike_obvious", item, top / 100.0)
                    return {"status": "found", "item": item, "candidates": []}

            # Try spec-based narrowing before returning ambiguous
            if q_medida or q_cal:
                _ilike_spec = []
                for s, r in scored:
                    n = (r[1] or "").lower()
                    if q_medida and not _medida_matches(q_medida, r[1] or ""):
                        print(f"ILIKE SPEC FILTER: '{r[1]}' rejected (medida={q_medida} not matched)")
                        continue
                    if q_cal and not _re.search(rf"\bcal(?:ibre)?\s*{q_cal}\b", n):
                        print(f"ILIKE SPEC FILTER: '{r[1]}' rejected (cal={q_cal} not matched)")
                        continue
                    _ilike_spec.append((s, r))
                print(f"ILIKE SPEC NARROWING: {len(scored)} → {len(_ilike_spec)} candidates (medida={q_medida}, cal={q_cal})")
                if len(_ilike_spec) == 1:
                    print(f"ILIKE SPEC RESOLVED: query='{user_query}' match='{_ilike_spec[0][1][1]}'")
                    item = _make_item(_ilike_spec[0][1])
                    _log_event("found", "ilike_spec", item, _ilike_spec[0][0] / 100.0)
                    return {"status": "found", "item": item, "candidates": []}
                elif len(_ilike_spec) >= 2:
                    # Tiebreak: if user didn't mention a qualifier (pvc, inox, etc.)
                    # and one candidate has it but another doesn't, prefer the simpler one
                    _q_low = q.lower()
                    _extra_qualifiers = ["pvc", "inox", "acero-inox", "negro", "blanco", "cromado"]
                    _unmentioned_q = [qf for qf in _extra_qualifiers if qf not in _q_low]
                    if _unmentioned_q:
                        _plain = [(s, r) for s, r in _ilike_spec
                                  if not any(qf in (r[1] or "").lower() for qf in _unmentioned_q)]
                        if 1 <= len(_plain) < len(_ilike_spec):
                            # Some candidates filtered out by unmentioned qualifier
                            if len(_plain) == 1:
                                print(f"ILIKE SPEC QUALIFIER RESOLVED: query='{user_query}' match='{_plain[0][1][1]}' (simpler variant)")
                                item = _make_item(_plain[0][1])
                                _log_event("found", "ilike_spec_qual", item, _plain[0][0] / 100.0)
                                return {"status": "found", "item": item, "candidates": []}
                            else:
                                _ilike_spec = _plain  # narrowed but not to 1
                    scored = _ilike_spec

            # Only return candidates if top score is decent (≥65).
            # Otherwise the results are irrelevant (e.g. "rollo" matching láminas
            # when user asked for "ciclónica") — let later steps handle it.
            if top < 65:
                print(f"ILIKE SCORES TOO LOW ({top:.0f}): query='{user_query}' → skipping to next step")
            else:
                # Before returning ambiguous, check if ANY candidate has the primary token.
                # If none do, the results are irrelevant — skip to GPT fallback.
                _primary_toks = _ilike_key[:3] if _ilike_key else []
                if _primary_toks:
                    _relevant = [(s, r) for s, r in scored[:5]
                                 if any(_has_primary(_phonetic((r[1] or "").lower()), tk) for tk in _primary_toks)]
                    if not _relevant:
                        print(f"ILIKE NO RELEVANT CANDIDATES: query='{user_query}' primary={_primary_toks} not in any of top 5 → skipping to GPT fallback")
                    else:
                        _relevant = _context_sort(_relevant, cart_context)
                        _log_event("ambiguous", "ilike_ambiguous")
                        return {"status": "ambiguous", "item": None, "candidates": [_make_item(r) for _, r in _relevant]}
                else:
                    q_first_token = q.split()[0] if q.split() else q
                    filtered = [(s, r) for s, r in scored[:5] if _phonetic((r[1] or "").lower()).startswith(q_first_token)]
                    candidates = filtered if filtered else scored[:5]
                    candidates = _context_sort(candidates, cart_context)
                    _log_event("ambiguous", "ilike_ambiguous")
                    return {"status": "ambiguous", "item": None, "candidates": [_make_item(r) for _, r in candidates]}

        # ── PASO 2: tsvector + fuzzy sobre candidatos + tiebreak ─────────────
        _tokens = [t for t in q.split() if len(t) >= 3]
        _tsquery = " | ".join(_tokens) if _tokens else q

        cur2 = conn.cursor()
        try:
            cur2.execute(
                f"""
                SELECT sku, name, unit, price, vat_rate, synonyms, bundle_size
                FROM pricebook_items
                WHERE company_id = %s
                  AND (
                      search_vector @@ to_tsquery('spanish', %s)
                      OR lower({_NAME_TRANSLATE}) LIKE lower(%s)
                      OR lower({_SYN_TRANSLATE}) LIKE lower(%s)
                  )
                LIMIT 30
                """,
                (company_id, _tsquery, f"%{_phonetic(q)}%", f"%{_phonetic(q)}%"),
            )
            rows = cur2.fetchall()
        except Exception as e:
            print(f"TSVECTOR FALLBACK: {repr(e)}")
            cur2_b = conn.cursor()
            try:
                cur2_b.execute(
                    f"SELECT sku, name, unit, price, vat_rate, synonyms, bundle_size FROM pricebook_items WHERE company_id = %s AND (lower({_NAME_TRANSLATE}) LIKE lower(%s) OR lower({_SYN_TRANSLATE}) LIKE lower(%s)) LIMIT 30",
                    (company_id, f"%{_phonetic(q)}%", f"%{_phonetic(q)}%"),
                )
                rows = cur2_b.fetchall()
            finally:
                cur2_b.close()
        finally:
            cur2.close()

        scored = []
        for r in rows:
            name = _phonetic((r[1] or "").lower())
            syns = [_phonetic(s.strip().lower()) for s in (r[5] or "").split(",") if s.strip()]
            all_terms = [name] + syns
            base = max(max(fuzz.token_set_ratio(q, t), fuzz.partial_ratio(q, t)) for t in all_terms)
            name_score = max(fuzz.token_set_ratio(q, name), fuzz.partial_ratio(q, name))
            if name_score < base - 20:
                base = name_score
            bonus = _spec_bonus(r[1], q_medida, q_cal) + _context_bonus(r[1] or "")
            total = base + bonus
            if total >= 80:
                item = {
                    "sku": r[0], "name": r[1], "unit": r[2],
                    "price": float(r[3]) if r[3] is not None else None,
                    "vat_rate": float(r[4]) if r[4] is not None else None,
                }
                scored.append((total, item))

        scored = [(_tiebreak(s, item), item) for s, item in scored]
        scored.sort(key=lambda x: x[0], reverse=True)

        if len(scored) == 1:
            _fu_name = _phonetic((scored[0][1].get("name") or "").lower())
            _fu_key = [t for t in q.split() if len(t) >= 4 and not t.replace(".", "").isdigit()]
            _fu_overlap = _token_overlap_smart(_fu_name, _fu_key)
            # Accept single fuzzy result if score >= 85 AND key tokens overlap
            if scored[0][0] >= 85 and (not _fu_key or _fu_overlap >= 0.3):
                if q != q_pre_llm and scored[0][0] < 92:
                    print(f"FUZZY UNIQUE SKIPPED (LLM-normalized): query='{user_query}' match='{scored[0][1]['name']}' score={scored[0][0]} → continuando a catalog fallback")
                else:
                    print(f"FUZZY UNIQUE: query='{user_query}' match='{scored[0][1]['name']}' score={scored[0][0]} overlap={_fu_overlap:.0%}")
                    _log_event("found", "fuzzy_unique", scored[0][1], scored[0][0] / 100.0)
                    return {"status": "found", "item": scored[0][1], "candidates": []}
            else:
                print(f"FUZZY UNIQUE LOW SCORE: query='{user_query}' match='{scored[0][1]['name']}' score={scored[0][0]} overlap={_fu_overlap:.0%} → semántico")

        if len(scored) > 1:
            top_score = scored[0][0]
            second_score = scored[1][0]
            gap = top_score - second_score
            min_score = 85 if (q_medida or q_cal) else 95
            if top_score >= min_score and gap >= 10:
                print(f"FUZZY CLEAR WIN: query='{user_query}' match='{scored[0][1]['name']}' score={top_score}")
                _log_event("found", "fuzzy_clear", scored[0][1], top_score / 100.0)
                return {"status": "found", "item": scored[0][1], "candidates": []}

            # Try spec-based narrowing: filter by medida/calibre if user specified
            if q_medida or q_cal:
                spec_filtered = []
                for s, item in scored:
                    n = (item.get("name") or "").lower()
                    if q_medida and q_medida not in n:
                        continue
                    if q_cal and not _re.search(rf"\bcal(?:ibre)?\s*{q_cal}\b", n):
                        continue
                    spec_filtered.append((s, item))
                if len(spec_filtered) == 1:
                    print(f"FUZZY SPEC RESOLVED: query='{user_query}' match='{spec_filtered[0][1]['name']}' score={spec_filtered[0][0]}")
                    _log_event("found", "fuzzy_spec", spec_filtered[0][1], spec_filtered[0][0] / 100.0)
                    return {"status": "found", "item": spec_filtered[0][1], "candidates": []}
                elif spec_filtered:
                    scored = spec_filtered  # narrow candidates for return

            # Key token overlap: if top match has strong token overlap, auto-pick
            _fa_name = _phonetic((scored[0][1].get("name") or "").lower())
            _fa_key = [t for t in q.split() if len(t) >= 4 and not t.replace(".", "").isdigit()
                       and t.lower() not in _PACKAGING_WORDS]
            if not _fa_key:
                _fa_key = [t for t in q.split() if len(t) >= 4 and not t.replace(".", "").isdigit()]
            _fa_overlap = _token_overlap_smart(_fa_name, _fa_key)
            if top_score >= 90 and _fa_overlap >= 0.5 and gap >= 5:
                print(f"FUZZY OVERLAP WIN: query='{user_query}' match='{scored[0][1]['name']}' score={top_score} overlap={_fa_overlap:.0%} gap={gap}")
                _log_event("found", "fuzzy_overlap", scored[0][1], top_score / 100.0)
                return {"status": "found", "item": scored[0][1], "candidates": []}

            # "Obvious winner": primary token in #1 but not #2
            if top_score >= 80 and _fa_key:
                _primary = _fa_key[0]
                _name1 = _phonetic((scored[0][1].get("name") or "").lower())
                _name2 = _phonetic((scored[1][1].get("name") or "").lower())
                _pp = _phonetic(_primary)
                _p1 = _pp in _name1 or any(_phonetic(sg) in _name1 for sg in _singulars_es(_primary))
                _p2 = _pp in _name2 or any(_phonetic(sg) in _name2 for sg in _singulars_es(_primary))
                if _p1 and not _p2:
                    print(f"FUZZY OBVIOUS WINNER: query='{user_query}' match='{scored[0][1]['name']}' (primary='{_primary}' in #1 but not #2)")
                    _log_event("found", "fuzzy_obvious", scored[0][1], top_score / 100.0)
                    return {"status": "found", "item": scored[0][1], "candidates": []}

            print(f"FUZZY AMBIGUOUS: query='{user_query}' found={len(scored)}")
            q_first_token = q.split()[0] if q.split() else q
            filtered = [(s, item) for s, item in scored[:5] if _phonetic((item.get("name") or "").lower()).startswith(q_first_token)]
            candidates = filtered if filtered else scored[:5]
            _log_event("ambiguous", "fuzzy_ambiguous")
            return {"status": "ambiguous", "item": None, "candidates": [item for _, item in candidates]}

        # ── PASO 3: HYBRID SEARCH (keyword + vector + RRF + LLM reranker) ────
        print(f"→ HYBRID SEARCH: query='{user_query}' q='{q}'")
        hybrid_result = hybrid_search(
            conn, company_id, user_query, q, q_tokens,
            q_medida=q_medida, q_cal=q_cal, cart_context=cart_context,
        )

        if hybrid_result["status"] == "found" and hybrid_result.get("item"):
            matched = hybrid_result["item"]
            print(f"HYBRID FOUND: query='{user_query}' match='{matched['name']}'")
            _cache_local_mapping(conn, company_id, q_pre_llm, matched["name"])
            if q != q_pre_llm:
                _cache_local_mapping(conn, company_id, q, matched["name"])
            _log_event("found", "hybrid", matched, 0.85)
            return hybrid_result

        if hybrid_result["status"] == "ambiguous" and hybrid_result.get("candidates"):
            print(f"HYBRID AMBIGUOUS: query='{user_query}' found={len(hybrid_result['candidates'])}")
            _log_event("ambiguous", "hybrid_ambiguous")
            return hybrid_result

        # ── PASO 4: not found ─────────────────────────────────────────────────
        print(f"NOT FOUND: query='{user_query}'")
        _log_event("not_found", "not_found")
        return {"status": "not_found", "item": None, "candidates": []}

    except Exception as e:
        print(f"SMART SEARCH FATAL ERROR: {repr(e)}")
        return {"status": "not_found", "item": None, "candidates": []}
