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

        _stopwords = {"para", "de", "del", "la", "el", "un", "una", "con", "sin", "los", "las"}
        _soft_stopwords = {"para", "del", "la", "el", "un", "una", "con", "sin", "los", "las"}
        q_tokens = [t for t in q.split() if t not in _stopwords]
        if not q_tokens or (len(q_tokens) == 1 and len(q_tokens[0]) <= 2 and not re.search(r"\d", q_tokens[0])):
            q_tokens = [t for t in q.split() if t not in _soft_stopwords]
        q = " ".join(q_tokens).strip() or q

        _NS_SQL = f"""SELECT sku, name, unit, price, vat_rate FROM pricebook_items
                      WHERE company_id = %s
                        AND lower({_sql_translate('name')}) LIKE lower(%s)
                      LIMIT 10"""

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
            # Match fractions like "2 1/2", "1 3/8", "1 5/8" FIRST (most specific)
            m_frac = _re.search(r"\b(\d+\s+\d+/\d+)\b", t)
            if m_frac:
                medida = m_frac.group(1)  # e.g. "2 1/2"
            else:
                # Match "N metro(s)" / "N m" / "N cm" / "N pulgadas" patterns
                m_unit = _re.search(r"\b(\d+(?:\.\d+)?)\s*(?:metros?|mts?|m|cm|centimetros?|pulgadas?|pulg|mm)\b", t)
                if m_unit:
                    medida = m_unit.group(1)
                elif _re.search(r"(?:de\s+)?(\d+(?:\.\d+)?)\s*(?:metros?|mts?|m)\b", t):
                    medida = _re.search(r"(?:de\s+)?(\d+(?:\.\d+)?)\s*(?:metros?|mts?|m)\b", t).group(1)
                else:
                    # Fallback: any decimal number (e.g., "2.50" in product names)
                    m_med = _re.search(r"\b(\d+\.\d+)\b", t)
                    if m_med:
                        medida = m_med.group(1)
            print(f">>> _extract_specs('{text[:60]}') → medida={medida}, cal={cal}")
            return medida, cal

        def _spec_bonus(item_name, medida, cal):
            n = item_name.lower()
            bonus = 0
            if medida:
                # For fractions like "2 1/2", use simple string containment
                if " " in medida or "/" in medida:
                    if medida in n:
                        bonus += 30
                    else:
                        # Penalize: has a different fraction/measurement
                        bonus -= 20
                else:
                    # Exact medida in product name (e.g., "2" in "2 metro x 2.50 m")
                    if _re.search(rf"\b{_re.escape(medida)}\s*(?:metros?|mts?|m|cm|mm|pulg)?\b", n):
                        bonus += 30
                    # Penalize if product has a DIFFERENT leading measurement
                    m_prod = _re.search(r"\b(\d+(?:\.\d+)?)\s*(?:metros?|mts?|m)\b", n)
                    if m_prod and m_prod.group(1) != medida:
                        bonus -= 20  # Wrong size variant
            if cal and (_re.search(rf"\bcal\s*{cal}\b", n)):
                bonus += 50
            q_tokens = [t for t in q.split() if len(t) >= 4]
            for tok in q_tokens:
                if tok in n:
                    bonus += 15
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
            return {
                "sku": r[0], "name": r[1], "unit": r[2],
                "price": float(r[3]) if r[3] is not None else None,
                "vat_rate": float(r[4]) if r[4] is not None else None,
            }

        # ── Context bonus: use other cart items to disambiguate ───────────
        # If cart has "ciclonica, concertina, espadas" → boost fence products
        _ctx_tokens = set()
        if cart_context:
            for w in _phonetic(cart_context.lower()).split():
                if len(w) >= 4 and not w.replace(".", "").isdigit() and w not in {"para", "metros", "metro", "rollos", "rollo", "piezas", "pieza", "bultos", "bulto", "cajas", "caja"}:
                    _ctx_tokens.add(w)
        _ctx_stopwords = {"para", "de", "del", "con", "sin", "los", "las", "una"}
        _ctx_tokens -= _ctx_stopwords

        def _context_bonus(item_name: str) -> int:
            """Bonus for products sharing tokens with other cart items."""
            if not _ctx_tokens:
                return 0
            name_lower = _phonetic(item_name.lower())
            hits = sum(1 for t in _ctx_tokens if t in name_lower)
            return min(hits * 10, 30)  # max 30 bonus from context

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
        q_llm, norm_source = llm_normalize_query(conn, company_id, q, tenant_context)
        if q_llm != q:
            q = q_llm
            # Recalculate q_tokens after LLM normalization so new tokens
            # (e.g. "malla" added to "ciclonica") are included in multi-token fallback
            q_tokens = [t for t in q.split() if t not in _stopwords]
            if not q_tokens or (len(q_tokens) == 1 and len(q_tokens[0]) <= 2 and not re.search(r"\d", q_tokens[0])):
                q_tokens = [t for t in q.split() if t not in _soft_stopwords]
            print(f"q_tokens UPDATED after LLM: {q_tokens}")
            # Recalculate specs from LLM-normalized query (e.g. "cal18" → "calibre 18")
            q_medida, q_cal = _extract_specs(q)

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
                f"SELECT sku, name, unit, price, vat_rate, synonyms FROM pricebook_items WHERE company_id = %s AND lower({_SYN_TRANSLATE}) LIKE lower(%s) LIMIT 5",
                (company_id, f"%{_phonetic(q)}%"),
            )
            syn_rows = cur0.fetchall()

            if not syn_rows and q_tokens:
                first_token = _phonetic(q_tokens[0])
                cur0.execute(
                    f"SELECT sku, name, unit, price, vat_rate, synonyms FROM pricebook_items WHERE company_id = %s AND lower({_SYN_TRANSLATE}) LIKE lower(%s) LIMIT 5",
                    (company_id, f"%{first_token}%"),
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
            if best_score >= 60 and (not _key_tokens or _overlap_ratio >= 0.3):
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
                    scored_syns.append((base + bonus, r))
                scored_syns.sort(key=lambda x: x[0], reverse=True)
                top_s = scored_syns[0][0]
                second_s = scored_syns[1][0]
                if top_s >= 75 and (top_s - second_s) >= 10:
                    print(f"SYNONYM SCORED RESOLVED: query='{user_query}' match='{scored_syns[0][1][1]}' score={top_s} gap={top_s - second_s}")
                    item = _make_item(scored_syns[0][1])
                    _log_event("found", "synonym_scored", item, top_s / 100.0)
                    return {"status": "found", "item": item, "candidates": []}

            # 5) Still ambiguous — return candidates for clarification
            if syn_rows:
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
            min_score = 80 if (q_medida or q_cal) else 85
            min_gap = 15 if (q_medida or q_cal) else 8
            if top >= min_score and gap >= min_gap:
                r = scored[0][1]
                # Sanity: verify key query tokens overlap with product name
                _ilike_name = _phonetic((r[1] or "").lower())
                _ilike_key = [t for t in q_tokens if len(t) > 3 and not t.replace(".", "").isdigit()]
                _ilike_overlap = sum(1 for t in _ilike_key if t in _ilike_name) / max(len(_ilike_key), 1)
                if not _ilike_key or _ilike_overlap >= 0.3:
                    print(f"ILIKE RESOLVED: query='{user_query}' match='{r[1]}' overlap={_ilike_overlap:.0%}")
                    item = _make_item(r)
                    _log_event("found", "ilike", item, top / 100.0)
                    return {"status": "found", "item": item, "candidates": []}
                else:
                    print(f"ILIKE REJECTED (low overlap): query='{user_query}' match='{r[1]}' score={top} overlap={_ilike_overlap:.0%} key={_ilike_key}")
            # Try spec-based narrowing before returning ambiguous
            if q_medida or q_cal:
                _ilike_spec = []
                for s, r in scored:
                    n = (r[1] or "").lower()
                    if q_medida and q_medida not in n:
                        continue
                    if q_cal and not _re.search(rf"\bcal(?:ibre)?\s*{q_cal}\b", n):
                        continue
                    _ilike_spec.append((s, r))
                if len(_ilike_spec) == 1:
                    print(f"ILIKE SPEC RESOLVED: query='{user_query}' match='{_ilike_spec[0][1][1]}'")
                    item = _make_item(_ilike_spec[0][1])
                    _log_event("found", "ilike_spec", item, _ilike_spec[0][0] / 100.0)
                    return {"status": "found", "item": item, "candidates": []}
                elif _ilike_spec:
                    scored = _ilike_spec

            # Only return candidates if top score is decent (≥65).
            # Otherwise the results are irrelevant (e.g. "rollo" matching láminas
            # when user asked for "ciclónica") — let later steps handle it.
            if top < 65:
                print(f"ILIKE SCORES TOO LOW ({top:.0f}): query='{user_query}' → skipping to next step")
            else:
                q_first_token = q.split()[0] if q.split() else q
                filtered = [(s, r) for s, r in scored[:5] if _phonetic((r[1] or "").lower()).startswith(q_first_token)]
                candidates = filtered if filtered else scored[:5]
                _log_event("ambiguous", "ilike_ambiguous")
                return {"status": "ambiguous", "item": None, "candidates": [_make_item(r) for _, r in candidates]}

        # ── PASO 2: tsvector + fuzzy sobre candidatos + tiebreak ─────────────
        _tokens = [t for t in q.split() if len(t) >= 3]
        _tsquery = " | ".join(_tokens) if _tokens else q

        cur2 = conn.cursor()
        try:
            cur2.execute(
                f"""
                SELECT sku, name, unit, price, vat_rate, synonyms
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
                    f"SELECT sku, name, unit, price, vat_rate, synonyms FROM pricebook_items WHERE company_id = %s AND (lower({_NAME_TRANSLATE}) LIKE lower(%s) OR lower({_SYN_TRANSLATE}) LIKE lower(%s)) LIMIT 30",
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
            _fu_overlap = sum(1 for t in _fu_key if t in _fu_name) / max(len(_fu_key), 1)
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
            _fa_key = [t for t in q.split() if len(t) >= 4 and not t.replace(".", "").isdigit()]
            _fa_overlap = sum(1 for t in _fa_key if t in _fa_name) / max(len(_fa_key), 1)
            if top_score >= 90 and _fa_overlap >= 0.5 and gap >= 5:
                print(f"FUZZY OVERLAP WIN: query='{user_query}' match='{scored[0][1]['name']}' score={top_score} overlap={_fa_overlap:.0%} gap={gap}")
                _log_event("found", "fuzzy_overlap", scored[0][1], top_score / 100.0)
                return {"status": "found", "item": scored[0][1], "candidates": []}

            print(f"FUZZY AMBIGUOUS: query='{user_query}' found={len(scored)}")
            q_first_token = q.split()[0] if q.split() else q
            filtered = [(s, item) for s, item in scored[:5] if _phonetic((item.get("name") or "").lower()).startswith(q_first_token)]
            candidates = filtered if filtered else scored[:5]
            _log_event("ambiguous", "fuzzy_ambiguous")
            return {"status": "ambiguous", "item": None, "candidates": [item for _, item in candidates]}

        # ── PASO 3: Semántico ─────────────────────────────────────────────────
        words = user_query.strip().split()
        cand_threshold = 0.55 if len(words) == 1 else 0.60 if len(words) == 2 else 0.65

        candidates = semantic_search_candidates(conn, company_id, user_query,
                                                threshold=cand_threshold, limit=5)
        if candidates and candidates[0].get("similarity", 0) >= 0.60:
            print(f"SEMANTIC CANDIDATES: query='{user_query}' found={len(candidates)}")
            _log_event("ambiguous", "semantic", None, candidates[0].get("similarity"))
            return {"status": "ambiguous", "item": None, "candidates": candidates}

        candidates_low = semantic_search_candidates(conn, company_id, user_query,
                                                    threshold=0.50, limit=3)
        if candidates_low:
            print(f"SEMANTIC LOW THRESHOLD: query='{user_query}' found={len(candidates_low)}")
            _log_event("ambiguous", "semantic_low", None, candidates_low[0].get("similarity"))
            return {"status": "ambiguous", "item": None, "candidates": candidates_low}

        # ── PASO 3.5: GPT Catalog Fallback — el LLM ve el catálogo real ────────
        try:
            fallback_results = _gpt_catalog_fallback(conn, company_id, q, cart_context)
            if fallback_results:
                if len(fallback_results) == 1:
                    matched = fallback_results[0]
                    print(f"GPT CATALOG FOUND: query='{user_query}' match='{matched['name']}'")
                    _cache_local_mapping(conn, company_id, q_pre_llm, matched["name"])
                    if q != q_pre_llm:
                        _cache_local_mapping(conn, company_id, q, matched["name"])
                    _auto_save_synonym(conn, company_id, q, matched["name"])
                    _log_event("found", "gpt_catalog", matched, 0.85)
                    return {"status": "found", "item": matched, "candidates": []}
                else:
                    print(f"GPT CATALOG AMBIGUOUS: query='{user_query}' found={len(fallback_results)}")
                    _log_event("ambiguous", "gpt_catalog_ambiguous")
                    return {"status": "ambiguous", "item": None, "candidates": fallback_results}
        except Exception as e:
            print(f"GPT CATALOG FALLBACK ERROR: {repr(e)}")

        # ── PASO 4: no encontrado → buscar sugerencias similares ──────────────
        # En vez de solo decir "no encontrado", buscar productos similares
        # por la primera palabra para dar opciones al cliente.
        suggestions = []
        try:
            first_word = q.split()[0] if q.split() else q
            if len(first_word) >= 3:
                cur_sug = conn.cursor()
                try:
                    cur_sug.execute(
                        "SELECT sku, name, unit, price, vat_rate FROM pricebook_items "
                        "WHERE company_id = %s AND lower(name) LIKE lower(%s) LIMIT 5",
                        (company_id, f"%{first_word}%"),
                    )
                    sug_rows = cur_sug.fetchall()
                finally:
                    cur_sug.close()
                if sug_rows:
                    suggestions = [_make_item(r) for r in sug_rows]
                    print(f"NOT FOUND WITH SUGGESTIONS: query='{user_query}' suggestions={[s['name'] for s in suggestions]}")
        except Exception as e:
            print(f"SUGGESTIONS ERROR: {repr(e)}")

        if not suggestions:
            print(f"NOT FOUND: query='{user_query}' → sin sugerencias")
        _log_event("not_found", "not_found_suggestions" if suggestions else "not_found")
        return {"status": "not_found", "item": None, "candidates": suggestions}

    except Exception as e:
        print(f"SMART SEARCH FATAL ERROR: {repr(e)}")
        return {"status": "not_found", "item": None, "candidates": []}
