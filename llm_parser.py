"""
llm_parser.py — LLM-first order parser for CotizaExpress.

Reemplaza extract_qty_items_robust + ner_extract_items.

Diseño:
- Un solo call a GPT-4o-mini con el mensaje completo + catálogo como contexto.
- Cero jerga manual por cliente. Jerga global opcional (típica de ferretería MX).
- Output JSON estructurado: key, name, qty, unit, confidence, matched_text, notes.
- key = identificador estable. Como pricebook_items.sku está vacío en Aceromax,
  usamos el nombre normalizado como key (lowercase, collapse spaces).
- Si confidence < min_confidence o key=null → needs_escalation=True → escalar a dueño.
- Costo estimado ~$0.0005/mensaje con catálogos de 200 SKUs en contexto.

Integración:
- build_reply_for_company() llama llm_parse_order() como path principal.
- Si LLM falla (timeout, error JSON) → fallback silencioso a extract_qty_items_robust.
- Shadow mode primero: loggear ambos resultados 24h antes de activar en producción.
"""

from __future__ import annotations

import json
import os
import re
import time
import unicodedata
from typing import Any

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # para tests offline

_client: Any = None


def _get_client():
    global _client
    if _client is None:
        if OpenAI is None:
            raise RuntimeError("openai package not installed")
        _client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return _client


# ---------------------------------------------------------------------------
# Normalización
# ---------------------------------------------------------------------------

def norm_key(name: str) -> str:
    """
    Convierte el nombre de producto a una key estable:
    lowercase, sin acentos, colapsando espacios, sin puntuación suelta.

    Sirve como identificador cuando el SKU real está vacío.
    """
    if not name:
        return ""
    s = name.strip().lower()
    # Quitar acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Normalizar comillas tipográficas
    s = s.replace("\u201c", '"').replace("\u201d", '"').replace("\u2019", "'")
    # Colapsar whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------------------------------------------------------------------------
# Pre-limpieza del mensaje
# ---------------------------------------------------------------------------

# Botones de la UI que el cliente puede tocar y llegan como texto
_UI_BUTTONS = {
    "🗑️ quitar producto", "quitar producto",
    "➕ agregar más", "agregar mas", "agregar más",
    "💳 pagar", "pagar",
    "👤 hablar con alguien", "hablar con alguien", "hablar con ejecutivo",
    "🔄 nueva cotización", "nueva cotizacion", "nueva cotización",
    "🔨 cotizar materiales", "cotizar materiales",
    "🕐 horarios y ubicación", "horarios y ubicacion", "horarios y ubicación",
    "📐 cotizar cálculo", "cotizar calculo", "cotizar cálculo",
    "salir", "saliir", "salor", "cancelar", "ver carrito", "hola",
    "buenos dias", "buenos días", "buenas tardes", "buenas tarde",
    "buen dia", "buen día",
}

# pick_A0, pick_A1... button IDs que se filtran como texto
_PICK_ID_RE = re.compile(r"^pick_[a-z]\d+$", re.IGNORECASE)

# Prefijos de timestamp de WhatsApp al pegar: "[14/04/26, 5:20:26 p.m.] ARB: ..."
_WA_TIMESTAMP_RE = re.compile(
    r"^\[\d{1,2}/\d{1,2}/\d{2,4},\s+\d{1,2}:\d{2}(:\d{2})?\s*[ap]\.?\s*m\.?\]\s*[^:]+:\s*",
    re.IGNORECASE | re.MULTILINE,
)

# Prefijos tipo viñeta markdown "•⁠  ⁠" (WhatsApp copia)
_BULLET_PREFIX_RE = re.compile(r"^\s*[•·⁠\-*]+\s*", re.MULTILINE)

# ── Pre-LLM phrase normalization ──────────────────────────────────────────
# Phrases the LLM consistently misparses. We rewrite them BEFORE sending to
# the model so it never sees the confusing token.  Each tuple is
# (compiled regex, replacement string).
_PHRASE_NORMALIZATIONS = [
    # "canal de amarre" / "canal amarre" / "canales de amarre" → just "canal"
    # The word "amarre" tricks GPT into matching "ángulo de amarre" instead.
    (re.compile(r"\bcanal(?:es)?\s+(?:de\s+)?amarre\b", re.IGNORECASE), "canal"),
]

# Líneas que son nombres de proyecto al inicio ("Mat. Privanzas", "Del closet")
_PROJECT_HEADER_RE = re.compile(
    r"^\s*(mat\.?|material|materiales|proyecto|del?|para|obra)\b[^\n]{0,40}$",
    re.IGNORECASE,
)


def is_ui_interaction(text: str) -> bool:
    """True si el mensaje es un botón/UI interaction y no una orden real."""
    t = (text or "").strip().lower()
    if not t:
        return True
    if _PICK_ID_RE.match(t):
        return True
    # Quitar puntuación trivial
    t_clean = re.sub(r"[^\w\s]", "", t).strip()
    t_clean = re.sub(r"\s+", " ", t_clean)
    if t_clean in _UI_BUTTONS:
        return True
    # Saludo puro sin más contenido
    if t_clean in {"hola", "buenos dias", "buenas tardes", "buenas noches", "buen dia"}:
        return True
    return False


def preclean_message(text: str) -> str:
    """
    Limpia el mensaje antes de pasarlo al LLM:
    - Quita timestamps de WhatsApp pegados
    - Quita viñetas/prefijos markdown
    - Quita headers de proyecto al inicio
    - Colapsa líneas vacías múltiples
    """
    if not text:
        return ""
    t = text

    # Quitar timestamps de WhatsApp
    t = _WA_TIMESTAMP_RE.sub("", t)

    # Quitar viñetas al inicio de cada línea
    t = _BULLET_PREFIX_RE.sub("", t)

    # Normalize known ambiguous phrases BEFORE the LLM sees them
    for _pat, _repl in _PHRASE_NORMALIZATIONS:
        t = _pat.sub(_repl, t)

    # Partir en líneas, quitar headers de proyecto de las primeras 2 líneas
    lines = [ln.rstrip() for ln in t.splitlines()]
    while lines and (not lines[0].strip() or _PROJECT_HEADER_RE.match(lines[0])):
        lines.pop(0)

    # Colapsar múltiples líneas vacías consecutivas
    cleaned: list[str] = []
    prev_empty = False
    for ln in lines:
        is_empty = not ln.strip()
        if is_empty and prev_empty:
            continue
        cleaned.append(ln)
        prev_empty = is_empty

    return "\n".join(cleaned).strip()


# ---------------------------------------------------------------------------
# Catálogo → contexto
# ---------------------------------------------------------------------------

def format_catalog_for_prompt(catalog: list[dict]) -> str:
    """
    Convierte pricebook_items a texto compacto para el prompt.

    Formato (una línea por producto):
        key: <norm_key>
        <name> | <unit> | <price>

    Usamos dos líneas por producto para que el LLM distinga claramente la key
    que debe devolver vs el nombre display. key siempre en lowercase sin acentos.
    """
    lines = []
    for item in catalog:
        name = (item.get("name") or "").strip()
        if not name:
            continue
        key = norm_key(name)
        unit = (item.get("unit") or "pza").strip()
        price = item.get("price")
        price_str = f"{float(price):.2f}" if price is not None else "-"
        default_tag = " [DEFAULT]" if item.get("is_default") else ""
        lines.append(f"- {key} || {name} | {unit} | {price_str}{default_tag}")
    return "\n".join(lines)


# Mapa de jerga → keywords del catálogo. Si el mensaje contiene la jerga,
# expandimos las búsqueda con estos términos para que el prefilter los rankee alto.
JERGA_EXPANSION = {
    "tablarock": ["tablaroca", "panel", "yeso"],
    "tabla": ["tablaroca", "panel", "yeso"],
    "panel": ["tablaroca", "panel", "yeso"],
    "rey": ["tablaroca", "panel"],
    "muro": ["tablaroca", "panel", "durock"],
    "muros": ["tablaroca", "panel", "durock"],
    "pared": ["tablaroca", "panel"],
    "paredes": ["tablaroca", "panel"],
    "hoja": ["tablaroca", "panel"],
    "hojas": ["tablaroca", "panel"],
    "lamina": ["tablaroca", "lamina"],
    "laminas": ["tablaroca", "lamina"],
    "lightrey": ["ultralight", "tablaroca"],
    "ultralight": ["ultralight", "tablaroca"],
    "duroc": ["durock"],
    "durrock": ["durock"],
    "permabase": ["durock"],
    "perma": ["durock"],
    "permabse": ["durock"],
    "biscoat": ["basecoat"],
    "basekoat": ["basecoat"],
    "bescool": ["basecoat"],
    "bescol": ["basecoat"],
    "bescot": ["basecoat"],
    "compuesto": ["basecoat", "redimix"],
    "pasta": ["basecoat", "redimix"],
    "redemix": ["redimix"],
    "ready": ["redimix"],
    "perfacita": ["perfacinta"],
    "prefacinta": ["perfacinta"],
    "cinta": ["perfacinta", "cinta", "fibra"],
    "malla": ["cinta", "fibra"],
    "maya": ["cinta", "fibra"],
    "pilas": ["pija"],
    "pila": ["pija"],
    "pijas": ["pija"],
    "tornillo": ["pija"],
    "tornillos": ["pija"],
    "framer": ["pija", "framer"],
    "fremer": ["pija", "framer"],
    "flamer": ["pija", "framer"],
    "fijasora": ["pija"],
    "taquete": ["taquete", "expansion"],
    "ancla": ["taquete", "expansion"],
    "cancel": ["canal"],
    "cnal": ["canal"],
    "canel": ["canal"],
    "cana": ["canal"],
    "canaleta": ["canal", "canaleta"],
    "amarre": ["canal", "amarre"],
    "carga": ["canaleta", "carga"],
    "cargadora": ["canaleta", "carga"],
    "reborde": ["reborde"],
    "revorde": ["reborde"],
    "jota": ["reborde"],
    "pste": ["poste"],
    "psts": ["poste"],
    "postes": ["poste"],
    "rejas": ["rejacero", "reja"],
    "reja": ["rejacero", "reja"],
    "abrazadera": ["abrazadera", "rejacero"],
    "galleta": ["plafon", "registrable"],
    "plafones": ["plafon", "registrable"],
    "plafon": ["plafon"],
    "barrote": ["barrote", "madera"],
    "barrotes": ["barrote", "madera"],
    "tira": ["barrote", "madera"],
    "tiras": ["barrote", "madera"],
    "tramos": ["barrote", "madera"],
    "liston": ["barrote", "madera"],
    "listones": ["barrote", "madera"],
    "tabla wr": ["tablaroca", "anti", "moho"],
    "hoja wr": ["tablaroca", "anti", "moho"],
    "celeste": ["tablaroca", "anti", "moho"],
    "azul": ["tablaroca", "anti", "moho"],
    "rh": ["tablaroca", "anti", "moho"],
    "wr": ["tablaroca", "anti", "moho"],
    "antimoho": ["tablaroca", "anti", "moho"],
    "securok": ["securock"],
}

# Productos que SIEMPRE se incluyen como contexto, pase lo que pase
# (los más comunes — dan al LLM un anclaje aunque no haya match exacto)
_ALWAYS_INCLUDE_KEYWORDS = {
    "tablaroca", "durock", "basecoat", "perfacinta", "pija",
    "canal", "poste", "rejacero", "redimix",
}


def prefilter_catalog(catalog: list[dict], text: str, max_items: int = 80) -> list[dict]:
    """
    Rankea catálogo por overlap de tokens (con expansión de jerga) y devuelve
    los top max_items. FALLBACK — se usa solo si el prefilter semántico no está
    disponible (sin company_id, sin embeddings en DB, o error).

    Esto reduce drásticamente los tokens del prompt — para Aceromax (~230 items)
    bajamos a ~50-80, lo que típicamente recorta 60-70% la latencia.
    """
    if len(catalog) <= max_items:
        return catalog

    msg_norm = norm_key(text)
    raw_tokens = [t for t in re.findall(r"[a-z0-9]{2,}", msg_norm) if t]
    msg_tokens = set(raw_tokens)
    # Expandir con jerga
    expanded = set(msg_tokens)
    for tok in raw_tokens:
        if tok in JERGA_EXPANSION:
            expanded.update(JERGA_EXPANSION[tok])
    if not expanded:
        return catalog[:max_items]

    scored = []
    for item in catalog:
        name_norm = norm_key(item.get("name") or "")
        name_tokens = set(re.findall(r"[a-z0-9]{2,}", name_norm))
        score = len(expanded & name_tokens)
        # Boost: items en _ALWAYS_INCLUDE_KEYWORDS reciben +0.1 para empate
        if name_tokens & _ALWAYS_INCLUDE_KEYWORDS:
            score += 0.1
        scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    # Cortar en max_items, pero garantizar que items con score >= 1 entren todos
    strong = [it for sc, it in scored if sc >= 1]
    if len(strong) >= max_items:
        return strong[:max_items]
    # Pad con los mejores remaining
    return [it for _sc, it in scored[:max_items]]


def _split_message_into_queries(text: str) -> list[str]:
    """
    Divide un mensaje multi-producto en líneas individuales para buscar
    cada producto por separado en embeddings.
    '31 perma base\\n7 rollos de maya' → ['perma base', 'maya']
    """
    import logging
    _log = logging.getLogger("cotizaexpress.llm_parser")

    # Noise words to strip from each line
    _NOISE = {
        "de", "del", "la", "las", "los", "el", "un", "una", "unos", "unas",
        "para", "con", "por", "en", "y", "o", "x", "mt", "mts", "metro",
        "metros", "pza", "pzas", "pieza", "piezas", "rollo", "rollos",
        "caja", "cajas", "bolsa", "bolsas", "bulto", "bultos", "kg", "kgs",
        "lt", "lts", "pqt", "saco", "sacos", "cubeta", "cubetas", "bote",
        "botes", "costal", "costales", "paquete", "paquetes", "atado",
        "atados", "hoja", "hojas", "tira", "tiras",
    }

    # Split on newlines, commas, bullets
    lines = re.split(r"[\n,•·\-*]+", text)
    queries = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Strip leading quantity (e.g. "31 ", "7 ", "1800 ")
        line = re.sub(r"^\d+[\.\,]?\d*\s*", "", line).strip()
        if not line:
            continue
        # Strip noise/packaging words
        tokens = line.lower().split()
        clean_tokens = [t for t in tokens if t not in _NOISE and not re.fullmatch(r"\d+", t)]
        clean = " ".join(clean_tokens).strip()
        if clean and len(clean) >= 2:
            queries.append(clean)

    _log.debug("PREFILTER SEMANTIC: split '%s' → %s", text[:80], queries)
    return queries


def prefilter_catalog_semantic(
    company_id: str,
    text: str,
    catalog: list[dict],
    max_items: int = 80,
    sim_threshold: float = 0.30,
    per_query_limit: int = 15,
) -> list[dict]:
    """
    Prefilter semántico: usa embeddings (pgvector) para encontrar los productos
    más relevantes del catálogo para el mensaje del cliente.

    Ventajas sobre token-based:
    - No requiere jerga map manual — 'bescool' queda cerca de 'basecoat'
    - Escala a cualquier giro sin configuración
    - Captura sinónimos y marcas competidoras automáticamente

    Estrategia:
    1. Divide el mensaje en líneas (un producto por línea)
    2. Embeddea cada línea
    3. Busca top-N productos más similares por línea en pgvector
    4. Unifica resultados, ordena por max similarity
    5. Si hay menos productos de lo esperado, complementa con el prefilter token-based
    """
    import logging
    _log = logging.getLogger("cotizaexpress.llm_parser")

    queries = _split_message_into_queries(text)
    if not queries:
        _log.info("PREFILTER SEMANTIC: no queries extracted, falling back to token-based")
        return prefilter_catalog(catalog, text, max_items)

    try:
        from semantic_search import get_embeddings_batch, build_query_text
        from db import get_conn

        # Build query texts with same normalization as semantic_search
        query_texts = [build_query_text(q) for q in queries]
        # Filter empty
        query_texts = [q for q in query_texts if q.strip()]
        if not query_texts:
            return prefilter_catalog(catalog, text, max_items)

        # Batch embed all queries in one API call
        t0 = time.time()
        vectors = get_embeddings_batch(query_texts)
        embed_ms = int((time.time() - t0) * 1000)
        _log.info("PREFILTER SEMANTIC: embedded %d queries in %dms", len(query_texts), embed_ms)

        # Query pgvector for each embedding
        conn = get_conn()
        cur = conn.cursor()
        # product_name → max_similarity
        product_scores: dict[str, float] = {}
        product_data: dict[str, dict] = {}

        for vec in vectors:
            vector_str = "[" + ",".join(str(x) for x in vec) + "]"
            cur.execute(
                """
                SELECT name, sku, unit, price, vat_rate, is_default, bundle_size,
                       1 - (embedding <=> %s::vector) AS similarity
                FROM pricebook_items
                WHERE company_id = %s AND embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (vector_str, company_id, vector_str, per_query_limit),
            )
            for row in cur.fetchall():
                name, sku, unit, price, vat_rate, is_default, bundle_size, sim = row
                sim = float(sim)
                if sim < sim_threshold:
                    continue
                nk = norm_key(name or "")
                if nk not in product_scores or sim > product_scores[nk]:
                    product_scores[nk] = sim
                    product_data[nk] = {
                        "name": name,
                        "sku": sku,
                        "unit": unit,
                        "price": float(price) if price is not None else None,
                        "vat_rate": float(vat_rate) if vat_rate is not None else None,
                        "is_default": bool(is_default) if is_default is not None else False,
                        "bundle_size": bundle_size,
                    }

        cur.close()
        conn.close()

        # Sort by similarity descending
        sorted_products = sorted(product_scores.items(), key=lambda x: x[1], reverse=True)

        # Build result list
        result = [product_data[nk] for nk, _sim in sorted_products[:max_items]]

        _log.info(
            "PREFILTER SEMANTIC: found %d products (threshold=%.2f), top: %s",
            len(result),
            sim_threshold,
            [(nk, f"{s:.3f}") for nk, s in sorted_products[:5]],
        )

        # If we found very few products, supplement with token-based prefilter
        # to ensure the LLM has enough context
        if len(result) < 20 and len(catalog) > max_items:
            _log.info("PREFILTER SEMANTIC: only %d products, supplementing with token-based", len(result))
            existing_keys = {norm_key(it.get("name") or "") for it in result}
            token_filtered = prefilter_catalog(catalog, text, max_items)
            for item in token_filtered:
                nk = norm_key(item.get("name") or "")
                if nk not in existing_keys:
                    result.append(item)
                    existing_keys.add(nk)
                if len(result) >= max_items:
                    break

        return result[:max_items]

    except Exception as e:
        _log.error("PREFILTER SEMANTIC FAILED: %s — falling back to token-based", repr(e))
        return prefilter_catalog(catalog, text, max_items)


# ---------------------------------------------------------------------------
# Jerga global opcional (ferretería MX)
# ---------------------------------------------------------------------------

JERGA_HINTS = """Jerga típica de ferretería mexicana:
- "tablaroca" = panel de yeso. "panel rey" / "panel de yeso" / "hojas de yeso" / "lamina de yeso" / "tablarock" / "tabla roca" / "tblrc" → tablaroca. "muro" / "muros" / "pared" / "paredes" + material = el cliente quiere las hojas/paneles para construir, NO es un producto llamado "muro". "Quiero muro de panel rey 12mm" = quiero tablaroca 12mm.
- "lightrey" / "light rey" → tablaroca ultralight.
- "tablaroca WR" / "tablaroca RH" / "tablaroca anti moho" / "azul celeste" / "panel rey MR" → tablaroca anti-moho.
- "securock" / "securok" → securock (material distinto a tablaroca).
- "durock" / "duroc" / "durrock" / "permabase" / "perma base" / "permabse" → durock. NOTA: Permabase es la marca competidora (National Gypsum), pero la tienda maneja Durock (USG) como equivalente. Siempre matchea a durock.
- "basecoat" / "base coat" / "basekoat" / "biscoat" / "bescool" / "bescol" / "bescot" / "pasta tablaroca" / "pasta para juntas" / "pasta durock" / "compuesto para juntas" → basecoat.
- "redimix" / "redemix" / "ready mix" / "compuesto std plus" / "compuesto estandar plus" → redimix.
- "perfacinta" / "perfacita" / "prefacinta" / "perfacintas" / "cinta papel" / "cinta de papel usg" / "cinta union" / "cinta de union" → perfacinta usg 75m x 5cm (es el ÚNICO producto de perfacinta — siempre matchea a esa key).
- "cinta fibra" / "cinta de malla" / "cinta maya" / "maya" / "mayas" / "rollos de maya" / "malla para tablaroca" / "malla tablaroca" / "malla durock" → cinta fibra de vidrio. IMPORTANTE: "maya" (con Y) es un error de ortografía de "malla", NUNCA es "plafón".
- "pilas" / "pila" / "pijas" = pijas (tornillos). "pija fremer" / "pija flamer" / "pija frame" / "framer" → pija framer. "pija para durock" / "pilas para durock". "pija para tablaroca" / "tornillo para tablaroca" / "pija 6x1" → pija 6 x 1. "pija 10x1 1/2" / "pija 10x1.5" / "fijasora" / "punta de broca" → pija 10 x 1 1/2.
- "taquete ancla" / "taquete anclo" / "ancla de expansion" → taquete expansion. "taquete un cuarto" / "taquetes 1/4" / "taquetes de un cuarto" → taquete de plástico 1/4".
- "cancel" / "cnal" / "canel" / "canaleta" → canal. "canal de amarre" / "canal amarre" / "canales de amarre" = SIEMPRE es un canal (track de tablaroca), NUNCA es "ángulo de amarre". Elige el canal marcado [DEFAULT] si no especifica medida. "canaleta de carga" / "cargadora" / "canaleta CA" → canaleta de carga.
- "reborde jota" / "revorde j" / "revoque j" / "reborder j" → reborde j galvanizado.
- "postes" / "pste" / "psts". "poste para la reja" / "poste de reja" → poste para rejacero.
- "reja" / "rejas" → rejacero.
- "abrazadera" (sin especificar) → abrazadera para rejacero.
- "galleta" / "plafon galleta" / "plafones registrables" → plafon registrable.
- "PTR" = perfil tubular rectangular (raramente en catálogo; escalar).
- "hoja" / "hojas" sueltas (sin material específico después) = tablaroca. PERO "hoja de securock" = securock, "hoja de durock" = durock. Siempre respeta el material que sigue después de "hoja/hojas de".
- "hojas blancas" / "hojas blancas tablaroca" / "hoja blanca" = tablaroca ultralight (la lámina blanca estándar). NO confundir con securock.
- "tabla WR" / "hoja WR" = tablaroca anti-moho.
- "tramos de madera" / "tiras de madera" / "barrotes" / "listones de madera" → barrote de madera.

RESOLUCIÓN DE AMBIGÜEDADES — USA TU CONOCIMIENTO DE LA INDUSTRIA:
Cuando el cliente NO especifica variante, calibre, medida, etc., NO adivines al azar ni elijas el primero del catálogo. En su lugar, usa tu conocimiento técnico de construcción y ferretería mexicana para inferir la opción correcta, tal como lo haría un vendedor experimentado:
- Analiza el CONTEXTO del pedido completo. Si el cliente pide tablaroca + postes + canales, es un sistema de tablaroca → la estructura es calibre 26. Si pide durock + postes, es sistema durock → calibre 20.
- "Tornillo para tablaroca" = pija 6x1 (punta fina). "Tornillo framer" / "punta broca" = pija framer. "Tornillo para durock" = pija para durock. Esto lo sabe cualquier tablaroquero.
- "Canal de amarre" sin calibre, en contexto de tablaroca = cal 26. En contexto de durock = cal 20.
- "Tiras de madera" = barrotes. "Taquetes un cuarto" = taquete de plástico 1/4".
- Cuando hay varias presentaciones de un producto (ej. Redimix 6kg, 21.8kg, 28kg), elige la que mejor corresponda a la cantidad o descripción del cliente ("cajas de 21kg" → Redimix 21.8 kg).
- IMPORTANTE: Si un producto tiene la marca [DEFAULT] en el catálogo y el cliente NO especifica variante/calibre/tamaño, SIEMPRE elige el [DEFAULT]. Es el producto estándar configurado por el dueño de la tienda.
- Si genuinamente no puedes determinar cuál variante es Y no hay un [DEFAULT] marcado, devuelve key=null y confidence baja para que el bot pregunte al cliente.
- Si el cliente SÍ especifica (ej. "postes cal 20", "canal cal 22"), SIEMPRE respeta lo que pidió, aunque haya otro marcado como [DEFAULT].
- NUNCA combines o sumes cantidades de productos diferentes. "1 hoja de securock" y "4 hojas tablaroca" son DOS líneas separadas (1x securock + 4x tablaroca), NO "5x tablaroca". Cada línea del mensaje del cliente es un producto independiente.
"""


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Eres un parser de órdenes para un distribuidor mexicano de materiales de construcción. Su fuerte es construcción ligera (drywall) con marcas como USG, Saint-Gobain y Panel Rey, pero también vende: rejacero y cercas (malla ciclónica, alambre de púas, poste ganadero), láminas (galvanizada, acero inoxidable, aluminio, policarbonato), pintura (esmalte, vinílica), puertas multipanel, plafones, impermeabilizante, herramienta menor, y accesorios varios. Los clientes típicos son contratistas, instaladores y ferreterías. Recibes mensajes de clientes por WhatsApp y los conviertes en una lista estructurada.

{jerga}

CATÁLOGO DISPONIBLE (formato: "- <key> || <nombre> | <unidad> | <precio>"):
{catalog}

REGLAS:
1. Identifica cada producto que el cliente quiere cotizar. El mensaje puede tener 1 o muchos productos separados por comas, guiones, asteriscos, viñetas, saltos de línea, "y", o listas enumeradas.
2. Para cada producto, elige el <key> del catálogo que mejor corresponde. NUNCA inventes una key que no esté listada arriba.
3. Si el cliente escribe jerga, typos o nombres cortos, usa tu conocimiento de ferretería mexicana y la sección de jerga arriba para identificar el producto correcto.
4. Si NO hay match claro en el catálogo, devuelve key=null y confidence baja (<0.6), con el texto original en matched_text y name. PERO si hay una sola opción posible del tipo de producto (ej. solo existe una perfacinta, un solo basecoat), SÍ devuelve esa key con confidence alta.
5. Cantidad por defecto: 1. Interpreta "2 de cada", "5 paquetes", "10 mts", cantidades al final del producto ("Tornillo 300"), números con decimal como cantidades (175 o 175.00 panel = 175 unidades, NO precio).
6. Si una línea dice "1 ???" o similar (basura), IGNÓRALA por completo.
7. Si el mensaje es un botón de UI, un saludo puro ("hola", "buenos días"), una pregunta de horarios, un número de teléfono, o un "salir" / "cancelar", devuelve items=[] y non_order=true.
8. REGLA CRÍTICA: Si el mensaje contiene un número + nombre de producto (ej. "3 martillos", "5 focos", "10 cables", "2 lámparas"), SIEMPRE es una orden. Devuelve items con key=null si no está en catálogo. NUNCA devuelvas non_order=true cuando hay un patrón de "cantidad + producto", aunque el producto NO esté en el catálogo. El bot necesita saber qué pidió el cliente para buscarlo.
9. Si un spec aparece al final y aplica a varios productos arriba (ej: "Canal 4 y 6.35 cal 26" → cal 26 aplica a ambos), propágalo.
10. Si aparece un forward con saludo de otro proveedor antes de la lista (ej: "Buenos días, seguimos a tus órdenes en Gram-Bel"), ignora el saludo y parsea SOLO la lista de productos.
11. Nombres de proyecto al inicio ("Mat. Privanzas", "Del closet") no son productos, son contexto.
12. PRODUCTOS DEFAULT: Algunos productos tienen la marca [DEFAULT] en el catálogo. Cuando el cliente pide un producto genérico SIN especificar tamaño, calibre, medida, presentación o variante (ej. "poste", "canal", "tablaroca", "redimix"), SIEMPRE elige el producto marcado [DEFAULT] de ese tipo. El [DEFAULT] representa el producto estándar/más vendido que el dueño de la tienda ha marcado como favorito. Solo ignora el [DEFAULT] si el cliente explícitamente pide otra variante (ej. "poste cal 20", "canal 4.10", "tablaroca anti fuego").
13. En matched_text, devuelve SOLO el nombre del producto SIN la cantidad. Ejemplo: si el cliente dice "5 focos", matched_text="focos" (NO "5 focos"). La cantidad va en el campo qty.

OUTPUT: JSON válido, sin markdown, exactamente esta estructura:
{{
  "items": [
    {{
      "matched_text": "fragmento original del mensaje",
      "key": "<key del catálogo o null>",
      "name": "nombre del catálogo si hay key, o nombre libre si key=null",
      "qty": entero o decimal,
      "unit": "pza|mt|kg|lt|pqt|caja|rollo|bulto|bolsa|null",
      "confidence": 0.0 a 1.0,
      "notes": "explicación breve si confidence<0.8 o key=null"
    }}
  ],
  "non_order": false
}}

Si el mensaje NO es una orden, devuelve:
{{"items": [], "non_order": true}}"""


SYSTEM_PROMPT_DYNAMIC = """{system_intro}

Recibes mensajes de clientes por WhatsApp y los conviertes en una lista estructurada.

{jerga}

CATÁLOGO DISPONIBLE (formato: "- <key> || <nombre> | <unidad> | <precio>"):
{catalog}

REGLAS:
1. Identifica cada producto que el cliente quiere cotizar. El mensaje puede tener 1 o muchos productos separados por comas, guiones, asteriscos, viñetas, saltos de línea, "y", o listas enumeradas.
2. Para cada producto, elige el <key> del catálogo que mejor corresponde. NUNCA inventes una key que no esté listada arriba.
3. Si el cliente escribe jerga, typos o nombres cortos, usa tu conocimiento de la industria y la sección de jerga arriba para identificar el producto correcto.
4. Si NO hay match claro en el catálogo, devuelve key=null y confidence baja (<0.6), con el texto original en matched_text y name. PERO si hay una sola opción posible del tipo de producto (ej. solo existe una perfacinta, un solo basecoat), SÍ devuelve esa key con confidence alta.
5. Cantidad por defecto: 1. Interpreta "2 de cada", "5 paquetes", "10 mts", cantidades al final del producto ("Tornillo 300"), números con decimal como cantidades (175 o 175.00 panel = 175 unidades, NO precio).
6. Si una línea dice "1 ???" o similar (basura), IGNÓRALA por completo.
7. Si el mensaje es un botón de UI, un saludo puro ("hola", "buenos días"), una pregunta de horarios, un número de teléfono, o un "salir" / "cancelar", devuelve items=[] y non_order=true.
8. REGLA CRÍTICA: Si el mensaje contiene un número + nombre de producto (ej. "3 martillos", "5 focos", "10 cables"), SIEMPRE es una orden. Devuelve items con key=null si no está en catálogo. NUNCA devuelvas non_order=true cuando hay un patrón de "cantidad + producto".
9. Si un spec aparece al final y aplica a varios productos arriba (ej: "Canal 4 y 6.35 cal 26" → cal 26 aplica a ambos), propágalo.
10. Si aparece un forward con saludo de otro proveedor antes de la lista, ignora el saludo y parsea SOLO la lista de productos.
11. Nombres de proyecto al inicio ("Mat. Privanzas", "Del closet") no son productos, son contexto.
12. PRODUCTOS DEFAULT: Algunos productos tienen la marca [DEFAULT] en el catálogo. Cuando el cliente pide un producto genérico SIN especificar tamaño, calibre, medida, presentación o variante, SIEMPRE elige el producto marcado [DEFAULT] de ese tipo.
13. En matched_text, devuelve SOLO el nombre del producto SIN la cantidad. La cantidad va en qty.

RESOLUCIÓN DE AMBIGÜEDADES — USA TU CONOCIMIENTO DE LA INDUSTRIA:
Cuando el cliente NO especifica variante, calibre, medida, etc., NO adivines al azar. Usa tu conocimiento técnico para inferir la opción correcta, tal como lo haría un vendedor experimentado. Analiza el CONTEXTO del pedido completo.
IMPORTANTE: Si un producto tiene la marca [DEFAULT] y el cliente NO especifica variante, SIEMPRE elige el [DEFAULT].
Si genuinamente no puedes determinar cuál variante es Y no hay un [DEFAULT] marcado, devuelve key=null y confidence baja.
NUNCA combines o sumes cantidades de productos diferentes. Cada línea del mensaje es un producto independiente.

OUTPUT: JSON válido, sin markdown, exactamente esta estructura:
{{{{
  "items": [
    {{{{
      "matched_text": "fragmento original del mensaje",
      "key": "<key del catálogo o null>",
      "name": "nombre del catálogo si hay key, o nombre libre si key=null",
      "qty": entero o decimal,
      "unit": "pza|mt|kg|lt|pqt|caja|rollo|bulto|bolsa|null",
      "confidence": 0.0 a 1.0,
      "notes": "explicación breve si confidence<0.8 o key=null"
    }}}}
  ],
  "non_order": false
}}}}

Si el mensaje NO es una orden, devuelve:
{{{{"items": [], "non_order": true}}}}"""


USER_TEMPLATE = "Mensaje del cliente:\n```\n{text}\n```"


# ---------------------------------------------------------------------------
# Parser principal
# ---------------------------------------------------------------------------

def _estimate_product_lines(text: str) -> int:
    """
    Estimate how many product lines a message contains.
    Used to decide whether to use gpt-4o (large orders) vs gpt-4o-mini (small).
    """
    if not text:
        return 0
    # Split on newlines, commas, bullets, "y" surrounded by spaces
    parts = re.split(r"[\n•·\-*]+|,\s*|\s+y\s+", text)
    # Count lines that look like they have a product (contain a number or 3+ char word)
    count = 0
    for p in parts:
        p = p.strip()
        if not p or len(p) < 3:
            continue
        # Has a quantity or product-like word
        if re.search(r"\d", p) or re.search(r"[a-záéíóúñ]{3,}", p, re.IGNORECASE):
            count += 1
    return count


_LARGE_ORDER_THRESHOLD = 6  # 6+ product lines → use gpt-4o


def llm_parse_order(
    text: str,
    catalog: list[dict],
    *,
    company_id: str | None = None,
    model: str | None = None,
    temperature: float = 0.0,
    timeout: float = 30.0,
    min_confidence: float = 0.7,
    include_jerga_hints: bool = True,
) -> dict:
    """
    Parsea un mensaje del cliente usando LLM con catálogo como contexto.

    Returns:
        {
          "items": [{key, name, qty, unit, confidence, matched_text,
                     notes, needs_escalation}],
          "non_order": bool,
          "raw_response": str,
          "latency_ms": int,
          "model": str,
          "error": str | None,
          "precleaned_text": str,
        }
    """
    # Fast path: botones y saludos puros ni siquiera llegan al LLM
    if is_ui_interaction(text):
        return {
            "items": [],
            "non_order": True,
            "raw_response": "",
            "latency_ms": 0,
            "model": "fast-path",
            "error": None,
            "precleaned_text": text.strip(),
        }

    cleaned = preclean_message(text)
    if not cleaned:
        return {
            "items": [],
            "non_order": True,
            "raw_response": "",
            "latency_ms": 0,
            "model": "fast-path",
            "error": None,
            "precleaned_text": "",
        }

    # Auto-select model based on order complexity
    import logging as _lg
    _log_model = _lg.getLogger("cotizaexpress.llm_parser")
    if model is None:
        _n_lines = _estimate_product_lines(cleaned)
        if _n_lines >= _LARGE_ORDER_THRESHOLD:
            model = "gpt-4o"
            _log_model.info("LARGE ORDER (%d lines) → using gpt-4o for accuracy", _n_lines)
        else:
            model = "gpt-4o-mini"
            _log_model.debug("Small order (%d lines) → using gpt-4o-mini", _n_lines)

    # Semantic prefilter (embedding-based) when company_id available, else token-based
    if company_id:
        filtered = prefilter_catalog_semantic(company_id, cleaned, catalog, max_items=80)
    else:
        filtered = prefilter_catalog(catalog, cleaned, max_items=80)
    catalog_block = format_catalog_for_prompt(filtered)

    # Dynamic LLM context per company (if available), else hardcoded fallback
    _dynamic_ctx = None
    if company_id:
        try:
            from llm_context_generator import get_company_llm_context
            _dynamic_ctx = get_company_llm_context(company_id)
        except Exception as _e:
            import logging as _lg
            _lg.getLogger("cotizaexpress.llm_parser").debug(
                "Dynamic LLM context unavailable: %s", repr(_e)
            )

    if _dynamic_ctx and _dynamic_ctx.get("jerga_hints"):
        # MERGE: hardcoded base jerga + dynamic catalog-specific jerga
        # The hardcoded hints have proven street-level terms (permabase, bescool, maya, muro)
        # The dynamic hints add catalog-specific disambiguation rules
        jerga_block = JERGA_HINTS + "\n\n--- Reglas adicionales por catálogo ---\n\n" + _dynamic_ctx["jerga_hints"]
        # Use dynamic system intro + the standard rules
        system_intro = _dynamic_ctx.get("system_intro", "")
        system = SYSTEM_PROMPT_DYNAMIC.format(
            system_intro=system_intro, jerga=jerga_block, catalog=catalog_block
        )
    else:
        jerga_block = JERGA_HINTS if include_jerga_hints else ""
        system = SYSTEM_PROMPT.format(jerga=jerga_block, catalog=catalog_block)
    user = USER_TEMPLATE.format(text=cleaned)

    # gpt-4o is slower, give it more time
    _effective_timeout = 60.0 if model == "gpt-4o" else timeout

    start = time.time()
    try:
        resp = _get_client().with_options(max_retries=0).chat.completions.create(
            model=model,
            temperature=temperature,
            timeout=_effective_timeout,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        raw = resp.choices[0].message.content or "{}"
    except Exception as e:
        return {
            "items": [],
            "non_order": False,
            "raw_response": "",
            "latency_ms": int((time.time() - start) * 1000),
            "model": model,
            "error": f"{type(e).__name__}: {e}",
            "precleaned_text": cleaned,
        }

    latency_ms = int((time.time() - start) * 1000)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        return {
            "items": [],
            "non_order": False,
            "raw_response": raw,
            "latency_ms": latency_ms,
            "model": model,
            "error": f"JSONDecodeError: {e}",
            "precleaned_text": cleaned,
        }

    valid_keys = {norm_key(it.get("name") or "") for it in catalog}
    valid_keys.discard("")

    items_out: list[dict] = []
    for it in parsed.get("items", []) or []:
        key = it.get("key")
        if key in ("", "null", "None", None):
            key = None
        else:
            key = norm_key(key) if isinstance(key, str) else None
            # Si el LLM se inventó una key no listada → forzar null
            if key and key not in valid_keys:
                key = None

        qty_raw = it.get("qty", 1)
        try:
            qty = float(qty_raw)
            if qty == int(qty):
                qty = int(qty)
        except (TypeError, ValueError):
            qty = 1

        conf = float(it.get("confidence") or 0.0)
        needs_escalation = (key is None) or (conf < min_confidence)

        # Filtra basura "1 ???" que algunos mensajes traen
        mt = (it.get("matched_text") or "").strip()
        if mt in ("???", "1 ???") or re.fullmatch(r"[\W_?]+", mt):
            continue

        items_out.append({
            "key": key,
            "name": (it.get("name") or "").strip(),
            "qty": qty,
            "unit": (it.get("unit") or "").strip() or None,
            "matched_text": mt,
            "confidence": conf,
            "notes": (it.get("notes") or "").strip(),
            "needs_escalation": needs_escalation,
        })

    # ── Post-LLM corrections ──────────────────────────────────────────────
    # Safety net: catch known LLM misparses that slip through despite hints.
    # We check the ORIGINAL user text (before preclean) so we can detect
    # when the user said "canal" but the LLM returned "angulo".
    items_out = _post_correct_items(items_out, text)

    return {
        "items": items_out,
        "non_order": bool(parsed.get("non_order", False)),
        "raw_response": raw,
        "latency_ms": latency_ms,
        "model": model,
        "error": None,
        "precleaned_text": cleaned,
    }


def _post_correct_items(items: list[dict], original_text: str) -> list[dict]:
    """
    Fix known LLM misparses after the fact.

    Two checks:
    1. Specific: "canal de amarre" → never "angulo de amarre"
    2. General: if the user's first noun (e.g. "canal") doesn't match the
       product's first word (e.g. "angulo"), null the key so smart_search
       re-resolves it correctly. This catches ALL product-type swaps, not
       just the ones we know about.
    """
    if not items or not original_text:
        return items

    import logging
    _log = logging.getLogger("cotizaexpress.llm_parser")

    # ── Build a map of what the user actually asked for ────────────────
    # Parse "15 canal de amarre, 24 tablaroca" → {item_index: first_product_word}
    # We match each LLM item's matched_text back to the original to extract
    # the user's intended product type (first non-numeric word).

    # Words that are filler, not product types
    _FILLER = {"de", "del", "la", "las", "los", "el", "un", "una", "unos",
               "unas", "para", "con", "por", "en", "y", "o", "x", "mt",
               "mts", "metro", "metros", "pza", "pzas", "pieza", "piezas",
               "rollo", "rollos", "caja", "cajas", "bolsa", "bolsas",
               "bulto", "bultos", "kg", "kgs", "lt", "lts", "pqt", "cal",
               "calibre", "mm", "cm", "m", "pulgada", "pulgadas"}

    def _first_product_word(text: str) -> str:
        """Extract the first meaningful product-type word from text."""
        text_n = norm_key(text)
        for tok in re.findall(r"[a-z]{3,}", text_n):
            if tok not in _FILLER and not tok.isdigit():
                return tok
        return ""

    corrected = []
    for it in items:
        key = it.get("key") or ""
        name = it.get("name") or ""
        matched = it.get("matched_text") or ""
        conf = it.get("confidence", 0)

        # Only check high-confidence matches — low-confidence already go to fallback
        if not key or conf < 0.7:
            corrected.append(it)
            continue

        # What the user said (from matched_text which is the original fragment)
        user_word = _first_product_word(matched)
        # What the LLM returned (from the catalog product name)
        product_word = _first_product_word(name) or _first_product_word(key)

        if user_word and product_word and user_word != product_word:
            # The user said one product type, the LLM returned a different one.
            # Check if they're known synonyms (e.g. "pija" vs "tornillo")
            if not _are_synonym_types(user_word, product_word):
                _log.warning(
                    "POST-CORRECT TYPE MISMATCH: user said '%s' (word='%s') "
                    "but LLM returned '%s' (word='%s') — nulling key for re-search",
                    matched, user_word, name, product_word
                )
                it["key"] = None
                it["name"] = user_word  # use the user's word for smart_search
                it["notes"] = f"post-corrected: user said '{user_word}', LLM returned '{product_word}'"
                it["confidence"] = 0.5
                it["needs_escalation"] = True

        corrected.append(it)
    return corrected


# Product-type words that are synonyms and should NOT trigger a mismatch correction
# Groups of words that refer to the same product type.
# If user says any word in a group and LLM returns a product whose first word
# is also in that group, it's NOT a mismatch.
_SYNONYM_TYPE_GROUPS = [
    {"pija", "pijas", "tornillo", "tornillos", "pilas", "fijasora"},
    {"taquete", "taquetes", "ancla", "anclas"},
    {"tablaroca", "panel", "hoja", "hojas", "tabla", "lamina", "muro", "muros", "pared"},
    {"barrote", "barrotes", "tira", "tiras", "liston", "listones", "tramos"},
    {"plafon", "plafones", "galleta", "galletas"},
    {"reja", "rejas", "rejacero"},
    {"canal", "canales", "cancel", "canaleta", "canaletas", "cnal", "canel"},
    {"poste", "postes", "pste", "psts"},
    {"redimix", "redemix", "ready"},
    {"perfacinta", "perfacita", "prefacinta", "cinta"},
    {"malla", "mallas", "maya", "mayas"},
    {"basecoat", "pasta", "compuesto", "biscoat", "bescool", "bescol"},
    {"durock", "duroc", "durrock", "permabase", "permabse", "perma"},
    {"securock", "securok"},
    {"abrazadera", "abrazaderas"},
    {"reborde", "revorde", "jota"},
    {"angulo", "angulos"},
    {"pija", "framer", "fremer", "flamer"},
    {"impermeabilizante", "impermeabilizantes", "impermeable"},
    {"pintura", "pinturas", "esmalte", "vinilica"},
]

# Build a lookup: word → set of group indices (a word can be in multiple groups)
_SYNONYM_LOOKUP: dict[str, set[int]] = {}
for _gi, _group in enumerate(_SYNONYM_TYPE_GROUPS):
    for _word in _group:
        _SYNONYM_LOOKUP.setdefault(_word, set()).add(_gi)


def _are_synonym_types(word_a: str, word_b: str) -> bool:
    """Check if two product-type words refer to the same product type."""
    if word_a == word_b:
        return True
    # Strip trailing 's' for basic plural handling
    a_base = word_a.rstrip("s") if len(word_a) > 3 else word_a
    b_base = word_b.rstrip("s") if len(word_b) > 3 else word_b
    if a_base == b_base:
        return True
    # Check if both share any synonym group
    def _groups(w):
        return _SYNONYM_LOOKUP.get(w, set()) | _SYNONYM_LOOKUP.get(
            w.rstrip("s") if len(w) > 3 else w, set()
        )
    return bool(_groups(word_a) & _groups(word_b))


# ---------------------------------------------------------------------------
# Helpers de integración
# ---------------------------------------------------------------------------

def split_found_vs_missing(parsed: dict) -> tuple[list[dict], list[dict]]:
    """Separa items con key confiable vs items que necesitan escalación."""
    found = [it for it in parsed["items"] if not it["needs_escalation"]]
    missing = [it for it in parsed["items"] if it["needs_escalation"]]
    return found, missing


def format_escalation_message(
    missing: list[dict], client_phone: str, client_text: str, company_name: str = ""
) -> str:
    """Mensaje que se manda al dueño cuando hay productos fuera del catálogo."""
    header = f"⚠️ Productos fuera de catálogo{' — ' + company_name if company_name else ''}"
    lines = [
        header,
        f"Cliente: {client_phone}",
        "",
        "Mensaje original:",
        f"```{client_text}```",
        "",
        "No encontrados en catálogo:",
    ]
    for it in missing:
        qty = it["qty"]
        name = it["matched_text"] or it["name"] or "(sin nombre)"
        lines.append(f"• {qty} × {name}")
    return "\n".join(lines)


def catalog_lookup_by_key(catalog: list[dict], key: str) -> dict | None:
    """Encuentra el item del catálogo por key normalizada."""
    k = norm_key(key)
    for item in catalog:
        if norm_key(item.get("name") or "") == k:
            return item
    return None
