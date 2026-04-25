"""
llm_context_generator.py — Genera contexto LLM dinámico por empresa.

En vez de hardcodear jerga hints y system prompts para tablaroca,
genera el contexto basándose en:
  - giro de la empresa (ferretería, plomería, electricidad, etc.)
  - descripción de productos que venden
  - marcas propias y de competencia
  - catálogo real (pricebook_items)

El resultado se guarda en companies.llm_context (JSONB) y se regenera
automáticamente cuando:
  - Se completa el onboarding
  - Se sube un catálogo nuevo
  - Se cambian marcas o giro en settings

Estructura de llm_context:
{
  "system_intro": "Eres un parser... para [tipo de negocio]...",
  "jerga_hints": "Jerga típica de [giro]:\n- ...",
  "generated_at": "2026-04-24T12:00:00",
  "product_count": 150,
  "giro": "Ferretería",
}
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

log = logging.getLogger("cotizaexpress.llm_context")

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

_client = None


def _get_client():
    global _client
    if _client is None:
        if OpenAI is None:
            raise RuntimeError("openai package not installed")
        _client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return _client


# ---------------------------------------------------------------------------
# System intro templates by giro
# ---------------------------------------------------------------------------

_GIRO_INTROS = {
    "ferretería": (
        "Eres un parser de órdenes para una ferretería mexicana. "
        "Vende materiales de construcción, herramientas, "
        "tornillería, material eléctrico, plomería y productos varios. "
        "Los clientes típicos son contratistas, maestros de obra y público general."
    ),
    "materiales de construcción": (
        "Eres un parser de órdenes para un distribuidor mexicano de materiales de construcción. "
        "Maneja productos como cemento, varilla, block, tablaroca, tubería, impermeabilizante, "
        "y materiales varios para obra. Los clientes típicos son contratistas e instaladores."
    ),
    "plomería": (
        "Eres un parser de órdenes para una tienda de plomería en México. "
        "Vende tubería (PVC, cobre, CPVC, galvanizada), conexiones, válvulas, "
        "tinacos, calentadores, llaves, accesorios sanitarios y herramienta especializada. "
        "Los clientes típicos son plomeros, contratistas y ferreterías."
    ),
    "electricidad": (
        "Eres un parser de órdenes para una tienda de material eléctrico en México. "
        "Vende cable, tubería conduit, cajas, interruptores, contactos, centros de carga, "
        "lámparas, focos, cinta aislante y accesorios eléctricos. "
        "Los clientes típicos son electricistas y contratistas."
    ),
    "pinturas": (
        "Eres un parser de órdenes para una tienda de pinturas en México. "
        "Vende pintura vinílica, esmalte, impermeabilizante, selladores, "
        "brochas, rodillos, lijas, masilla y accesorios para pintura. "
        "Los clientes típicos son pintores, contratistas y público general."
    ),
    "herrería": (
        "Eres un parser de órdenes para una herrería o distribuidora de acero en México. "
        "Vende perfil tubular (PTR), ángulo, solera, lámina, varilla, alambre, "
        "soldadura, discos de corte y herramienta para herrería. "
        "Los clientes típicos son herreros, soldadores y talleres metalmecánicos."
    ),
    "refaccionaria": (
        "Eres un parser de órdenes para una refaccionaria en México. "
        "Vende refacciones automotrices, aceites, filtros, balatas, bujías, "
        "bandas, amortiguadores y accesorios para vehículos. "
        "Los clientes típicos son mecánicos y talleres automotrices."
    ),
}

_DEFAULT_INTRO = (
    "Eres un parser de órdenes para un negocio mexicano. "
    "Recibes mensajes de clientes por WhatsApp y los conviertes en una lista estructurada "
    "de productos con cantidades."
)


def _get_system_intro(giro: str, descripcion: str = "", company_name: str = "") -> str:
    """Build the system intro paragraph based on company giro."""
    giro_lower = (giro or "").strip().lower()

    # Try exact match first, then partial
    intro = _GIRO_INTROS.get(giro_lower)
    if not intro:
        for key, val in _GIRO_INTROS.items():
            if key in giro_lower or giro_lower in key:
                intro = val
                break

    if not intro:
        if giro:
            intro = (
                f"Eres un parser de órdenes para un negocio de {giro} en México. "
                "Recibes mensajes de clientes por WhatsApp y los conviertes en una lista estructurada."
            )
        else:
            intro = _DEFAULT_INTRO

    # Add company-specific context
    extras = []
    if company_name:
        extras.append(f"La empresa se llama {company_name}.")
    if descripcion:
        extras.append(f"Productos principales: {descripcion.strip().rstrip('.')}.")

    if extras:
        intro += " " + " ".join(extras)

    return intro


# ---------------------------------------------------------------------------
# Jerga generation via LLM
# ---------------------------------------------------------------------------

_JERGA_GENERATION_PROMPT = """Analiza este catálogo de productos de un negocio mexicano ({giro}) y genera una guía de jerga/slang que los clientes usan por WhatsApp.

CATÁLOGO (nombre | unidad | precio):
{catalog_sample}

{brand_context}

INSTRUCCIONES:
1. Para cada producto o grupo de productos, lista las formas informales, abreviaciones, errores ortográficos comunes y marcas competidoras que un cliente mexicano usaría por WhatsApp.
2. Incluye errores fonéticos comunes del español mexicano (b↔v, s↔z↔c, omisión de acentos, etc.).
3. Si hay marcas de competencia listadas, indica a qué producto del catálogo equivalen.
4. Agrupa por categoría de producto.
5. Incluye reglas de desambiguación (qué hacer cuando el cliente no especifica tamaño, calibre, etc.).
6. Marca con [DEFAULT] los productos genéricos cuando el cliente no especifica variante.

FORMATO de respuesta — texto plano, estilo:
- "nombre informal" / "typo" / "slang" → nombre exacto del catálogo.
- REGLA: cuando el cliente pide X sin especificar, elegir Y.

NO incluyas JSON, solo texto plano con guiones. Sé conciso pero exhaustivo con los typos más comunes."""


def _generate_jerga_hints(
    catalog: list[dict],
    giro: str = "",
    marcas_propias: str = "",
    marcas_competencia: str = "",
    max_catalog_items: int = 200,
) -> str:
    """Use LLM to generate industry-specific jerga hints from the catalog."""
    if not catalog:
        return ""

    # Build catalog sample
    sample = catalog[:max_catalog_items]
    catalog_lines = []
    for item in sample:
        name = (item.get("name") or "").strip()
        unit = (item.get("unit") or "pza").strip()
        price = item.get("price")
        price_str = f"${float(price):,.2f}" if price is not None else "-"
        is_default = " [DEFAULT]" if item.get("is_default") else ""
        catalog_lines.append(f"- {name} | {unit} | {price_str}{is_default}")

    catalog_text = "\n".join(catalog_lines)

    # Brand context
    brand_parts = []
    if marcas_propias:
        brand_parts.append(f"Marcas que maneja la tienda: {marcas_propias}")
    if marcas_competencia:
        brand_parts.append(
            f"Marcas de competencia (el cliente puede pedir estas pero la tienda "
            f"vende el equivalente propio): {marcas_competencia}"
        )
    brand_context = "\n".join(brand_parts) if brand_parts else "No hay información de marcas."

    prompt = _JERGA_GENERATION_PROMPT.format(
        giro=giro or "ferretería general",
        catalog_sample=catalog_text,
        brand_context=brand_context,
    )

    try:
        t0 = time.time()
        resp = _get_client().chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.3,
            timeout=60,
            messages=[
                {"role": "system", "content": "Eres un experto en ferretería y materiales de construcción en México. Conoces toda la jerga, slang, marcas y errores ortográficos comunes de los clientes."},
                {"role": "user", "content": prompt},
            ],
        )
        jerga = resp.choices[0].message.content or ""
        ms = int((time.time() - t0) * 1000)
        log.info("JERGA GENERATION: %d chars in %dms for %d products", len(jerga), ms, len(catalog))
        return jerga.strip()
    except Exception as e:
        log.error("JERGA GENERATION FAILED: %s", repr(e))
        return ""


# ---------------------------------------------------------------------------
# Main: generate + store
# ---------------------------------------------------------------------------

def generate_and_store_llm_context(company_id: str) -> dict:
    """
    Generate dynamic LLM context for a company and store it in DB.

    Pulls giro, descripcion, marcas, and catalog from DB,
    generates system_intro + jerga_hints, stores in companies.llm_context.

    Returns the generated context dict.
    """
    from db import get_conn

    conn = get_conn()
    cur = conn.cursor()

    try:
        # 1. Get company info
        cur.execute(
            """
            SELECT name, giro, descripcion, marcas_propias, marcas_competencia
            FROM companies WHERE id=%s LIMIT 1
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            log.error("LLM CONTEXT: company %s not found", company_id)
            return {}

        company_name, giro, descripcion, marcas_propias, marcas_competencia = row
        giro = (giro or "").strip()
        descripcion = (descripcion or "").strip()
        marcas_propias = (marcas_propias or "").strip()
        marcas_competencia = (marcas_competencia or "").strip()

        # 2. Get catalog
        cur.execute(
            "SELECT name, unit, price, is_default FROM pricebook_items "
            "WHERE company_id=%s AND name IS NOT NULL ORDER BY name",
            (company_id,),
        )
        catalog = [
            {
                "name": r[0],
                "unit": r[1],
                "price": float(r[2]) if r[2] is not None else None,
                "is_default": bool(r[3]) if r[3] is not None else False,
            }
            for r in cur.fetchall()
        ]

        # 3. Generate system intro
        system_intro = _get_system_intro(giro, descripcion, company_name)

        # 4. Generate jerga hints (LLM-powered)
        jerga_hints = ""
        if catalog:
            jerga_hints = _generate_jerga_hints(
                catalog, giro, marcas_propias, marcas_competencia
            )

        # 5. Build context object
        llm_context = {
            "system_intro": system_intro,
            "jerga_hints": jerga_hints,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "product_count": len(catalog),
            "giro": giro,
            "company_name": company_name,
        }

        # 6. Store in DB
        # Ensure column exists
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='companies' AND column_name='llm_context'
                ) THEN
                    ALTER TABLE companies ADD COLUMN llm_context JSONB;
                END IF;
            END $$;
        """)

        cur.execute(
            "UPDATE companies SET llm_context=%s, updated_at=now() WHERE id=%s",
            (json.dumps(llm_context, ensure_ascii=False), company_id),
        )
        conn.commit()

        log.info(
            "LLM CONTEXT GENERATED: company=%s giro='%s' products=%d jerga=%d chars",
            company_id, giro, len(catalog), len(jerga_hints),
        )
        return llm_context

    except Exception as e:
        log.error("LLM CONTEXT GENERATION ERROR: %s", repr(e))
        conn.rollback()
        return {}
    finally:
        cur.close()
        conn.close()


def get_company_llm_context(company_id: str) -> dict | None:
    """
    Load the stored LLM context for a company.
    Returns None if not generated yet.
    """
    from db import get_conn

    conn = get_conn()
    cur = conn.cursor()
    try:
        # Ensure column exists first
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='companies' AND column_name='llm_context'
                ) THEN
                    ALTER TABLE companies ADD COLUMN llm_context JSONB;
                END IF;
            END $$;
        """)
        conn.commit()

        cur.execute(
            "SELECT llm_context FROM companies WHERE id=%s LIMIT 1",
            (company_id,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        ctx = row[0]
        if isinstance(ctx, str):
            return json.loads(ctx)
        return ctx  # already dict from JSONB
    except Exception as e:
        log.error("LLM CONTEXT LOAD ERROR: %s", repr(e))
        return None
    finally:
        cur.close()
        conn.close()
