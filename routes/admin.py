"""
routes/admin.py — Admin endpoints for CotizaExpress.

Embeddings, synonyms, statistics, jerga management, and query logging.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from auth import get_user_from_session, require_company_id
from db import get_conn
from queries import clear_quote_state
from semantic_search import rebuild_embeddings_for_company, auto_generate_context_groups

log = logging.getLogger("cotizaexpress.admin")

router = APIRouter()


# ── Helpers ────────────────────────────────────────────────────────────────

def _norm_name(s: str) -> str:
    """Normalize name: strip, lowercase, collapse whitespace."""
    return " ".join((s or "").strip().lower().split())


def _require_admin(request: Request):
    """Check that logged-in user is an admin. Lazy-import ADMIN_EMAILS to avoid circular deps."""
    from server import ADMIN_EMAILS
    u = get_user_from_session(request)
    if u["email"].lower() not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="No autorizado")
    return u


def _get_openai_client():
    """Lazy getter for openai_client to avoid circular deps."""
    from server import openai_client
    return openai_client


def _auto_plural_singular(name: str) -> list:
    """Disabled: auto-plurals cause more harm than good."""
    return []


def _is_junk_synonym(syn: str, product_name: str) -> bool:
    """Detect garbage synonyms that should be removed."""
    s = syn.strip().lower()
    pn = product_name.strip().lower()
    pn_tokens = set(_norm_name(pn).split())

    # Broken: contains parentheses, empty, too short
    if not s or len(s) < 3:
        return True
    if "(" in s or ")" in s:
        return True

    # Single-word synonym that's just a plural/singular of a product name token
    if " " not in s:
        # Check if it's a trivial inflection of any token in the name
        for tok in pn_tokens:
            # "laminas" from "lamina", "galvanizadas" from "galvanizado", etc.
            if s == tok:
                return True  # exact duplicate of name token
            # s is tok+"s", tok+"es", or tok minus "s"/"es"
            if s == tok + "s" or s == tok + "es":
                return True
            if tok == s + "s" or tok == s + "es":
                return True
            if s.endswith("es") and s[:-2] == tok:
                return True
            if tok.endswith("es") and tok[:-2] == s:
                return True
            if s.endswith("s") and s[:-1] == tok:
                return True
            if tok.endswith("s") and tok[:-1] == s:
                return True
            # "galvanizadas" ↔ "galvanizados" (gender swap)
            if len(s) > 4 and len(tok) > 4:
                if s[:-1] == tok[:-1] and s[-1] in "aeos" and tok[-1] in "aeos":
                    return True

    return False


# ── Pydantic models ────────────────────────────────────────────────────────

class AdminJergaUpdate(BaseModel):
    termino_original: str
    termino_normalizado: Optional[str] = None
    is_protected: Optional[bool] = None
    industry: Optional[str] = None


class AdminJergaCreate(BaseModel):
    termino_original: str
    termino_normalizado: str
    industry: Optional[str] = None


class JergaLocalBody(BaseModel):
    termino_original: str
    termino_normalizado: str


class BrandSuggestBody(BaseModel):
    marcas_propias: str = ""
    giro: str = ""


# ── Embeddings endpoints ────────────────────────────────────────────────────

@router.post("/api/admin/rebuild-embeddings")
def rebuild_embeddings_endpoint(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    """Re-generate embeddings for all products of a company."""
    conn = get_conn()
    conn.autocommit = False
    try:
        result = rebuild_embeddings_for_company(conn, company_id)
        conn.commit()
        # Auto-generate context groups after rebuilding embeddings
        try:
            cg_result = auto_generate_context_groups(conn, company_id)
            log.debug("AUTO CONTEXT GROUPS after rebuild: %s", cg_result.get('status'))
        except Exception as cge:
            log.error("AUTO CONTEXT GROUPS ERROR: %s", repr(cge))
        return {"ok": True, **result}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@router.post("/api/admin/generate-context-groups")
def generate_context_groups_endpoint(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    """Generate context groups for a company using LLM clustering."""
    conn = get_conn()
    try:
        result = auto_generate_context_groups(conn, company_id)
        return {"ok": result.get("status") == "ok", **result}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# ── Synonyms endpoints ─────────────────────────────────────────────────────

@router.post("/api/admin/rebuild-synonyms-public")
def rebuild_synonyms_public(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, name, synonyms FROM pricebook_items WHERE company_id=%s", (company_id,))
        rows = cur.fetchall()
        updated = 0
        for item_id, name, synonyms in rows:
            existing = (synonyms or "").strip()
            auto_vars = _auto_plural_singular(name)
            if auto_vars:
                existing_set = {s.strip().lower() for s in existing.split(",") if s.strip()}
                new_vars = [v for v in auto_vars if v not in existing_set]
                if new_vars:
                    new_synonyms = (existing + ", " + ", ".join(new_vars)).strip(", ")
                    cur.execute(
                        "UPDATE pricebook_items SET synonyms=%s, updated_at=now() WHERE id=%s",
                        (new_synonyms, item_id)
                    )
                    updated += 1
        return {"ok": True, "total": len(rows), "updated": updated}
    finally:
        cur.close()
        conn.close()


@router.get("/api/admin/synonyms-audit")
def synonyms_audit(company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75"):
    """List all products with their synonyms for auditing"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT name, synonyms FROM pricebook_items WHERE company_id=%s ORDER BY name",
            (company_id,)
        )
        rows = cur.fetchall()
        result = []
        total_with_syn = 0
        total_without_syn = 0
        for name, synonyms in rows:
            syn = (synonyms or "").strip()
            if syn:
                total_with_syn += 1
                result.append({"name": name, "synonyms": syn})
            else:
                total_without_syn += 1
        return {
            "total": len(rows),
            "with_synonyms": total_with_syn,
            "without_synonyms": total_without_syn,
            "items": result
        }
    finally:
        cur.close()
        conn.close()


@router.post("/api/admin/synonyms-clean")
def synonyms_clean(
    company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75",
    dry_run: bool = True
):
    """
    Clean garbage synonyms from pricebook_items.

    Removes:
    1. Broken auto-plurals (parentheses, too short, etc.)
    2. Single-word trivial inflections of product name tokens
    3. Duplicate synonyms that appear in 10+ products (too generic to help)

    Keeps:
    - Multi-word jerga phrases (e.g. "cinta para durock", "teja roja")
    - Unique alternative names

    Pass dry_run=false to actually update the database.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, synonyms FROM pricebook_items WHERE company_id=%s AND synonyms IS NOT NULL AND synonyms != '' ORDER BY name",
            (company_id,)
        )
        rows = cur.fetchall()

        # First pass: count synonym frequency across products
        syn_frequency = {}
        for item_id, name, synonyms in rows:
            for s in (synonyms or "").split(","):
                s = s.strip().lower()
                if s:
                    syn_frequency[s] = syn_frequency.get(s, 0) + 1

        # Second pass: clean each product
        changes = []
        total_removed = 0
        total_kept = 0
        removed_examples = []

        for item_id, name, synonyms in rows:
            original_syns = [s.strip() for s in (synonyms or "").split(",") if s.strip()]
            kept = []
            removed = []

            for syn in original_syns:
                sl = syn.strip().lower()
                reason = None

                if _is_junk_synonym(syn, name):
                    reason = "junk_inflection"
                elif syn_frequency.get(sl, 0) >= 10:
                    reason = "too_generic(appears_in_%d_products)" % syn_frequency[sl]

                if reason:
                    removed.append({"synonym": syn, "reason": reason})
                    total_removed += 1
                else:
                    kept.append(syn)
                    total_kept += 1

            if removed:
                new_synonyms = ", ".join(kept) if kept else ""
                changes.append({
                    "id": item_id,
                    "name": name,
                    "original": synonyms,
                    "cleaned": new_synonyms,
                    "removed": removed,
                    "kept": kept
                })
                if len(removed_examples) < 30:
                    removed_examples.append({
                        "product": name,
                        "removed": [r["synonym"] + " (%s)" % r['reason'] for r in removed],
                        "kept": kept
                    })

        # Apply changes if not dry_run
        updated = 0
        if not dry_run:
            for change in changes:
                cur.execute(
                    "UPDATE pricebook_items SET synonyms=%s, updated_at=NOW() WHERE id=%s",
                    (change["cleaned"], change["id"])
                )
                updated += 1
            conn.commit()

        return {
            "dry_run": dry_run,
            "total_products_analyzed": len(rows),
            "products_with_changes": len(changes),
            "total_synonyms_removed": total_removed,
            "total_synonyms_kept": total_kept,
            "updated_in_db": updated,
            "examples": removed_examples,
            "hint": "Pass dry_run=false to apply changes" if dry_run else "Changes applied! Run rebuild-embeddings next."
        }
    finally:
        cur.close()
        conn.close()


@router.post("/api/admin/set-bundle-size")
def set_bundle_size(
    company_id: str = "30208e3c-70c6-4203-97d9-172fad7d3c75",
    name_contains: str = "",
    bundle_size: int = 12,
    dry_run: bool = True
):
    """Set bundle_size for products matching name_contains. E.g. name_contains=poste&bundle_size=12"""
    if not name_contains:
        raise HTTPException(status_code=400, detail="name_contains requerido")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, bundle_size FROM pricebook_items WHERE company_id=%s AND lower(name) LIKE lower(%s) ORDER BY name",
            (company_id, "%%%s%%" % name_contains)
        )
        rows = cur.fetchall()
        results = []
        for item_id, name, current_bs in rows:
            results.append({"id": item_id, "name": name, "old_bundle_size": current_bs, "new_bundle_size": bundle_size})
            if not dry_run:
                cur.execute("UPDATE pricebook_items SET bundle_size=%s WHERE id=%s", (bundle_size, item_id))
        if not dry_run:
            conn.commit()
        return {
            "dry_run": dry_run,
            "matched": len(results),
            "bundle_size": bundle_size,
            "products": results,
            "hint": "Pass dry_run=false to apply" if dry_run else "Done!"
        }
    finally:
        cur.close()
        conn.close()


@router.get("/api/_version")
def get_version():
    """Version endpoint."""
    return {"version": "1.0.0"}


# ── Quote state endpoints ──────────────────────────────────────────────────

@router.delete("/api/admin/quote-state/{wa_from}")
def admin_clear_quote_state(wa_from: str, request: Request):
    """Clear a stuck quote state for a given WhatsApp number."""
    _require_admin(request)
    # Use Aceromax company_id by default (only tenant currently)
    company_id = "30208e3c-70c6-4203-97d9-172fad7d3c75"
    clear_quote_state(company_id, wa_from)
    return {"ok": True, "cleared": wa_from}


# ── Statistics endpoints ───────────────────────────────────────────────────

@router.get("/api/admin/stats/overview")
def admin_stats_overview(request: Request):
    """Dashboard overview: totales, búsquedas recientes, tasa de éxito."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Total companies activas (con productos)
        cur.execute("SELECT COUNT(DISTINCT company_id) FROM pricebook_items")
        total_companies = cur.fetchone()[0]

        # Total productos
        cur.execute("SELECT COUNT(*) FROM pricebook_items")
        total_products = cur.fetchone()[0]

        # Query events stats (últimos 7 días)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE search_status = 'found') as found,
                COUNT(*) FILTER (WHERE search_status = 'ambiguous') as ambiguous,
                COUNT(*) FILTER (WHERE search_status = 'not_found') as not_found
            FROM query_events
            WHERE created_at > NOW() - INTERVAL '7 days'
        """)
        row = cur.fetchone()
        total_queries = row[0] or 0
        found_queries = row[1] or 0
        ambiguous_queries = row[2] or 0
        not_found_queries = row[3] or 0
        success_rate = round(found_queries / max(total_queries, 1) * 100, 1)

        # Queries hoy
        cur.execute("""
            SELECT COUNT(*) FROM query_events
            WHERE created_at > NOW() - INTERVAL '1 day'
        """)
        queries_today = cur.fetchone()[0] or 0

        # Total jerga global
        cur.execute("SELECT COUNT(*) FROM diccionario_jerga_global")
        total_jerga = cur.fetchone()[0]

        # Jerga auto-promovida
        cur.execute("""
            SELECT COUNT(*) FROM diccionario_jerga_global
            WHERE is_protected = TRUE AND source != 'seed'
        """)
        auto_promoted = cur.fetchone()[0]

        # Total conversaciones WhatsApp
        cur.execute("SELECT COUNT(*) FROM conversations")
        total_conversations = cur.fetchone()[0]

        return {
            "ok": True,
            "stats": {
                "total_companies": total_companies,
                "total_products": total_products,
                "total_conversations": total_conversations,
                "total_jerga": total_jerga,
                "auto_promoted_jerga": auto_promoted,
                "queries_7d": {
                    "total": total_queries,
                    "found": found_queries,
                    "ambiguous": ambiguous_queries,
                    "not_found": not_found_queries,
                    "success_rate": success_rate,
                },
                "queries_today": queries_today,
            },
        }
    except Exception as e:
        log.error("ADMIN STATS ERROR: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.get("/api/admin/stats/top-errors")
def admin_top_errors(request: Request, days: int = 7, limit: int = 20):
    """Top búsquedas que terminaron en not_found — dónde falla el bot."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                original_text,
                normalized_text,
                normalization_source,
                COUNT(*) as count,
                MIN(created_at) as first_seen,
                MAX(created_at) as last_seen
            FROM query_events
            WHERE search_status = 'not_found'
              AND created_at > NOW() - INTERVAL '%s days'
            GROUP BY original_text, normalized_text, normalization_source
            ORDER BY count DESC
            LIMIT %s
        """, (days, limit))
        rows = cur.fetchall()
        errors = []
        for r in rows:
            errors.append({
                "original_text": r[0],
                "normalized_text": r[1],
                "normalization_source": r[2],
                "count": r[3],
                "first_seen": r[4].isoformat() if r[4] else None,
                "last_seen": r[5].isoformat() if r[5] else None,
            })
        return {"ok": True, "errors": errors}
    except Exception as e:
        log.error("ADMIN TOP ERRORS: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.get("/api/admin/stats/top-searches")
def admin_top_searches(request: Request, days: int = 7, limit: int = 30):
    """Top búsquedas exitosas — qué piden más los clientes."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                original_text,
                normalized_text,
                matched_item_name,
                search_paso,
                COUNT(*) as count,
                ROUND(AVG(confidence_score)::numeric, 2) as avg_confidence
            FROM query_events
            WHERE search_status = 'found'
              AND created_at > NOW() - INTERVAL '%s days'
            GROUP BY original_text, normalized_text, matched_item_name, search_paso
            ORDER BY count DESC
            LIMIT %s
        """, (days, limit))
        rows = cur.fetchall()
        searches = []
        for r in rows:
            searches.append({
                "original_text": r[0],
                "normalized_text": r[1],
                "matched_item": r[2],
                "paso": r[3],
                "count": r[4],
                "avg_confidence": float(r[5]) if r[5] else None,
            })
        return {"ok": True, "searches": searches}
    except Exception as e:
        log.info("ADMIN TOP SEARCHES: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.get("/api/admin/stats/by-company")
def admin_stats_by_company(request: Request, days: int = 7):
    """Stats por empresa — quién usa más, quién tiene más errores."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                qe.company_id,
                c.name as company_name,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE qe.search_status = 'found') as found,
                COUNT(*) FILTER (WHERE qe.search_status = 'not_found') as not_found,
                COUNT(*) FILTER (WHERE qe.search_status = 'ambiguous') as ambiguous
            FROM query_events qe
            LEFT JOIN companies c ON c.id = qe.company_id
            WHERE qe.created_at > NOW() - INTERVAL '%s days'
            GROUP BY qe.company_id, c.name
            ORDER BY total DESC
        """, (days,))
        rows = cur.fetchall()
        companies = []
        for r in rows:
            total = r[2] or 1
            companies.append({
                "company_id": str(r[0]),
                "name": r[1] or "Sin nombre",
                "total": r[2],
                "found": r[3],
                "not_found": r[4],
                "ambiguous": r[5],
                "success_rate": round((r[3] or 0) / total * 100, 1),
            })
        return {"ok": True, "companies": companies}
    except Exception as e:
        log.info("ADMIN BY COMPANY: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ── Global Jerga endpoints ────────────────────────────────────────────────

@router.get("/api/admin/jerga")
def admin_jerga_list(request: Request, page: int = 1, per_page: int = 50):
    """Lista la jerga global con stats de uso."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        offset = (max(page, 1) - 1) * per_page

        cur.execute("SELECT COUNT(*) FROM diccionario_jerga_global")
        total = cur.fetchone()[0]

        cur.execute("""
            SELECT
                termino_original,
                termino_normalizado,
                is_protected,
                usage_count,
                success_count,
                CASE WHEN usage_count > 0
                     THEN ROUND((success_count::numeric / usage_count) * 100, 1)
                     ELSE 0 END as confidence,
                industry,
                source
            FROM diccionario_jerga_global
            ORDER BY usage_count DESC, termino_original ASC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        rows = cur.fetchall()
        jerga = []
        for r in rows:
            jerga.append({
                "termino_original": r[0],
                "termino_normalizado": r[1],
                "is_protected": r[2],
                "usage_count": r[3],
                "success_count": r[4],
                "confidence": float(r[5]) if r[5] else 0,
                "industry": r[6],
                "source": r[7],
            })
        return {"ok": True, "jerga": jerga, "total": total, "page": page, "per_page": per_page}
    except Exception as e:
        log.info("ADMIN JERGA LIST: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.put("/api/admin/jerga")
def admin_jerga_update(request: Request, body: AdminJergaUpdate):
    """Actualizar/proteger/corregir un término de jerga."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        updates = []
        params = []
        if body.termino_normalizado is not None:
            updates.append("termino_normalizado = %s")
            params.append(body.termino_normalizado.strip().lower())
        if body.is_protected is not None:
            updates.append("is_protected = %s")
            params.append(body.is_protected)
        if body.industry is not None:
            updates.append("industry = %s")
            params.append(body.industry.strip() or None)
        if not updates:
            raise HTTPException(status_code=400, detail="Nada que actualizar")
        updates.append("source = 'admin'")
        params.append(body.termino_original.strip().lower())
        cur.execute(
            "UPDATE diccionario_jerga_global SET %s WHERE termino_original = %%s" % (", ".join(updates),),
            tuple(params),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Término no encontrado")
        return {"ok": True, "updated": body.termino_original}
    except HTTPException:
        raise
    except Exception as e:
        log.info("ADMIN JERGA UPDATE: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.post("/api/admin/jerga")
def admin_jerga_create(request: Request, body: AdminJergaCreate):
    """Crear nuevo término de jerga protegido manualmente."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO diccionario_jerga_global "
            "(termino_original, termino_normalizado, is_protected, source, industry) "
            "VALUES (%s, %s, TRUE, 'admin', %s) "
            "ON CONFLICT (termino_original) DO UPDATE "
            "SET termino_normalizado = EXCLUDED.termino_normalizado, "
            "    is_protected = TRUE, source = 'admin', "
            "    industry = EXCLUDED.industry",
            (body.termino_original.strip().lower(),
             body.termino_normalizado.strip().lower(),
             (body.industry or "").strip() or None),
        )
        return {"ok": True, "created": body.termino_original}
    except Exception as e:
        log.info("ADMIN JERGA CREATE: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.delete("/api/admin/jerga/{termino}")
def admin_jerga_delete(request: Request, termino: str):
    """Eliminar término de jerga global."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM diccionario_jerga_global WHERE termino_original = %s",
            (termino.strip().lower(),),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Término no encontrado")
        return {"ok": True, "deleted": termino}
    except HTTPException:
        raise
    except Exception as e:
        log.info("ADMIN JERGA DELETE: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ── Per-tenant jerga local (equivalencias de marca / sinónimos locales) ─────

@router.get("/api/company/jerga")
def company_jerga_list(request: Request):
    """Lista la jerga local del tenant (equivalencias de marcas, sinónimos propios)."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT termino_original, termino_normalizado, source, usage_count "
            "FROM diccionario_jerga_local WHERE company_id = %s "
            "ORDER BY termino_original",
            (company_id,),
        )
        rows = cur.fetchall()
        return {"ok": True, "jerga": [
            {"termino_original": r[0], "termino_normalizado": r[1],
             "source": r[2] or "manual", "usage_count": r[3] or 0}
            for r in rows
        ]}
    except Exception as e:
        log.info("JERGA LOCAL LIST: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.post("/api/company/suggest-brands")
def suggest_competitor_brands(request: Request, body: BrandSuggestBody):
    """Use AI to suggest competitor brands based on the tenant's own brands."""
    require_company_id(request)
    openai_client = _get_openai_client()
    if not openai_client:
        return {"suggestions": []}
    marcas = (body.marcas_propias or "").strip()
    giro = (body.giro or "").strip()
    if not marcas and not giro:
        return {"suggestions": []}
    try:
        prompt = (
            "Eres experto en el mercado de materiales de construcción, ferretería y distribución en México.\n\n"
            "Este negocio vende estas marcas: %s\n" % marcas
        )
        if giro:
            prompt += "Giro del negocio: %s\n" % giro
        prompt += (
            "\nPara CADA marca que vende, lista las marcas COMPETIDORAS directas en México "
            "(marcas que venden productos equivalentes/similares). "
            "Incluye también los nombres comerciales de productos específicos de esas marcas "
            "que los clientes podrían usar como sinónimo.\n\n"
            "Ejemplos:\n"
            "- USG/Tablaroca → competidores: Panel Rey (productos: Lightrey, MR Panel Rey, Volcanrey)\n"
            "- Redimix USG → competidores: Knauf (Readyfix), Panel Rey (Compuesto PR)\n"
            "- Coflex → competidores: Rugo, Nacobre\n"
            "- Truper → competidores: Surtej, Pretul, Surtek\n\n"
            "Responde SOLO con una lista de marcas/nombres separados por coma, sin explicaciones. "
            "No repitas las marcas que el negocio ya vende. "
            "Máximo 15 sugerencias, las más relevantes primero."
        )
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=200,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Parse comma-separated list, clean up
        suggestions = [s.strip() for s in raw.split(",") if s.strip()]
        # Remove any that match the tenant's own brands
        _own = set(m.strip().lower() for m in marcas.split(",") if m.strip())
        suggestions = [s for s in suggestions if s.lower() not in _own]
        return {"suggestions": suggestions[:15]}
    except Exception as e:
        log.error("BRAND SUGGEST ERROR: %s", repr(e))
        return {"suggestions": []}


@router.post("/api/company/jerga")
def company_jerga_create(request: Request, body: JergaLocalBody):
    """Crear/actualizar equivalencia local."""
    company_id = require_company_id(request)
    orig = body.termino_original.strip().lower()
    norm = body.termino_normalizado.strip()
    if not orig or not norm:
        raise HTTPException(status_code=400, detail="Ambos campos son requeridos")
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO diccionario_jerga_local "
            "(company_id, termino_original, termino_normalizado, source, usage_count) "
            "VALUES (%s, %s, %s, 'manual', 0) "
            "ON CONFLICT (company_id, termino_original) DO UPDATE "
            "SET termino_normalizado = EXCLUDED.termino_normalizado, source = 'manual'",
            (company_id, orig, norm),
        )
        return {"ok": True, "termino_original": orig, "termino_normalizado": norm}
    except Exception as e:
        log.info("JERGA LOCAL CREATE: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.delete("/api/company/jerga/{termino}")
def company_jerga_delete(request: Request, termino: str):
    """Eliminar equivalencia local."""
    company_id = require_company_id(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM diccionario_jerga_local WHERE company_id = %s AND termino_original = %s",
            (company_id, termino.strip().lower()),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Término no encontrado")
        return {"ok": True, "deleted": termino}
    except HTTPException:
        raise
    except Exception as e:
        log.info("JERGA LOCAL DELETE: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ── Query log endpoint ──────────────────────────────────────────────────────

@router.get("/api/admin/query-log")
def admin_query_log(request: Request, days: int = 1, limit: int = 100,
                    status: Optional[str] = None, company_id: Optional[str] = None):
    """Log detallado de búsquedas recientes."""
    _require_admin(request)
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        where = ["qe.created_at > NOW() - INTERVAL '%s days'"]
        params = [days]
        if status:
            where.append("qe.search_status = %s")
            params.append(status)
        if company_id:
            where.append("qe.company_id = %s::uuid")
            params.append(company_id)
        params.append(limit)

        cur.execute("""
            SELECT
                qe.original_text,
                qe.cleaned_text,
                qe.normalized_text,
                qe.normalization_source,
                qe.matched_item_name,
                qe.search_status,
                qe.search_paso,
                qe.confidence_score,
                qe.created_at,
                c.name as company_name
            FROM query_events qe
            LEFT JOIN companies c ON c.id = qe.company_id
            WHERE %s
            ORDER BY qe.created_at DESC
            LIMIT %%s
        """ % (" AND ".join(where),), tuple(params))
        rows = cur.fetchall()
        events = []
        for r in rows:
            events.append({
                "original_text": r[0],
                "cleaned_text": r[1],
                "normalized_text": r[2],
                "normalization_source": r[3],
                "matched_item": r[4],
                "status": r[5],
                "paso": r[6],
                "confidence": float(r[7]) if r[7] else None,
                "created_at": r[8].isoformat() if r[8] else None,
                "company": r[9],
            })
        return {"ok": True, "events": events}
    except Exception as e:
        log.info("ADMIN QUERY LOG: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
