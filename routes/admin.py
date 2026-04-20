"""
routes/admin.py — Admin endpoints for CotizaExpress.

Embeddings, synonyms, statistics, jerga management, and query logging.
"""

import base64
import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from auth import get_user_from_session, require_company_id
from db import get_conn
from queries import clear_quote_state
from semantic_search import rebuild_embeddings_for_company, auto_generate_context_groups
from whatsapp_api import update_wa_profile_photo

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


# ── WhatsApp Profile Photo Upload ────────────────────────────────────────

_ACEROMAX_LOGO_B64 = "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ra197733bL+64YYbBg0a1PG3/fr1u/XWW20vW7FiRfs40St38tlnn23cuLF95k5Dz1cuHx+f66+/XlGUS5cu2ZZ4clldXRh2OH7NGFGqrtTV1f35z38uKCjo6gXp6em230ZERBg6ehuaEcDuzfbdHRISYlu5qCt+fn7Dhw9XFCUtLa3bWfVbW1uLiorWrl37+eefK4piMpluvvlmVytku+joaNvyutnZ2ba5M9stXLhw9uzZiqIUFha++uqrGzZsKCgoaGpqam5uLioqSk1NfeWVV2wVkenTpy9evPjKnc+ZM8dWxSkuLn711Ve//PLL06dP19TUtLW11dbWnjt37rvvvnvllVc6RZGh5yvdjBkzbOtSqHnILZGdC8MOx68ZI0p1VVar9fjx42+88cbvf//7HTt2XLhwob6+3mKx1NfXnz179osvvli5cqWix98vjMMzYDd24cIF20KBY8eO7XYAa2Ji4pkzZ+rq6jIyMjpNxGNn5h1fX9+77rrL1m52JftT9txyyy0LFizQq5B23HjjjRkZGW1tbWvWrBk9enTHo9xxxx0RERFr166tq6vbsGHDlfNI+Pn53XDDDXPnzrWz8+jo6O+++668vHzXrl22bqWdDBgw4JZbbrEN7nTC+dp/24OCgt544w2Vu9LAw8Pjxhtv/OSTT4zYub6nZufCsMPxa8bBUom+Cbm5ubYeYVfy9/e/6667WJHQZRHAbqx9WVk1s0WOGzfuq6++UhRl79699r/rPT09/f39Bw0alJCQMH36dEcan40rZEehoaG2ETKFhYX79++fPn16x9/OnDlz8uTJaWlpp06dKigoqK2ttVqtgYGB4eHho0aNmjp1arczH02YMGHcuHEZGRknT548f/58TU1NQ0ODj49P3759hwwZkpiYOHLkyPavUSecr3QTJkyIjY09f/687IJ0w/6FYYfj14wRpeokMDDwpZdesqVvfn5+TU1NbW1tY2Oj2WwOCgqKiIgYOXLk5MmTHZnKDUYz2V+X1NbYYr+fPQAA6KTbAOUZMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgAQEMAAAEhDAAABIQAADACABAQwAgATdBHBMTIyiKHl5eU4pDAAAPYEtN20Z2hVqwAAASEAAAwAgQfcBTCs0AADqqWl/VqgBAwAghaoAphIMAIAaKqu/imgNmAwGAKArQimpNoDVhDkAAFCZmAI1YBqiAQDoivrGZxuxJmgyGACAK4mmr6IoJqvVqu0wokcCAKDn0ZyJWoYhtR+DqjAAoDdzpEaqpQbc6ajaDgwAgPtyPAS1B/CVJdBcCAAA3IKOqedoANvQFg0A6FUcr3DqE8DtSGIAQA+mY0OvzgEMAADUYDEGAAAkIIABAJCAAAYAQAICGAAACQhgAAAkIIABAJCAAAYAQAICGAAACf4/g1N/VUEcL3sAAAAASUVORK5CYII="

@router.get("/admin/set-wa-profile-photo")
async def set_wa_profile_photo(request: Request):
    """One-time endpoint to set WhatsApp Business profile photo via Graph API.
    Uses the embedded Aceromax logo as a fallback.
    """
    user = get_user_from_session(request)
    company_id = require_company_id(user)

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT wa_api_key, wa_phone_number_id, logo_url FROM companies WHERE id=%s",
            (company_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")

        wa_token, phone_number_id, logo_url = row[0], row[1], row[2]
        if not wa_token or not phone_number_id:
            raise HTTPException(status_code=400, detail="WhatsApp not configured")

        # Use stored logo if available, otherwise fall back to embedded Aceromax logo
        if logo_url and logo_url.startswith("data:"):
            # Parse data URL: data:image/png;base64,xxxxx
            header, b64_data = logo_url.split(",", 1)
            mime_type = header.split(":")[1].split(";")[0]
            img_bytes = base64.b64decode(b64_data)
        else:
            img_bytes = base64.b64decode(_ACEROMAX_LOGO_B64)
            mime_type = "image/png"

        log.info("SET-WA-PROFILE: Uploading for company=%s phone=%s", company_id, phone_number_id)
        result = update_wa_profile_photo(wa_token, phone_number_id, img_bytes, mime_type)
        return result

    except HTTPException:
        raise
    except Exception as e:
        log.error("SET-WA-PROFILE: Error: %s", repr(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()
