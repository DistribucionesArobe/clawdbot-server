"""
migrations.py — Idempotent database migrations for CotizaExpress.

Run at startup to ensure schema is up to date.
"""

import logging

log = logging.getLogger("cotizaexpress.migrations")


def run_pricebook_migrations(conn):
    """Idempotent DB migrations for pricebook."""
    cur = conn.cursor()
    try:
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='pricebook_items' AND column_name='bundle_size'
                ) THEN
                    ALTER TABLE pricebook_items ADD COLUMN bundle_size INTEGER;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='companies' AND column_name='context_groups'
                ) THEN
                    ALTER TABLE companies ADD COLUMN context_groups JSONB;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='pricebook_items' AND column_name='is_default'
                ) THEN
                    ALTER TABLE pricebook_items ADD COLUMN is_default BOOLEAN DEFAULT FALSE;
                END IF;
            END $$;
        """)
        conn.commit()
        log.debug("PRICEBOOK MIGRATIONS: OK (bundle_size, context_groups, is_default)")
    except Exception as e:
        log.error("PRICEBOOK MIGRATION ERROR: %s", repr(e))
        conn.rollback()
    finally:
        cur.close()


def run_promo_codes_migration(conn):
    """Create promo_codes tables + trial_end column (idempotent)."""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                code        TEXT NOT NULL UNIQUE,
                discount_type TEXT NOT NULL DEFAULT 'trial_days',
                discount_value NUMERIC NOT NULL DEFAULT 10,
                max_uses    INT DEFAULT NULL,
                times_used  INT NOT NULL DEFAULT 0,
                one_per_customer BOOLEAN NOT NULL DEFAULT TRUE,
                active      BOOLEAN NOT NULL DEFAULT TRUE,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at  TIMESTAMPTZ DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS promo_code_uses (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                promo_code_id UUID NOT NULL REFERENCES promo_codes(id),
                company_id  UUID NOT NULL,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_promo_code_uses_company
                ON promo_code_uses(company_id);
            CREATE INDEX IF NOT EXISTS idx_promo_code_uses_code
                ON promo_code_uses(promo_code_id);
        """)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'companies' AND column_name = 'trial_end'
                ) THEN
                    ALTER TABLE companies ADD COLUMN trial_end TIMESTAMPTZ DEFAULT NULL;
                END IF;
            END $$;
        """)
        log.info("PROMO_CODES MIGRATION: OK")
    except Exception as e:
        log.error("PROMO_CODES MIGRATION ERROR: %s", repr(e))
    finally:
        cur.close()


def fix_plan_code_promo_bug(conn):
    """One-time fix: companies that paid via GRATIS92 promo but plan_code wasn't committed."""
    cur = conn.cursor()
    try:
        # Fix company 5ed41151 which used GRATIS92 but plan_code stayed 'free' due to missing commit()
        cur.execute("""
            UPDATE companies
            SET plan_code = 'cotizabot', updated_at = now()
            WHERE id = '5ed41151-926f-4c1f-9099-78f829f72ab7'
              AND (plan_code IS NULL OR plan_code = 'free')
        """)
        if cur.rowcount > 0:
            log.info("FIX_PLAN_CODE: Updated company 5ed41151 to cotizabot")
        conn.commit()
    except Exception as e:
        log.error("FIX_PLAN_CODE ERROR: %s", repr(e))
    finally:
        cur.close()


def run_affiliates_migration(conn):
    """Create affiliate program tables (idempotent)."""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS affiliates (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                nombre          TEXT NOT NULL,
                email           TEXT NOT NULL UNIQUE,
                telefono        TEXT NOT NULL,
                empresa         TEXT,
                zona            TEXT,
                notas           TEXT,
                referral_code   TEXT NOT NULL UNIQUE,
                access_token    TEXT NOT NULL DEFAULT encode(gen_random_bytes(24), 'hex'),
                mp_email        TEXT,
                total_earned    NUMERIC NOT NULL DEFAULT 0,
                total_referrals INT NOT NULL DEFAULT 0,
                activo          BOOLEAN NOT NULL DEFAULT TRUE,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_affiliates_referral_code ON affiliates(referral_code);
            CREATE INDEX IF NOT EXISTS idx_affiliates_email ON affiliates(email);

            CREATE TABLE IF NOT EXISTS affiliate_referrals (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                affiliate_id    UUID NOT NULL REFERENCES affiliates(id),
                company_id      UUID NOT NULL,
                referral_code   TEXT NOT NULL,
                converted       BOOLEAN NOT NULL DEFAULT FALSE,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_affiliate_referrals_affiliate ON affiliate_referrals(affiliate_id);
            CREATE INDEX IF NOT EXISTS idx_affiliate_referrals_company ON affiliate_referrals(company_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_affiliate_referrals_company_unique ON affiliate_referrals(company_id);

            CREATE TABLE IF NOT EXISTS affiliate_commissions (
                id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                affiliate_id        UUID NOT NULL REFERENCES affiliates(id),
                referral_id         UUID NOT NULL REFERENCES affiliate_referrals(id),
                company_id          UUID NOT NULL,
                payment_id          TEXT,
                plan                TEXT NOT NULL,
                commission_type     TEXT NOT NULL,
                base_amount         NUMERIC NOT NULL,
                commission_amount   NUMERIC NOT NULL,
                description         TEXT,
                status              TEXT NOT NULL DEFAULT 'pending',
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                paid_at             TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_affiliate ON affiliate_commissions(affiliate_id);
            CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_status ON affiliate_commissions(status);
        """)
        conn.commit()
        log.info("AFFILIATES MIGRATION: OK")
    except Exception as e:
        log.error("AFFILIATES MIGRATION ERROR: %s", repr(e))
        conn.rollback()
    finally:
        cur.close()


def run_mp_payment_id_migration(conn):
    """Add mp_payment_id column to companies (idempotent)."""
    cur = conn.cursor()
    try:
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'companies' AND column_name = 'mp_payment_id'
                ) THEN
                    ALTER TABLE companies ADD COLUMN mp_payment_id TEXT;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'companies' AND column_name = 'referred_by'
                ) THEN
                    ALTER TABLE companies ADD COLUMN referred_by TEXT;
                END IF;
            END $$;
        """)
        conn.commit()
        log.info("MP_PAYMENT_ID + REFERRED_BY MIGRATION: OK")
    except Exception as e:
        log.error("MP_PAYMENT_ID MIGRATION ERROR: %s", repr(e))
        conn.rollback()
    finally:
        cur.close()


def run_all(conn):
    """Run all migrations."""
    run_pricebook_migrations(conn)
    run_promo_codes_migration(conn)
    fix_plan_code_promo_bug(conn)
    run_affiliates_migration(conn)
    run_mp_payment_id_migration(conn)
