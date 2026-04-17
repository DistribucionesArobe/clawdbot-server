-- 002_promo_codes.sql
-- Sistema de códigos promocionales y trials para CotizaExpress

-- Tabla de códigos promo
CREATE TABLE IF NOT EXISTS promo_codes (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code        TEXT NOT NULL UNIQUE,
    discount_type TEXT NOT NULL DEFAULT 'trial_days',   -- 'trial_days' | 'percentage'
    discount_value NUMERIC NOT NULL DEFAULT 10,          -- días para trial, % para descuento
    max_uses    INT DEFAULT NULL,                        -- NULL = ilimitado
    times_used  INT NOT NULL DEFAULT 0,
    one_per_customer BOOLEAN NOT NULL DEFAULT TRUE,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ DEFAULT NULL                 -- NULL = no expira
);

-- Registro de quién usó cada código
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

-- Agregar columna trial_end a companies (para saber cuándo expira el trial)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'companies' AND column_name = 'trial_end'
    ) THEN
        ALTER TABLE companies ADD COLUMN trial_end TIMESTAMPTZ DEFAULT NULL;
    END IF;
END $$;
