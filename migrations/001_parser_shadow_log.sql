-- Migración: tabla para shadow mode del LLM parser.
-- Correr una vez en psql contra cotizaexpress_db.
--
-- Cada fila loggea el mensaje del cliente + lo que parseó el regex + lo que parseó el LLM.
-- Así podemos comparar accuracy en Aceromax 24-48h antes de hacer el switch.

CREATE TABLE IF NOT EXISTS parser_shadow_log (
    id              bigserial PRIMARY KEY,
    company_id      uuid NOT NULL,
    client_phone    text,
    user_text       text NOT NULL,
    regex_items     jsonb NOT NULL DEFAULT '[]'::jsonb,
    llm_items       jsonb NOT NULL DEFAULT '[]'::jsonb,
    llm_non_order   boolean DEFAULT false,
    llm_error       text,
    llm_latency_ms  integer,
    llm_model       text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_parser_shadow_company_time
    ON parser_shadow_log(company_id, created_at DESC);

-- Para revisar luego:
--   SELECT user_text, jsonb_array_length(regex_items) AS r, jsonb_array_length(llm_items) AS l,
--          llm_latency_ms, llm_error
--   FROM parser_shadow_log
--   WHERE company_id = '30208e3c-70c6-4203-97d9-172fad7d3c75'
--   ORDER BY created_at DESC LIMIT 100;
