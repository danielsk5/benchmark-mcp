-- ============================================================
-- benchmark.db → Supabase PostgreSQL migration
-- Generated: 2026-02-25
-- Schema: public (use public to avoid extra schema creation step)
-- Tables: asset_metrics, assets, entities, entity_asset_stakes,
--         ingestion_log, portfolio_metrics
-- ============================================================

-- ── entities ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.entities (
    id          TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    type        TEXT        NOT NULL,
    ticker      TEXT,
    ir_url      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

-- ── assets ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.assets (
    id          TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    city        TEXT,
    state       TEXT,
    category    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

-- ── entity_asset_stakes ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.entity_asset_stakes (
    entity_id       TEXT            NOT NULL,
    asset_id        TEXT            NOT NULL,
    quarter         TEXT            NOT NULL,
    stake_pct       DOUBLE PRECISION,
    abl_own_sqm     DOUBLE PRECISION,
    abl_total_sqm   DOUBLE PRECISION,
    PRIMARY KEY (entity_id, asset_id, quarter)
);

-- ── asset_metrics ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.asset_metrics (
    asset_id            TEXT                NOT NULL,
    entity_id           TEXT                NOT NULL,
    quarter             TEXT                NOT NULL,
    period_type         TEXT                NOT NULL,
    sales_total         DOUBLE PRECISION,
    sales_own           DOUBLE PRECISION,
    sales_psqm          DOUBLE PRECISION,
    rent_min            DOUBLE PRECISION,
    rent_pct            DOUBLE PRECISION,
    rent_temp           DOUBLE PRECISION,
    rent_total          DOUBLE PRECISION,
    rent_psqm           DOUBLE PRECISION,
    parking_revenue     DOUBLE PRECISION,
    occupancy_rate      DOUBLE PRECISION,
    sss                 DOUBLE PRECISION,
    sas                 DOUBLE PRECISION,
    ssr                 DOUBLE PRECISION,
    sar                 DOUBLE PRECISION,
    occ_cost_pct        DOUBLE PRECISION,
    default_rate        DOUBLE PRECISION,
    noi                 DOUBLE PRECISION,
    noi_margin          DOUBLE PRECISION,
    source_file         TEXT,
    loaded_at           TIMESTAMPTZ         DEFAULT now(),
    PRIMARY KEY (asset_id, entity_id, quarter, period_type)
);

-- ── portfolio_metrics ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.portfolio_metrics (
    entity_id           TEXT                NOT NULL,
    quarter             TEXT                NOT NULL,
    period_type         TEXT                NOT NULL,
    total_assets        INTEGER,
    abl_total_sqm       DOUBLE PRECISION,
    abl_own_sqm         DOUBLE PRECISION,
    sales_total         DOUBLE PRECISION,
    sales_psqm          DOUBLE PRECISION,
    gross_revenue       DOUBLE PRECISION,
    net_revenue         DOUBLE PRECISION,
    noi                 DOUBLE PRECISION,
    ebitda              DOUBLE PRECISION,
    ebitda_margin       DOUBLE PRECISION,
    ffo                 DOUBLE PRECISION,
    ffo_margin          DOUBLE PRECISION,
    occupancy_rate      DOUBLE PRECISION,
    sss                 DOUBLE PRECISION,
    ssr                 DOUBLE PRECISION,
    rent_psqm           DOUBLE PRECISION,
    occ_cost_pct        DOUBLE PRECISION,
    net_debt            DOUBLE PRECISION,
    net_debt_ebitda     DOUBLE PRECISION,
    source_file         TEXT,
    loaded_at           TIMESTAMPTZ         DEFAULT now(),
    PRIMARY KEY (entity_id, quarter, period_type)
);

-- ── ingestion_log ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.ingestion_log (
    id                      SERIAL          PRIMARY KEY,
    entity_id               TEXT,
    quarter                 TEXT,
    file_name               TEXT,
    file_type               TEXT,
    rows_asset_metrics      INTEGER         DEFAULT 0,
    rows_portfolio_metrics  INTEGER         DEFAULT 0,
    rows_stakes             INTEGER         DEFAULT 0,
    loaded_at               TIMESTAMPTZ     DEFAULT now(),
    notes                   TEXT
);

-- ── RLS: disable for service role access (enable per-table as needed) ──
ALTER TABLE public.entities              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.assets                ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.entity_asset_stakes   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.asset_metrics         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.portfolio_metrics     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ingestion_log         ENABLE ROW LEVEL SECURITY;

-- Grant service role full access
GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role;
