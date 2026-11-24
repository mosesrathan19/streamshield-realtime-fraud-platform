-- StreamShield Phase 4 (local dev): SQL models for synthetic transactions.
-- Never run against real payment data or an internet-exposed database.
CREATE TABLE IF NOT EXISTS transactions (
    event_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    currency TEXT NOT NULL CHECK (currency = 'USD'),
    country TEXT NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    risk_high_amount BOOLEAN NOT NULL,
    risk_synthetic_country BOOLEAN NOT NULL,
    risk_flag BOOLEAN NOT NULL,
    CONSTRAINT risk_rule_consistency CHECK (
        risk_flag = (risk_high_amount OR risk_synthetic_country)
    )
);
CREATE INDEX IF NOT EXISTS idx_tx_event_ts ON transactions (event_ts);
CREATE INDEX IF NOT EXISTS idx_tx_merchant_event ON transactions (merchant_category, event_ts);
CREATE INDEX IF NOT EXISTS idx_tx_account_ts ON transactions (account_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_tx_risk_event ON transactions (event_ts) WHERE risk_flag;

CREATE OR REPLACE VIEW fraud_alerts AS
SELECT event_id, account_id, merchant_id, merchant_category,
       amount_cents, event_ts, risk_high_amount, risk_synthetic_country
FROM transactions WHERE risk_flag;

CREATE OR REPLACE VIEW daily_kpis AS
SELECT (event_ts AT TIME ZONE 'UTC')::date AS event_day_utc,
       COUNT(*) AS transactions,
       COUNT(*) FILTER (WHERE risk_flag) AS flagged_transactions,
       SUM(amount_cents) AS amount_cents,
       ROUND(100.0 * COUNT(*) FILTER (WHERE risk_flag) / NULLIF(COUNT(*), 0), 2)
          AS flagged_pct
FROM transactions GROUP BY 1;

CREATE OR REPLACE VIEW merchant_category_kpis AS
SELECT merchant_category,
       COUNT(*) AS transactions,
       COUNT(*) FILTER (WHERE risk_flag) AS flagged_transactions,
       SUM(amount_cents) AS amount_cents,
       ROUND(100.0 * COUNT(*) FILTER (WHERE risk_flag) / NULLIF(COUNT(*), 0), 2)
          AS flagged_pct
FROM transactions GROUP BY merchant_category;
