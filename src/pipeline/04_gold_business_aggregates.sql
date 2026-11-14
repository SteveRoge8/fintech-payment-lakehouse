CREATE OR REFRESH MATERIALIZED VIEW fintech_prod.gold.gold_daily_merchant_liquidity
CLUSTER BY (settlement_date, merchant_id)
COMMENT 'Daily merchant settlement liquidity, pending exposure, and reversed volume by historical KYC risk'
AS
SELECT
  settlement_date,
  merchant_id,
  historical_kyc_tier,
  historical_risk_status,
  COUNT(transaction_id) AS total_transactions,
  ROUND(SUM(CASE WHEN payment_status = 'SETTLED' THEN amount ELSE 0.0 END), 2) AS settled_volume_kes,
  ROUND(SUM(CASE WHEN payment_status IN ('INITIATED', 'AUTHORIZED') THEN amount ELSE 0.0 END), 2) AS pending_volume_kes,
  ROUND(SUM(CASE WHEN payment_status = 'REVERSED' THEN amount ELSE 0.0 END), 2) AS reversed_volume_kes
FROM fintech_prod.gold.gold_fact_settlements
GROUP BY
  settlement_date,
  merchant_id,
  historical_kyc_tier,
  historical_risk_status;

CREATE OR REFRESH MATERIALIZED VIEW fintech_prod.gold.gold_quarantine_audit
CLUSTER BY (merchant_id)
COMMENT 'Operational audit summary of quarantined poison-pill payment records by merchant and file'
AS
SELECT
  TO_DATE(quarantined_at) AS quarantine_date,
  merchant_id,
  quarantine_reason,
  source_file,
  COUNT(transaction_id) AS quarantined_record_count,
  ROUND(MIN(amount), 2) AS min_invalid_amount,
  MAX(quarantined_at) AS latest_quarantine_timestamp
FROM fintech_prod.silver.silver_transactions_quarantine
GROUP BY
  TO_DATE(quarantined_at),
  merchant_id,
  quarantine_reason,
  source_file;