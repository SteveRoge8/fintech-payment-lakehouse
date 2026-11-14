WITH layer_counts AS (
  SELECT
    (SELECT COUNT(*) FROM fintech_prod.bronze.bronze_transactions_cdc) AS bronze_tx_count,
    (SELECT COUNT(*) FROM fintech_prod.silver.silver_transactions_clean) AS silver_clean_count,
    (SELECT COUNT(*) FROM fintech_prod.silver.silver_transactions_quarantine) AS silver_quarantine_count,
    (SELECT COUNT(*) FROM fintech_prod.silver.silver_transactions_scd1) AS silver_scd1_count,
    (SELECT COUNT(*) FROM fintech_prod.gold.gold_fact_settlements) AS gold_fact_count
),
sla_metrics AS (
  SELECT
    bronze_tx_count,
    silver_clean_count,
    silver_quarantine_count,
    silver_scd1_count,
    gold_fact_count,
    ROUND((silver_quarantine_count * 100.0) / NULLIF(bronze_tx_count, 0), 2) AS quarantine_rate_pct
  FROM layer_counts
)
SELECT
  bronze_tx_count,
  silver_clean_count,
  silver_quarantine_count,
  silver_scd1_count,
  gold_fact_count,
  quarantine_rate_pct,
  ASSERT_TRUE(
    (silver_clean_count + silver_quarantine_count) = bronze_tx_count,
    'SLA BREACH: Row conservation failed between Bronze and Silver!'
  ) IS NULL AS row_conservation_sla_passed,
  ASSERT_TRUE(
    quarantine_rate_pct <= 10.0,
    'SLA BREACH: Poison-pill quarantine rate exceeded 10% threshold!'
  ) IS NULL AS quarantine_threshold_sla_passed,
  ASSERT_TRUE(
    gold_fact_count = silver_scd1_count,
    'SLA BREACH: Gold Fact row count does not match Silver SCD1 (SCD2 fan-out or drop detected)!'
  ) IS NULL AS star_schema_grain_sla_passed,
  CURRENT_TIMESTAMP() AS audited_at
FROM sla_metrics;