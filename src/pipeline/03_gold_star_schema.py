from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="fintech_prod.gold.gold_fact_settlements",
    comment="Kimball Fact table joining SCD1 transactions with point-in-time SCD2 wallet attributes",
    cluster_by=["merchant_id", "payment_status"]
)
def gold_fact_settlements():
    tx = spark.read.table("fintech_prod.silver.silver_transactions_scd1").alias("tx")
    w = spark.read.table("fintech_prod.silver.silver_wallets_scd2").alias("w")

    pit_join_cond = F.expr("""
        tx.wallet_id = w.wallet_id
        AND tx.event_timestamp >= (w.__START_AT - INTERVAL 1 MINUTE)
        AND (w.__END_AT IS NULL OR tx.event_timestamp < (w.__END_AT - INTERVAL 1 MINUTE))
    """)

    return (
        tx.join(w, on=pit_join_cond, how="left")
        .select(
            F.col("tx.transaction_id"),
            F.col("tx.wallet_id"),
            F.col("w.customer_id"),
            F.col("tx.merchant_id"),
            F.col("tx.amount"),
            F.col("tx.currency"),
            F.col("tx.payment_status"),
            F.col("w.kyc_tier").alias("historical_kyc_tier"),
            F.col("w.risk_status").alias("historical_risk_status"),
            F.col("w.country_code"),
            F.col("tx.event_timestamp"),
            F.to_date(F.col("tx.event_timestamp")).alias("settlement_date")
        )
    )