from pyspark import pipelines as dp
from pyspark.sql import functions as F

TX_QUALITY_RULES = {
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_wallet_id": "wallet_id IS NOT NULL",
    "positive_amount": "amount > 0",
    "valid_currency": "currency = 'KES'"
}

QUARANTINE_FILTER = f"NOT coalesce(({' AND '.join(TX_QUALITY_RULES.values())}), false)"

@dp.table(
    name="fintech_prod.silver.silver_transactions_clean",
    comment="Validated payment transactions passing all Silver data contracts",
    cluster_by=["transaction_id", "wallet_id"]
)
@dp.expect_all_or_drop(TX_QUALITY_RULES)
def silver_transactions_clean():
    return (
        spark.readStream.table("fintech_prod.bronze.bronze_transactions_cdc")
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
    )

@dp.table(
    name="fintech_prod.silver.silver_transactions_quarantine",
    comment="Dead-letter quarantine table isolating malformed payment transactions",
    cluster_by=["transaction_id"]
)
def silver_transactions_quarantine():
    return (
        spark.readStream.table("fintech_prod.bronze.bronze_transactions_cdc")
        .filter(F.expr(QUARANTINE_FILTER))
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("quarantine_reason", F.lit("Failed Silver data contract: NULL key or non-positive amount"))
    )

@dp.temporary_view(
    name="wallets_cdc_clean_v",
    comment="Pre-processed wallet CDC stream with native TIMESTAMP for SCD2 sequencing"
)
@dp.expect_or_drop("valid_wallet_id", "wallet_id IS NOT NULL")
def wallets_cdc_clean_v():
    return (
        spark.readStream.table("fintech_prod.bronze.bronze_wallets_cdc")
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
    )

dp.create_streaming_table(
    name="fintech_prod.silver.silver_wallets_scd2",
    comment="SCD Type 2 customer wallet dimension tracking historical KYC tier and risk changes",
    cluster_by=["wallet_id"]
)

dp.create_auto_cdc_flow(
    target="fintech_prod.silver.silver_wallets_scd2",
    source="wallets_cdc_clean_v",
    keys=["wallet_id"],
    sequence_by=F.col("event_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=["kyc_tier", "risk_status"],
    except_column_list=["cdc_operation", "_rescued_data", "source_file", "ingested_at"]
)

dp.create_streaming_table(
    name="fintech_prod.silver.silver_transactions_scd1",
    comment="SCD Type 1 deduplicated transactions reflecting latest payment lifecycle state",
    cluster_by=["transaction_id", "wallet_id"]
)

dp.create_auto_cdc_flow(
    target="fintech_prod.silver.silver_transactions_scd1",
    source="fintech_prod.silver.silver_transactions_clean",
    keys=["transaction_id"],
    sequence_by=F.col("event_timestamp"),
    stored_as_scd_type=1,
    except_column_list=["_rescued_data", "source_file", "ingested_at"]
)