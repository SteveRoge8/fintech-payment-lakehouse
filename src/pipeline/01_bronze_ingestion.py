from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_BASE = "/Volumes/fintech_prod/raw_landing/cdc_landing_vol"

@dp.table(
    name="fintech_prod.bronze.bronze_wallets_cdc",
    comment="Raw incremental Customer Wallet CDC events ingested via Auto Loader",
    cluster_by=["wallet_id"]
)
def bronze_wallets_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME_BASE}/wallets_cdc")
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at")
        )
    )

@dp.table(
    name="fintech_prod.bronze.bronze_transactions_cdc",
    comment="Raw incremental Payment Transaction CDC events with schema drift rescue",
    cluster_by=["transaction_id"]
)
def bronze_transactions_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME_BASE}/transactions_cdc")
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at")
        )
    )