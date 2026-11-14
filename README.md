# Event-Driven FinTech Payment & Settlement Lakehouse

## 1. Executive Summary
Production-grade Medallion Lakehouse built on Databricks Serverless, Unity Catalog, Auto Loader, and Lakeflow Spark Declarative Pipelines (`pyspark.pipelines`). This system ingests stateful payment lifecycle transitions and customer wallet updates, isolates malformed poison-pill records into a Dead-Letter Quarantine table, reconciles out-of-order CDC streams via SCD Type 1 and SCD Type 2 flows, and serves point-in-time Star Schema views to Databricks AI/BI and Genie.

## 2. Architecture Flow
```text
[Stateful CDC Generator] --> (/Volumes/fintech_prod/raw_landing/cdc_landing_vol)
                                     |
       +-----------------------------+-----------------------------+
       v                                                           v
[bronze_wallets_cdc]                                  [bronze_transactions_cdc]
       |                                                           |
       v                                             +-------------+-------------+
(wallets_cdc_clean_v)                                |                           |
       |                                             v                           v
       v                                [silver_transactions_clean] [silver_transactions_quarantine]
[silver_wallets_scd2]                                |                           |
  (SCD Type 2 History)                               v                           v
       |                                [silver_transactions_scd1]     [gold_quarantine_audit]
       |                                  (SCD Type 1 Upserts)
       +----------------------+----------------------+
                              |
                              v
                  [gold_fact_settlements]
               (Point-in-Time Interval Join)
                              |
                              v
             [gold_daily_merchant_liquidity] --> [SLA Circuit-Breaker & AI/BI Control Tower]