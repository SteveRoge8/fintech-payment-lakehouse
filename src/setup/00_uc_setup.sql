CREATE CATALOG IF NOT EXISTS fintech_prod
COMMENT 'Production catalog for the Event-Driven FinTech Payment and Settlement Lakehouse';

USE CATALOG fintech_prod;

CREATE SCHEMA IF NOT EXISTS fintech_prod.raw_landing
COMMENT 'Landing zone schema hosting Managed Volumes for raw CDC JSON streams and Auto Loader schemas';

CREATE SCHEMA IF NOT EXISTS fintech_prod.bronze
COMMENT 'Bronze layer: Raw incremental append-only CDC events ingested via Auto Loader with rescued data';

CREATE SCHEMA IF NOT EXISTS fintech_prod.silver
COMMENT 'Silver layer: Enforced data contracts, dead-letter quarantine, and deduplicated SCD1/SCD2 entities';

CREATE SCHEMA IF NOT EXISTS fintech_prod.gold
COMMENT 'Gold layer: Analytics-ready Star Schema materialized views and settlement liquidity aggregates';

CREATE VOLUME IF NOT EXISTS fintech_prod.raw_landing.cdc_landing_vol
COMMENT 'Managed Volume storing raw incremental CDC JSON files and Auto Loader schema locations';

SHOW SCHEMAS IN fintech_prod;

SHOW VOLUMES IN fintech_prod.raw_landing;