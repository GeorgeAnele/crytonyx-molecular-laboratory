/*
==============================================================================
Project: crytonyx_enteprice_dw
Script: Bronze Layer Table Creation
Author: George Anele
Date: 25-Dec-2025

Purpose:
    This script initializes the Bronze Layer of the crytonyx_enteprice_dw
    data warehouse by creating raw staging tables for laboratory, billing,
    and sample shipment source systems.

    - Drops existing Bronze tables if they already exist to ensure a clean,
      deterministic deployment.
    - Defines table schemas that closely mirror upstream source systems,
      preserving data fidelity at ingestion time.
    - Establishes the foundational landing zone for all raw data ingested
      via ETL/ELT pipelines.

Usage Notes:
    - The Bronze Layer is strictly intended for raw data ingestion with
      minimal structural alteration.
    - Data cleansing, standardization, and business logic transformations
      are deferred to the Silver and Gold layers.
    - Dropping and recreating tables will permanently remove existing data;
      exercise caution outside development and test environments.
==============================================================================
*/

-- Drop and recreate to ensure a clean, idempotent Bronze load
IF OBJECT_ID('bronze.lab_test_raw', 'U') IS NOT NULL
    DROP TABLE bronze.lab_test_raw;
GO

CREATE TABLE bronze.lab_test_raw (
    pt_id               VARCHAR(50),
    test_date            DATE,
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    age                  INT,
    gender               VARCHAR(20),
    marital_status       VARCHAR(20),
    test_name            VARCHAR(200),
    country              VARCHAR(100),
    continent            VARCHAR(50),
    sample_number        VARCHAR(50),
    test_price_usd       DECIMAL(10,2),

    -- Ingestion metadata for traceability and audit
    ingestion_id         UNIQUEIDENTIFIER,
    source_file_name     VARCHAR(255),
    source_system        VARCHAR(50),
    ingested_at          DATETIME2(3)
);
GO


-- Raw billing data captured without transformation
IF OBJECT_ID('bronze.billing_invoice_raw', 'U') IS NOT NULL
    DROP TABLE bronze.billing_invoice_raw;
GO

CREATE TABLE bronze.billing_invoice_raw (
    invoice_number       VARCHAR(50),
    pt_id                VARCHAR(50),
    sample_number        VARCHAR(50),
    test_name            VARCHAR(200),
    invoice_date         DATE,
    currency             VARCHAR(20),
    gross_amount_usd     DECIMAL(12,2),
    tax_usd              DECIMAL(12,2),
    discount_usd         DECIMAL(12,2),
    net_amount_usd       DECIMAL(12,2),
    payment_status       VARCHAR(30),
    payment_method       VARCHAR(50),

    -- Ingestion metadata
    ingestion_id         UNIQUEIDENTIFIER,
    source_file_name     VARCHAR(255),
    source_system        VARCHAR(50),
    ingested_at          DATETIME2(3)
);
GO


-- Shipment and logistics events as received from source systems
IF OBJECT_ID('bronze.sample_shipment_raw', 'U') IS NOT NULL
    DROP TABLE bronze.sample_shipment_raw;
GO

CREATE TABLE bronze.sample_shipment_raw (
    shipment_id          VARCHAR(50),
    pt_id                VARCHAR(50),
    sample_number        VARCHAR(50),
    origin_country       VARCHAR(100),
    origin_continent     VARCHAR(50),
    courier              VARCHAR(100),
    pickup_datetime      DATETIME2(3),
    delivery_datetime    DATETIME2(3),
    transit_hours        INT,
    shipping_cost_usd    DECIMAL(12,2),
    shipment_status      VARCHAR(50),
    
    -- Ingestion metadata
    ingestion_id         UNIQUEIDENTIFIER,
    source_file_name     VARCHAR(255),
    source_system        VARCHAR(50),
    ingested_at          DATETIME2(3)
);
GO


