/*
==============================================================================
Project: crytonyx_enterprise_dw
Script: Silver Layer Core Table Creation
Author: George Anele
Date: 27-12-2025
Purpose:
    Creates cleansed, conformed Silver layer tables derived from Bronze data.

    - Applies business rules, constraints, and standardization.
    - Enforces data quality via NOT NULLs, CHECK constraints, and uniqueness.
    - Prepares data for downstream dimensional modeling in the Gold layer.

Usage Notes:
    - Tables are dropped and recreated to ensure schema consistency.
    - Execute in controlled environments due to potential data loss.
==============================================================================
*/

IF OBJECT_ID('silver.lab_test', 'U') IS NOT NULL
    DROP TABLE silver.lab_test;
GO

CREATE TABLE silver.lab_test (
    lab_test_sk        INT IDENTITY(1,1) PRIMARY KEY,   -- Surrogate key

    pt_id              VARCHAR(50) NOT NULL,
    sample_number      VARCHAR(50) NOT NULL,
    test_name          VARCHAR(200) NOT NULL,
    test_date          DATE NOT NULL,

    first_name         VARCHAR(100) NOT NULL,
    last_name          VARCHAR(100) NOT NULL,
    age                INT CHECK (age BETWEEN 0 AND 120),
    gender             CHAR(1) CHECK (gender IN ('M','F')),
    marital_status     VARCHAR(20),

    country            VARCHAR(100),
    continent          VARCHAR(50),
    test_price_usd     DECIMAL(10,2) CHECK (test_price_usd >= 0),

    ingestion_id       UNIQUEIDENTIFIER NOT NULL,       -- Load lineage
    source_system      VARCHAR(50),
    ingested_at        DATETIME2(3) NOT NULL,

    CONSTRAINT uq_silver_lab_test 
        UNIQUE (pt_id, sample_number, test_name, test_date) -- Business key
);
GO


IF OBJECT_ID('silver.billing_invoice', 'U') IS NOT NULL
    DROP TABLE silver.billing_invoice;
GO

CREATE TABLE silver.billing_invoice (
    billing_sk         INT IDENTITY(1,1) PRIMARY KEY,   -- Surrogate key

    invoice_number     VARCHAR(50) NOT NULL,
    pt_id              VARCHAR(50) NOT NULL,
    sample_number      VARCHAR(50) NOT NULL,
    test_name          VARCHAR(200) NOT NULL,

    invoice_date       DATE NOT NULL,
    currency           CHAR(3) NOT NULL,

    gross_amount_usd   DECIMAL(12,2) CHECK (gross_amount_usd >= 0),
    tax_usd            DECIMAL(12,2) CHECK (tax_usd >= 0),
    discount_usd       DECIMAL(12,2) CHECK (discount_usd >= 0),
    net_amount_usd     DECIMAL(12,2) CHECK (net_amount_usd >= 0),

    payment_status     VARCHAR(30),
    payment_method     VARCHAR(50),

    ingestion_id       UNIQUEIDENTIFIER NOT NULL,       -- Load lineage
    source_system      VARCHAR(50),
    ingested_at        DATETIME2(3) NOT NULL,

    CONSTRAINT uq_silver_invoice UNIQUE (invoice_number) -- Natural key
);
GO

IF OBJECT_ID('silver.sample_shipment', 'U') IS NOT NULL
    DROP TABLE silver.sample_shipment;
GO

CREATE TABLE silver.sample_shipment (
    shipment_sk        INT IDENTITY(1,1) PRIMARY KEY,   -- Surrogate key

    shipment_id        VARCHAR(50) NOT NULL,
    pt_id              VARCHAR(50) NOT NULL,
    sample_number      VARCHAR(50) NOT NULL,

    origin_country     VARCHAR(100),
    origin_continent   VARCHAR(50),
    courier            VARCHAR(100),

    pickup_datetime    DATETIME2(3) NOT NULL,
    delivery_datetime  DATETIME2(3),
    transit_hours      INT CHECK (transit_hours >= 0),

    shipping_cost_usd  DECIMAL(12,2) CHECK (shipping_cost_usd >= 0),
    shipment_status    VARCHAR(20),

    ingestion_id       UNIQUEIDENTIFIER NOT NULL,       -- Load lineage
    source_system      VARCHAR(50),
    ingested_at        DATETIME2(3) NOT NULL,

    CONSTRAINT uq_silver_shipment UNIQUE (shipment_id)  -- Natural key
);
GO
