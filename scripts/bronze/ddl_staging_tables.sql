/*
==============================================================================
Project: crytonyx_enteprice_dw
Script: Bronze Layer Stage Table Creation
Author: George Anele
Date: 25-Dec-2025

Purpose:
    This script creates staging tables within the Bronze layer to support
    controlled loading of raw source data prior to final ingestion into
    persistent raw tables.

Usage Notes:
    - Stage tables are intended for transient data handling and validation.
    - Data in these tables may be truncated or overwritten during pipeline runs.
    - No transformation or enrichment is applied at this stage.
==============================================================================
*/

-- Recreate staging table to ensure a clean load per pipeline execution
IF OBJECT_ID('bronze.lab_test_stage', 'U') IS NOT NULL
    DROP TABLE bronze.lab_test_stage;
GO

CREATE TABLE bronze.lab_test_stage (
    pt_id             VARCHAR(50),
    test_date          DATE,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    age                INT,
    gender             VARCHAR(20),
    marital_status     VARCHAR(20),
    test_name          VARCHAR(200),
    country            VARCHAR(100),
    continent          VARCHAR(50),
    sample_number      VARCHAR(50),
    test_price_usd     DECIMAL(10,2)
);
GO


-- Raw invoice data staged prior to persistence in Bronze raw tables
IF OBJECT_ID('bronze.billing_invoice_stage', 'U') IS NOT NULL
    DROP TABLE bronze.billing_invoice_stage;
GO

CREATE TABLE bronze.billing_invoice_stage (
    invoice_number     VARCHAR(50),
    pt_id              VARCHAR(50),
    sample_number      VARCHAR(50),
    test_name          VARCHAR(200),
    invoice_date       DATE,
    currency           VARCHAR(20),
    gross_amount_usd   DECIMAL(12,2),
    tax_usd            DECIMAL(12,2),
    discount_usd       DECIMAL(12,2),
    net_amount_usd     DECIMAL(12,2),
    payment_status     VARCHAR(30),
    payment_method     VARCHAR(50)
);
GO


-- Shipment events staged to support validation and batch ingestion
IF OBJECT_ID('bronze.sample_shipment_stage', 'U') IS NOT NULL
    DROP TABLE bronze.sample_shipment_stage;
GO

CREATE TABLE bronze.sample_shipment_stage (
    shipment_id        VARCHAR(50),
    pt_id              VARCHAR(50),
    sample_number      VARCHAR(50),
    origin_country     VARCHAR(100),
    origin_continent   VARCHAR(50),
    courier            VARCHAR(100),
    pickup_datetime    DATETIME2(3),
    delivery_datetime  DATETIME2(3),
    transit_hours      INT,
    shipping_cost_usd  DECIMAL(12,2),
    shipment_status    VARCHAR(50)
);
GO
