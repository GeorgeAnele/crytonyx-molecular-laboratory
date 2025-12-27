/*
==============================================================================
Project: crytonyx_enterprise_dw
Script: Bronze Layer Data Quality Validation Procedure
Author: George Anele
Date: 27-12-2025
Purpose:
    Performs data quality checks on the Bronze layer tables after each ingestion.
    - Focuses on mandatory fields, duplicates, numeric validation, and row counts.
    - Reports are printed to the SQL Server messages pane for quick verification.
==============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.validate_bronze_dq
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @latest_ingestion_lab UNIQUEIDENTIFIER,
        @latest_ingestion_billing UNIQUEIDENTIFIER,
        @latest_ingestion_shipment UNIQUEIDENTIFIER;

    -- ===========================================
    -- Identify latest ingestion_id per table
    -- ===========================================
    SET @latest_ingestion_lab = (SELECT MAX(ingestion_id) FROM bronze.lab_test_raw);
    SET @latest_ingestion_billing = (SELECT MAX(ingestion_id) FROM bronze.billing_invoice_raw);
    SET @latest_ingestion_shipment = (SELECT MAX(ingestion_id) FROM bronze.sample_shipment_raw);

    ------------------------------------------------------------------------
    -- LAB TEST RAW DQ CHECKS
    ------------------------------------------------------------------------
    PRINT '================================================';
    PRINT 'LAB TEST RAW TABLE DATA QUALITY CHECKS';
    PRINT 'Ingestion ID: ' + CAST(@latest_ingestion_lab AS VARCHAR(36));
    PRINT '================================================';

    -- Row count
    DECLARE @lab_count INT;
    SELECT @lab_count = COUNT(*) 
    FROM bronze.lab_test_raw 
    WHERE ingestion_id = @latest_ingestion_lab;
    PRINT 'Total rows ingested: ' + CAST(@lab_count AS VARCHAR);

    -- Nulls in mandatory columns
    DECLARE @lab_null_count INT;
    SELECT @lab_null_count = COUNT(*) 
    FROM bronze.lab_test_raw
    WHERE ingestion_id = @latest_ingestion_lab
      AND (pt_id IS NULL OR test_date IS NULL OR test_name IS NULL OR sample_number IS NULL);
    PRINT 'Rows with NULLs in mandatory columns: ' + CAST(@lab_null_count AS VARCHAR);

    -- Duplicates
    DECLARE @lab_dup_count INT;
    SELECT @lab_dup_count = SUM(duplicate_count) 
    FROM (
        SELECT COUNT(*) AS duplicate_count
        FROM bronze.lab_test_raw
        WHERE ingestion_id = @latest_ingestion_lab
        GROUP BY pt_id, test_date, sample_number
        HAVING COUNT(*) > 1
    ) AS t;
    PRINT 'Duplicate records found: ' + CAST(ISNULL(@lab_dup_count,0) AS VARCHAR);

    -- Invalid ages
    DECLARE @lab_invalid_age INT;
    SELECT @lab_invalid_age = COUNT(*) 
    FROM bronze.lab_test_raw
    WHERE ingestion_id = @latest_ingestion_lab
      AND (age < 0 OR age > 120);
    PRINT 'Rows with invalid age values: ' + CAST(@lab_invalid_age AS VARCHAR);

    ------------------------------------------------------------------------
    -- BILLING INVOICE RAW DQ CHECKS
    ------------------------------------------------------------------------
    PRINT '================================================';
    PRINT 'BILLING INVOICE RAW TABLE DATA QUALITY CHECKS';
    PRINT 'Ingestion ID: ' + CAST(@latest_ingestion_billing AS VARCHAR(36));
    PRINT '================================================';

    -- Row count
    DECLARE @billing_count INT;
    SELECT @billing_count = COUNT(*) 
    FROM bronze.billing_invoice_raw
    WHERE ingestion_id = @latest_ingestion_billing;
    PRINT 'Total rows ingested: ' + CAST(@billing_count AS VARCHAR);

    -- Nulls in mandatory columns
    DECLARE @billing_null_count INT;
    SELECT @billing_null_count = COUNT(*) 
    FROM bronze.billing_invoice_raw
    WHERE ingestion_id = @latest_ingestion_billing
      AND (invoice_number IS NULL OR pt_id IS NULL OR sample_number IS NULL OR net_amount_usd IS NULL);
    PRINT 'Rows with NULLs in mandatory columns: ' + CAST(@billing_null_count AS VARCHAR);

    -- Duplicate invoices
    DECLARE @billing_dup_count INT;
    SELECT @billing_dup_count = SUM(duplicate_count)
    FROM (
        SELECT COUNT(*) AS duplicate_count
        FROM bronze.billing_invoice_raw
        WHERE ingestion_id = @latest_ingestion_billing
        GROUP BY invoice_number
        HAVING COUNT(*) > 1
    ) AS t;
    PRINT 'Duplicate invoice_number records: ' + CAST(ISNULL(@billing_dup_count,0) AS VARCHAR);

    -- Negative amounts
    DECLARE @billing_negative_count INT;
    SELECT @billing_negative_count = COUNT(*)
    FROM bronze.billing_invoice_raw
    WHERE ingestion_id = @latest_ingestion_billing
      AND (gross_amount_usd < 0 OR net_amount_usd < 0 OR tax_usd < 0 OR discount_usd < 0);
    PRINT 'Rows with negative amounts: ' + CAST(@billing_negative_count AS VARCHAR);

    ------------------------------------------------------------------------
    -- SAMPLE SHIPMENT RAW DQ CHECKS
    ------------------------------------------------------------------------
    PRINT '================================================';
    PRINT 'SAMPLE SHIPMENT RAW TABLE DATA QUALITY CHECKS';
    PRINT 'Ingestion ID: ' + CAST(@latest_ingestion_shipment AS VARCHAR(36));
    PRINT '================================================';

    -- Row count
    DECLARE @shipment_count INT;
    SELECT @shipment_count = COUNT(*) 
    FROM bronze.sample_shipment_raw
    WHERE ingestion_id = @latest_ingestion_shipment;
    PRINT 'Total rows ingested: ' + CAST(@shipment_count AS VARCHAR);

    -- Nulls in mandatory columns
    DECLARE @shipment_null_count INT;
    SELECT @shipment_null_count = COUNT(*) 
    FROM bronze.sample_shipment_raw
    WHERE ingestion_id = @latest_ingestion_shipment
      AND (shipment_id IS NULL OR pt_id IS NULL OR sample_number IS NULL OR pickup_datetime IS NULL OR delivery_datetime IS NULL);
    PRINT 'Rows with NULLs in mandatory columns: ' + CAST(@shipment_null_count AS VARCHAR);

    -- Duplicate shipments
    DECLARE @shipment_dup_count INT;
    SELECT @shipment_dup_count = SUM(duplicate_count)
    FROM (
        SELECT COUNT(*) AS duplicate_count
        FROM bronze.sample_shipment_raw
        WHERE ingestion_id = @latest_ingestion_shipment
        GROUP BY shipment_id
        HAVING COUNT(*) > 1
    ) AS t;
    PRINT 'Duplicate shipment_id records: ' + CAST(ISNULL(@shipment_dup_count,0) AS VARCHAR);

    -- Negative shipping costs or transit hours
    DECLARE @shipment_negative_count INT;
    SELECT @shipment_negative_count = COUNT(*)
    FROM bronze.sample_shipment_raw
    WHERE ingestion_id = @latest_ingestion_shipment
      AND (shipping_cost_usd < 0 OR transit_hours < 0);
    PRINT 'Rows with negative shipping_cost_usd or transit_hours: ' + CAST(@shipment_negative_count AS VARCHAR);

    PRINT '================================================';
    PRINT 'Bronze Data Quality Validation Completed';
    PRINT '================================================';
END;
GO
