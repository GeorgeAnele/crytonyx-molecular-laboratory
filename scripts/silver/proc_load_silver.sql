/*
==============================================================================
Script: Silver Layer Load Procedure
Project: crytonyx_enterprise_dw
Author: George Anele
Created: 27-Dec-2025

Purpose:
    Orchestrates the incremental loading of curated Silver-layer tables
    from Bronze raw sources, applying standardization, validation, and
    deduplication rules.

Scope:
    - Loads laboratory test data
    - Loads billing invoice data
    - Loads sample shipment data

Operational Notes:
    - Uses ingestion_id to track batch-level lineage
    - Prevents duplicate records via natural key checks
    - Designed for repeatable, idempotent execution
============================================================================== 
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @batch_start_time DATETIME2(3),
        @batch_end_time   DATETIME2(3),
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @rows_inserted    INT,
        @ingestion_id     UNIQUEIDENTIFIER;

    BEGIN TRY
        SET @batch_start_time = SYSDATETIME();
        SET @ingestion_id = NEWID();  -- Batch-level lineage identifier

        PRINT '================================================';
        PRINT 'Crytonyx Silver Load Started';
        PRINT 'Ingestion ID: ' + CAST(@ingestion_id AS VARCHAR(36));
        PRINT '================================================';


        ----------------------------------------------------------------------
        -- LAB TEST DATA
        ----------------------------------------------------------------------
        PRINT '================================================';
        PRINT 'Loading LAB TEST data';
        PRINT '================================================';

        SET @start_time = SYSDATETIME();

        INSERT INTO silver.lab_test
        (
            pt_id,
            sample_number,
            test_name,
            test_date,
            first_name,
            last_name,
            age,
            gender,
            marital_status,
            country,
            continent,
            test_price_usd,
            ingestion_id,
            source_system,
            ingested_at
        )
        SELECT DISTINCT
            b.pt_id,
            b.sample_number,
            LTRIM(RTRIM(b.test_name)),        -- Standardize text fields
            b.test_date,
            LTRIM(RTRIM(b.first_name)),
            LTRIM(RTRIM(b.last_name)),
            b.age,
            CASE                              -- Normalize gender values
                WHEN b.gender IN ('Male','M') THEN 'M'
                WHEN b.gender IN ('Female','F') THEN 'F'
                ELSE 'U'
            END,
            UPPER(LTRIM(RTRIM(b.marital_status))),
            LTRIM(RTRIM(b.country)),
            LTRIM(RTRIM(b.continent)),
            CASE                              -- Guard against invalid pricing
                WHEN b.test_price_usd < 0 THEN NULL
                ELSE b.test_price_usd
            END,
            b.ingestion_id,
            b.source_system,
            b.ingested_at
        FROM bronze.lab_test_raw b
        WHERE b.pt_id IS NOT NULL
          AND b.sample_number IS NOT NULL
          AND b.test_name IS NOT NULL
          AND b.test_date IS NOT NULL
          AND NOT EXISTS                     -- Deduplication check
          (
              SELECT 1
              FROM silver.lab_test s
              WHERE s.pt_id = b.pt_id
                AND s.sample_number = b.sample_number
                AND s.test_name = LTRIM(RTRIM(b.test_name))
                AND s.test_date = b.test_date
          );

        SET @end_time = SYSDATETIME();

        -- Row count based on current ingestion batch
        SELECT @rows_inserted = COUNT(*)
        FROM silver.lab_test
        WHERE ingestion_id = @ingestion_id;

        PRINT CAST(@rows_inserted AS VARCHAR) + ' rows inserted into silver.lab_test';
        PRINT 'LAB TEST duration (sec): ' +
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);


        ----------------------------------------------------------------------
        -- BILLING INVOICE DATA
        ----------------------------------------------------------------------
        PRINT '================================================';
        PRINT 'Loading BILLING data';
        PRINT '================================================';

        SET @start_time = SYSDATETIME();

        INSERT INTO silver.billing_invoice
        (
            invoice_number,
            pt_id,
            sample_number,
            test_name,
            invoice_date,
            currency,
            gross_amount_usd,
            tax_usd,
            discount_usd,
            net_amount_usd,
            payment_status,
            payment_method,
            ingestion_id,
            source_system,
            ingested_at
        )
        SELECT
            b.invoice_number,
            b.pt_id,
            b.sample_number,
            LTRIM(RTRIM(b.test_name)),
            b.invoice_date,
            UPPER(b.currency),
            b.gross_amount_usd,
            b.tax_usd,
            b.discount_usd,
            b.net_amount_usd,
            UPPER(LTRIM(RTRIM(b.payment_status))),
            UPPER(LTRIM(RTRIM(b.payment_method))),
            b.ingestion_id,
            b.source_system,
            b.ingested_at
        FROM bronze.billing_invoice_raw b
        WHERE b.invoice_number IS NOT NULL
          AND b.pt_id IS NOT NULL
          AND b.invoice_date IS NOT NULL
          AND b.gross_amount_usd >= 0
          AND NOT EXISTS                     -- Invoice-level uniqueness
          (
              SELECT 1
              FROM silver.billing_invoice s
              WHERE s.invoice_number = b.invoice_number
          );

        SET @end_time = SYSDATETIME();

        SELECT @rows_inserted = COUNT(*)
        FROM silver.billing_invoice
        WHERE ingestion_id = @ingestion_id;

        PRINT CAST(@rows_inserted AS VARCHAR) + ' rows inserted into silver.billing_invoice';
        PRINT 'BILLING duration (sec): ' +
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);


        ----------------------------------------------------------------------
        -- SAMPLE SHIPMENT DATA
        ----------------------------------------------------------------------
        PRINT '================================================';
        PRINT 'Loading SHIPMENT data';
        PRINT '================================================';

        SET @start_time = SYSDATETIME();

        INSERT INTO silver.sample_shipment
        (
            shipment_id,
            pt_id,
            sample_number,
            origin_country,
            origin_continent,
            courier,
            pickup_datetime,
            delivery_datetime,
            transit_hours,
            shipping_cost_usd,
            shipment_status,
            ingestion_id,
            source_system,
            ingested_at
        )
        SELECT
            b.shipment_id,
            b.pt_id,
            b.sample_number,
            LTRIM(RTRIM(b.origin_country)),
            LTRIM(RTRIM(b.origin_continent)),
            LTRIM(RTRIM(b.courier)),
            b.pickup_datetime,
            b.delivery_datetime,
            CASE                              -- Derive transit duration when possible
                WHEN b.delivery_datetime IS NOT NULL
                 AND b.pickup_datetime IS NOT NULL
                THEN DATEDIFF(HOUR, b.pickup_datetime, b.delivery_datetime)
                ELSE b.transit_hours
            END,
            b.shipping_cost_usd,
            UPPER(LTRIM(RTRIM(b.shipment_status))),
            b.ingestion_id,
            b.source_system,
            b.ingested_at
        FROM bronze.sample_shipment_raw b
        WHERE b.shipment_id IS NOT NULL
          AND b.pickup_datetime IS NOT NULL
          AND NOT EXISTS                     -- Shipment-level deduplication
          (
              SELECT 1
              FROM silver.sample_shipment s
              WHERE s.shipment_id = b.shipment_id
          );

        SET @end_time = SYSDATETIME();

        SELECT @rows_inserted = COUNT(*)
        FROM silver.sample_shipment
        WHERE ingestion_id = @ingestion_id;

        PRINT CAST(@rows_inserted AS VARCHAR) + ' rows inserted into silver.sample_shipment';
        PRINT 'SHIPMENT duration (sec): ' +
              CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);


        ----------------------------------------------------------------------
        -- COMPLETION
        ----------------------------------------------------------------------
        SET @batch_end_time = SYSDATETIME();

        PRINT '================================================';
        PRINT 'Silver Load Completed Successfully';
        PRINT 'Total duration (sec): ' +
              CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR);
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '========================================';
    END CATCH
END;
GO
