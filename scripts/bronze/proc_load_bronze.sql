/*
==============================================================================
Project: crytonyx_enteprice_dw
Script: Bronze Layer Load Procedure
Author: George Anele
Date: 26-Dec-2025

Purpose:
    This procedure orchestrates the end-to-end loading of raw source data
    into the Bronze layer by:
    - Truncating staging tables
    - Bulk loading CSV files into stage
    - Persisting data into raw Bronze tables with ingestion metadata

Usage Notes:
    - Designed for batch execution as part of the Bronze ingestion pipeline.
    - Each execution generates a unique ingestion_id for lineage tracking.
    - Source files are assumed to be available at the configured file paths.
==============================================================================
*/

-- Execute the Bronze load procedure
EXEC bronze.load_bronze
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @batch_start_time DATETIME2(3),
        @batch_end_time   DATETIME2(3),
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @ingestion_id     UNIQUEIDENTIFIER;

    BEGIN TRY
        -- Initialize batch-level metadata
        SET @batch_start_time = SYSDATETIME();
        SET @ingestion_id = NEWID();

        PRINT '================================================';
        PRINT 'Crytonyx Bronze Load Started';
        PRINT 'Ingestion ID: ' + CAST(@ingestion_id AS VARCHAR(36));
        PRINT '================================================';

        /* ============================================================
           1. LAB TEST DATA
           ============================================================ */

        PRINT 'Loading LAB TEST data';

        -- Reset stage table to guarantee a clean batch load
        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE bronze.lab_test_stage;

        -- Bulk load raw CSV data into staging
        BULK INSERT bronze.lab_test_stage
        FROM 'C:\Users\user\Desktop\DATA ENGR\Crytonyx_lab\datasets\lab_test.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        -- Persist staged data into Bronze raw table with ingestion metadata
        INSERT INTO bronze.lab_test_raw (
            pt_id,
            test_date,
            first_name,
            last_name,
            age,
            gender,
            marital_status,
            test_name,
            country,
            continent,
            sample_number,
            test_price_usd,
            ingestion_id,
            source_file_name,
            source_system,
            ingested_at
        )
        SELECT
            pt_id,
            test_date,
            first_name,
            last_name,
            age,
            gender,
            marital_status,
            test_name,
            country,
            continent,
            sample_number,
            test_price_usd,
            @ingestion_id,
            'lab_test.csv',
            'CRYTONYX_LIMS',
            SYSDATETIME()
        FROM bronze.lab_test_stage;

        SET @end_time = SYSDATETIME();
        PRINT 'LAB TEST load duration (sec): ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);

        /* ============================================================
           2. BILLING INVOICE DATA
           ============================================================ */

        PRINT 'Loading BILLING data';

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE bronze.billing_invoice_stage;

        BULK INSERT bronze.billing_invoice_stage
        FROM 'C:\Users\user\Desktop\DATA ENGR\Crytonyx_lab\datasets\billing_invoice.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        INSERT INTO bronze.billing_invoice_raw (
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
            source_file_name,
            source_system,
            ingested_at
        )
        SELECT
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
            @ingestion_id,
            'billing_invoice.csv',
            'CRYTONYX_BILLING',
            SYSDATETIME()
        FROM bronze.billing_invoice_stage;

        SET @end_time = SYSDATETIME();
        PRINT 'BILLING load duration (sec): ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);

        /* ============================================================
           3. SAMPLE SHIPMENT DATA
           ============================================================ */

        PRINT 'Loading SHIPMENT data';

        SET @start_time = SYSDATETIME();
        TRUNCATE TABLE bronze.sample_shipment_stage;

        BULK INSERT bronze.sample_shipment_stage
        FROM 'C:\Users\user\Desktop\DATA ENGR\Crytonyx_lab\datasets\sample_shipment.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        INSERT INTO bronze.sample_shipment_raw (
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
            source_file_name,
            source_system,
            ingested_at
        )
        SELECT
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
            @ingestion_id,
            'sample_shipment.csv',
            'CRYTONYX_LOGISTICS',
            SYSDATETIME()
        FROM bronze.sample_shipment_stage;

        -- Capture batch completion timestamp
        SET @batch_end_time = SYSDATETIME();

        PRINT '================================================';
        PRINT 'Bronze Load Completed Successfully';
        PRINT 'Total Duration (sec): ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR);
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        -- Surface error details and rethrow for upstream handling
        PRINT '================================================';
        PRINT 'ERROR DURING BRONZE LOAD';
        PRINT ERROR_MESSAGE();
        PRINT '================================================';
        THROW;
    END CATCH
END;
GO
