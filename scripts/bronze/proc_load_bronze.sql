/*
==============================================================================
Script: Bronze Layer Load Procedure
Project: crytonyx_enterprise_dw
Author: George Anele
Date: 26-12-2025
Purpose:
    Orchestrates raw ingestion of CSV datasets into the Bronze layer.
    - Preserves data as-is from source systems: LIMS, Billing, Logistics.
    - Adds ingestion metadata for lineage and auditing.
    - Idempotent execution via TRUNCATE + reload of staging tables.
Usage Notes:
    - Bronze layer stores raw data with minimal or no transformation.
    - Performance metrics are captured per dataset and per batch.
    - Errors are centrally handled and logged via TRY...CATCH.
==============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;
    ------------------------------------------------------------
    -- Suppress row count messages for clean logs
    ------------------------------------------------------------

    DECLARE 
        @batch_start_time DATETIME2(3),
        @batch_end_time   DATETIME2(3),
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @ingestion_id     UNIQUEIDENTIFIER;
    ------------------------------------------------------------
    -- Batch timing and metadata variables
    ------------------------------------------------------------

    BEGIN TRY
        ------------------------------------------------------------
        -- Initialize batch metadata
        ------------------------------------------------------------
        SET @batch_start_time = SYSDATETIME();
        SET @ingestion_id = NEWID();

        PRINT '================================================';
        PRINT 'Crytonyx Bronze Load Started';
        PRINT 'Ingestion ID: ' + CAST(@ingestion_id AS VARCHAR(36));
        PRINT '================================================';

        /* ============================================================
           1. LAB TEST DATA
        ============================================================ */

        PRINT '================================================';
        PRINT 'Loading LAB TEST data';
        PRINT '================================================';

        SET @start_time = SYSDATETIME();
        ------------------------------------------------------------
        -- Clear staging table for repeatable execution
        ------------------------------------------------------------
        TRUNCATE TABLE bronze.lab_test_stage;

        ------------------------------------------------------------
        -- Bulk load CSV into staging table
        ------------------------------------------------------------
        BULK INSERT bronze.lab_test_stage
        FROM 'C:\Users\user\Desktop\DATA ENGR\Crytonyx_lab\datasets\lab_test.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows ingested from lab_test.csv';

        ------------------------------------------------------------
        -- Insert staged data into Bronze raw table with metadata
        ------------------------------------------------------------
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

        PRINT '================================================';
        PRINT 'Loading BILLING data';
        PRINT '================================================';

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

        PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows ingested from billing_invoice.csv';

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

        PRINT '================================================';
        PRINT 'Loading SHIPMENT data';
        PRINT '================================================';

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

        PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows ingested from sample_shipment.csv';

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

        SET @end_time = SYSDATETIME();
        PRINT 'SHIPMENT load duration (sec): ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);

        ------------------------------------------------------------
        -- Finalize batch timing
        ------------------------------------------------------------
        SET @batch_end_time = SYSDATETIME();

        PRINT '================================================';
        PRINT 'Bronze Load Completed Successfully';
        PRINT 'Total Duration (sec): ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR);
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        ------------------------------------------------------------
        -- Capture and print any errors during Bronze load
        ------------------------------------------------------------
        PRINT '========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '========================================';
    END CATCH
END;
GO
