
/*
==============================================================================
Script: Silver Layer Data Quality Checks
Project: crytonyx_enterprise_dw
Object: silver.run_dq_checks
Author: George Anele
Created: 27-Dec-2025

Purpose:
    Executes standardized data quality validations on Silver-layer tables
    and records results for audit and monitoring purposes.

Scope:
    - Validates lab_test, billing_invoice, and sample_shipment tables
    - Performs null, duplicate, range, and consistency checks
    - Logs outcomes to silver.dq_log
============================================================================== 
*/
IF OBJECT_ID('silver.dq_log', 'U') IS NULL
BEGIN
    CREATE TABLE silver.dq_log (
        dq_log_id      INT IDENTITY(1,1) PRIMARY KEY,
        table_name     VARCHAR(100) NOT NULL,
        check_type     VARCHAR(100) NOT NULL,
        error_count    INT NOT NULL,
        batch_time     DATETIME2(3) NOT NULL DEFAULT SYSDATETIME(),
        notes          VARCHAR(500) NULL
    );
END;
GO

CREATE OR ALTER PROCEDURE silver.run_dq_checks
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @batch_time  DATETIME2(3) = SYSDATETIME(),
        @error_count INT,
        @total_rows  INT;

    BEGIN TRY
        PRINT '================================================';
        PRINT 'Starting Silver Layer Data Quality Checks';
        PRINT 'Batch Time: ' + CAST(@batch_time AS VARCHAR(30));
        PRINT '================================================';


        ------------------------------------------------------------------
        -- LAB TEST DQ CHECKS
        ------------------------------------------------------------------
        SELECT
            @total_rows = COUNT(*)
        FROM silver.lab_test;

        -- Mandatory field validation
        SELECT
            @error_count = COUNT(*)
        FROM silver.lab_test
        WHERE pt_id IS NULL
           OR sample_number IS NULL
           OR test_name IS NULL
           OR test_date IS NULL;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'lab_test',
            'Null Check',
            @error_count,
            @batch_time,
            'pt_id, sample_number, test_name, test_date must not be null'
        );

        PRINT 'LAB_TEST: ' + CAST(@total_rows AS VARCHAR)
            + ' rows checked, '
            + CAST(@error_count AS VARCHAR) + ' null errors';

        -- Duplicate natural key validation
        SELECT
            @error_count = COUNT(*)
        FROM
        (
            SELECT
                pt_id,
                sample_number,
                test_name,
                test_date,
                COUNT(*) AS cnt
            FROM silver.lab_test
            GROUP BY
                pt_id,
                sample_number,
                test_name,
                test_date
            HAVING COUNT(*) > 1
        ) dup;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'lab_test',
            'Duplicate Check',
            @error_count,
            @batch_time,
            'pt_id + sample_number + test_name + test_date'
        );

        PRINT 'LAB_TEST Duplicate Rows: ' + CAST(@error_count AS VARCHAR);

        -- Range validation
        SELECT
            @error_count = COUNT(*)
        FROM silver.lab_test
        WHERE age < 0
           OR age > 120
           OR test_price_usd < 0;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'lab_test',
            'Range Check',
            @error_count,
            @batch_time,
            'age 0-120, test_price_usd >=0'
        );

        PRINT 'LAB_TEST Range Errors: ' + CAST(@error_count AS VARCHAR);


        ------------------------------------------------------------------
        -- BILLING INVOICE DQ CHECKS
        ------------------------------------------------------------------
        SELECT
            @total_rows = COUNT(*)
        FROM silver.billing_invoice;

        -- Mandatory field validation
        SELECT
            @error_count = COUNT(*)
        FROM silver.billing_invoice
        WHERE invoice_number IS NULL
           OR pt_id IS NULL
           OR invoice_date IS NULL;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'billing_invoice',
            'Null Check',
            @error_count,
            @batch_time,
            'invoice_number, pt_id, invoice_date must not be null'
        );

        PRINT 'BILLING: ' + CAST(@total_rows AS VARCHAR)
            + ' rows checked, '
            + CAST(@error_count AS VARCHAR) + ' null errors';

        -- Uniqueness validation
        SELECT
            @error_count = COUNT(*)
        FROM
        (
            SELECT
                invoice_number,
                COUNT(*) AS cnt
            FROM silver.billing_invoice
            GROUP BY invoice_number
            HAVING COUNT(*) > 1
        ) dup;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'billing_invoice',
            'Duplicate Check',
            @error_count,
            @batch_time,
            'invoice_number must be unique'
        );

        PRINT 'BILLING Duplicate Rows: ' + CAST(@error_count AS VARCHAR);

        -- Amount validation
        SELECT
            @error_count = COUNT(*)
        FROM silver.billing_invoice
        WHERE gross_amount_usd < 0
           OR tax_usd < 0
           OR discount_usd < 0
           OR net_amount_usd < 0;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'billing_invoice',
            'Range Check',
            @error_count,
            @batch_time,
            'Amounts must be >=0'
        );

        PRINT 'BILLING Range Errors: ' + CAST(@error_count AS VARCHAR);


        ------------------------------------------------------------------
        -- SAMPLE SHIPMENT DQ CHECKS
        ------------------------------------------------------------------
        SELECT
            @total_rows = COUNT(*)
        FROM silver.sample_shipment;

        -- Mandatory field validation
        SELECT
            @error_count = COUNT(*)
        FROM silver.sample_shipment
        WHERE shipment_id IS NULL
           OR pt_id IS NULL
           OR pickup_datetime IS NULL;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'sample_shipment',
            'Null Check',
            @error_count,
            @batch_time,
            'shipment_id, pt_id, pickup_datetime must not be null'
        );

        PRINT 'SHIPMENT: ' + CAST(@total_rows AS VARCHAR)
            + ' rows checked, '
            + CAST(@error_count AS VARCHAR) + ' null errors';

        -- Uniqueness validation
        SELECT
            @error_count = COUNT(*)
        FROM
        (
            SELECT
                shipment_id,
                COUNT(*) AS cnt
            FROM silver.sample_shipment
            GROUP BY shipment_id
            HAVING COUNT(*) > 1
        ) dup;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'sample_shipment',
            'Duplicate Check',
            @error_count,
            @batch_time,
            'shipment_id must be unique'
        );

        PRINT 'SHIPMENT Duplicate Rows: ' + CAST(@error_count AS VARCHAR);

        -- Temporal consistency validation
        SELECT
            @error_count = COUNT(*)
        FROM silver.sample_shipment
        WHERE delivery_datetime IS NOT NULL
          AND pickup_datetime IS NOT NULL
          AND delivery_datetime < pickup_datetime;

        INSERT INTO silver.dq_log
        (
            table_name,
            check_type,
            error_count,
            batch_time,
            notes
        )
        VALUES
        (
            'sample_shipment',
            'Date Consistency',
            @error_count,
            @batch_time,
            'delivery_datetime >= pickup_datetime'
        );

        PRINT 'SHIPMENT Date Consistency Errors: ' + CAST(@error_count AS VARCHAR);

        PRINT '================================================';
        PRINT 'Silver Layer Data Quality Checks Completed Successfully';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'ERROR OCCURRED DURING SILVER DQ CHECKS';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '========================================';
    END CATCH
END;
GO
