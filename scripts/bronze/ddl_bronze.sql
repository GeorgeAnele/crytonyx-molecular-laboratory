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

    ingestion_id         UNIQUEIDENTIFIER,
    source_file_name     VARCHAR(255),
    source_system        VARCHAR(50),
    ingested_at          DATETIME2(3)
);
GO
