IF OBJECT_ID('gold.dim_patient', 'V') IS NOT NULL
    DROP VIEW gold.dim_patient;
GO

CREATE VIEW gold.dim_patient AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pt_id) AS patient_sk,   -- Surrogate key
    pt_id                              AS patient_id,
    first_name,
    last_name,
    gender,
    age,
    marital_status
FROM (
    SELECT DISTINCT
        pt_id,
        first_name,
        last_name,
        gender,
        age,
        marital_status
    FROM silver.lab_test
) p;
GO

IF OBJECT_ID('gold.dim_test', 'V') IS NOT NULL
    DROP VIEW gold.dim_test;
GO

CREATE VIEW gold.dim_test AS
SELECT
    ROW_NUMBER() OVER (ORDER BY test_name) AS test_sk,
    test_name
FROM (
    SELECT DISTINCT test_name
    FROM silver.lab_test
) t;
GO


IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO

CREATE VIEW gold.dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY calendar_date) AS date_sk,
    calendar_date                              AS date,
    YEAR(calendar_date)                        AS year,
    MONTH(calendar_date)                       AS month_number,
    DATENAME(MONTH, calendar_date)             AS month_name,
    DAY(calendar_date)                         AS day_of_month,
    DATEPART(WEEKDAY, calendar_date)           AS day_of_week_number,
    DATENAME(WEEKDAY, calendar_date)           AS day_of_week_name,
    CASE 
        WHEN DATEPART(WEEKDAY, calendar_date) IN (1, 7) THEN 1 
        ELSE 0 
    END                                        AS is_weekend
FROM (
    SELECT DISTINCT test_date AS calendar_date FROM silver.lab_test
    UNION
    SELECT DISTINCT invoice_date FROM silver.billing_invoice
    UNION
    SELECT DISTINCT CAST(pickup_datetime AS DATE) FROM silver.sample_shipment
) d;
GO


IF OBJECT_ID('gold.dim_location', 'V') IS NOT NULL
    DROP VIEW gold.dim_location;
GO

CREATE VIEW gold.dim_location AS
SELECT
    ROW_NUMBER() OVER (ORDER BY country) AS location_sk,
    country,
    continent
FROM (
    SELECT DISTINCT
        country,
        continent
    FROM silver.lab_test
    WHERE country IS NOT NULL
) l;
GO


IF OBJECT_ID('gold.dim_payment', 'V') IS NOT NULL
    DROP VIEW gold.dim_payment;
GO

CREATE VIEW gold.dim_payment AS
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_method, payment_status) AS payment_sk,
    payment_method,
    payment_status
FROM (
    SELECT DISTINCT
        payment_method,
        payment_status
    FROM silver.billing_invoice
) p;
GO


IF OBJECT_ID('gold.dim_courier', 'V') IS NOT NULL
    DROP VIEW gold.dim_courier;
GO

CREATE VIEW gold.dim_courier AS
SELECT
    ROW_NUMBER() OVER (ORDER BY courier) AS courier_sk,
    courier AS courier_name
FROM (
    SELECT DISTINCT courier
    FROM silver.sample_shipment
    WHERE courier IS NOT NULL
) c;
GO


IF OBJECT_ID('gold.fact_lab_test', 'V') IS NOT NULL
    DROP VIEW gold.fact_lab_test;
GO

CREATE VIEW gold.fact_lab_test AS
SELECT
    lt.lab_test_sk        AS lab_test_id,      -- Degenerate key
    dp.patient_sk         AS patient_sk,
    dt.test_sk            AS test_sk,
    dd.date_sk            AS test_date_sk,
    dl.location_sk        AS location_sk,

    lt.sample_number      AS sample_id,        -- Degenerate dimension
    lt.test_price_usd     AS test_amount_usd
FROM silver.lab_test lt
JOIN gold.dim_patient dp
    ON lt.pt_id = dp.patient_id
JOIN gold.dim_test dt
    ON lt.test_name = dt.test_name
JOIN gold.dim_date dd
    ON lt.test_date = dd.date
LEFT JOIN gold.dim_location dl
    ON lt.country = dl.country;
GO


IF OBJECT_ID('gold.fact_billing_revenue', 'V') IS NOT NULL
    DROP VIEW gold.fact_billing_revenue;
GO

CREATE VIEW gold.fact_billing_revenue AS
SELECT
    bi.billing_sk         AS billing_id,       -- Degenerate key
    dp.patient_sk         AS patient_sk,
    dd.date_sk            AS invoice_date_sk,
    dpay.payment_sk       AS payment_sk,

    bi.invoice_number     AS invoice_id,       -- Degenerate dimension
    bi.sample_number      AS sample_id,

    bi.currency           AS billing_currency,
    bi.gross_amount_usd   AS gross_revenue_usd,
    bi.tax_usd            AS tax_amount_usd,
    bi.discount_usd       AS discount_amount_usd,
    bi.net_amount_usd     AS net_revenue_usd
FROM silver.billing_invoice bi
JOIN gold.dim_patient dp
    ON bi.pt_id = dp.patient_id
JOIN gold.dim_date dd
    ON bi.invoice_date = dd.date
LEFT JOIN gold.dim_payment dpay
    ON bi.payment_method = dpay.payment_method
   AND bi.payment_status = dpay.payment_status;
GO

IF OBJECT_ID('gold.fact_sample_shipment', 'V') IS NOT NULL
    DROP VIEW gold.fact_sample_shipment;
GO

CREATE VIEW gold.fact_sample_shipment AS
SELECT
    ss.shipment_sk            AS shipment_sk,       -- Degenerate
    dp.patient_sk             AS patient_sk,
    dd.date_sk                AS pickup_date_sk,
    dc.courier_sk             AS courier_sk,
    dl.location_sk            AS origin_location_sk,

    ss.shipment_id            AS shipment_reference_id,
    ss.sample_number          AS sample_id,

    ss.transit_hours          AS transit_duration_hours,
    ss.shipping_cost_usd      AS shipping_cost_usd
FROM silver.sample_shipment ss
JOIN gold.dim_patient dp
    ON ss.pt_id = dp.patient_id
JOIN gold.dim_date dd
    ON CAST(ss.pickup_datetime AS DATE) = dd.date
LEFT JOIN gold.dim_courier dc
    ON ss.courier = dc.courier_name
LEFT JOIN gold.dim_location dl
    ON ss.origin_country = dl.country;
GO
