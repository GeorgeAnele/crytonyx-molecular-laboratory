INSERT INTO silver.lab_test (
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
    pt_id,
    sample_number,
    LTRIM(RTRIM(test_name)),
    test_date,
    LTRIM(RTRIM(first_name)),
    LTRIM(RTRIM(last_name)),
    age,
    CASE 
        WHEN gender IN ('Male','M') THEN 'M'
        WHEN gender IN ('Female','F') THEN 'F'
        ELSE 'U'
    END AS gender,
    UPPER(LTRIM(RTRIM(marital_status))),
    LTRIM(RTRIM(country)),
    LTRIM(RTRIM(continent)),
    CASE 
        WHEN test_price_usd < 0 THEN NULL
        ELSE test_price_usd
    END,
    ingestion_id,
    source_system,
    ingested_at
FROM bronze.lab_test_raw
WHERE pt_id IS NOT NULL
  AND sample_number IS NOT NULL
  AND test_name IS NOT NULL
  AND test_date IS NOT NULL;

INSERT INTO silver.billing_invoice (
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
    invoice_number,
    pt_id,
    sample_number,
    LTRIM(RTRIM(test_name)),
    invoice_date,
    UPPER(currency),
    gross_amount_usd,
    tax_usd,
    discount_usd,
    net_amount_usd,
    UPPER(LTRIM(RTRIM(payment_status))),
    UPPER(LTRIM(RTRIM(payment_method))),
    ingestion_id,
    source_system,
    ingested_at
FROM bronze.billing_invoice_raw
WHERE invoice_number IS NOT NULL
  AND pt_id IS NOT NULL
  AND invoice_date IS NOT NULL
  AND gross_amount_usd >= 0;

INSERT INTO silver.sample_shipment (
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
    shipment_id,
    pt_id,
    sample_number,
    LTRIM(RTRIM(origin_country)),
    LTRIM(RTRIM(origin_continent)),
    LTRIM(RTRIM(courier)),
    pickup_datetime,
    delivery_datetime,
    CASE 
        WHEN delivery_datetime IS NOT NULL
             AND pickup_datetime IS NOT NULL
        THEN DATEDIFF(HOUR, pickup_datetime, delivery_datetime)
        ELSE transit_hours
    END AS transit_hours,
    shipping_cost_usd,
    UPPER(LTRIM(RTRIM(shipment_status))),
    ingestion_id,
    source_system,
    ingested_at
FROM bronze.sample_shipment_raw
WHERE shipment_id IS NOT NULL
  AND pickup_datetime IS NOT NULL;
