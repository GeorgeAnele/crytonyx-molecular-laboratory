/*
==============================================================================
Script: Gold Layer Analytical Queries
Project: crytonyx_enteprice_dw
Author: George Anele
Date: 27-Dec-2025

Purpose:
    This script contains curated analytical queries built on the Gold Layer
    of the Crytonyx Enterprise Data Warehouse.

    The queries are designed to support business reporting and insights across:
    - Revenue performance (daily and monthly)
    - Test-level revenue contribution
    - Sample shipment SLA monitoring
    - Payment method and status analysis

Usage Notes:
    - All queries consume Gold Layer fact and dimension views.
    - Intended for analytics, dashboards, and ad-hoc business analysis.
    - Script is read-only and safe for reporting environments.
==============================================================================
*/

-- =========================================================
-- Daily Revenue Summary
-- =========================================================
SELECT
    d.date            AS revenue_date,
    d.year            AS revenue_year,
    d.month_number    AS revenue_month,
    d.month_name      AS revenue_month_name,

    COUNT(DISTINCT f.invoice_id) AS total_invoices,
    COUNT(DISTINCT f.sample_id)  AS total_samples,

    SUM(f.gross_revenue_usd)     AS gross_revenue_usd,
    SUM(f.tax_amount_usd)        AS tax_amount_usd,
    SUM(f.discount_amount_usd)   AS discount_amount_usd,
    SUM(f.net_revenue_usd)       AS net_revenue_usd
FROM gold.fact_billing_revenue f
JOIN gold.dim_date d
    ON f.invoice_date_sk = d.date_sk
GROUP BY
    d.date,
    d.year,
    d.month_number,
    d.month_name
ORDER BY
    revenue_date;


-- =========================================================
-- Monthly Revenue Summary
-- =========================================================
SELECT
    d.year            AS revenue_year,
    d.month_number    AS revenue_month,
    d.month_name      AS revenue_month_name,

    COUNT(DISTINCT f.invoice_id) AS total_invoices,
    COUNT(DISTINCT f.sample_id)  AS total_samples,

    SUM(f.gross_revenue_usd)     AS gross_revenue_usd,
    SUM(f.tax_amount_usd)        AS tax_amount_usd,
    SUM(f.discount_amount_usd)   AS discount_amount_usd,
    SUM(f.net_revenue_usd)       AS net_revenue_usd
FROM gold.fact_billing_revenue f
JOIN gold.dim_date d
    ON f.invoice_date_sk = d.date_sk
GROUP BY
    d.year,
    d.month_number,
    d.month_name
ORDER BY
    revenue_year,
    revenue_month;


-- =========================================================
-- Revenue Contribution by Test
-- =========================================================
SELECT
    t.test_name,
    COUNT(DISTINCT f.invoice_id) AS invoice_count,
    SUM(f.net_revenue_usd)       AS total_revenue_usd
FROM gold.fact_billing_revenue f
JOIN gold.fact_lab_test flt
    ON f.sample_id = flt.sample_id     -- Bridge billing to lab tests
JOIN gold.dim_test t
    ON flt.test_sk = t.test_sk
GROUP BY
    t.test_name
ORDER BY
    total_revenue_usd DESC;


-- =========================================================
-- Shipment SLA Detail Analysis
-- =========================================================
SELECT
    s.shipment_reference_id,
    s.sample_id,
    d.date                    AS pickup_date,
    c.courier_name,
    l.country                 AS origin_country,
    l.continent               AS origin_continent,

    s.transit_duration_hours,
    72                         AS sla_target_hours,

    CASE
        WHEN s.transit_duration_hours <= 72 THEN 'Y'
        ELSE 'N'
    END AS sla_met_flag,

    CASE
        WHEN s.transit_duration_hours > 72
            THEN s.transit_duration_hours - 72
        ELSE 0
    END AS delay_hours,

    s.shipping_cost_usd
FROM gold.fact_sample_shipment s
JOIN gold.dim_date d
    ON s.pickup_date_sk = d.date_sk
LEFT JOIN gold.dim_courier c
    ON s.courier_sk = c.courier_sk
LEFT JOIN gold.dim_location l
    ON s.origin_location_sk = l.location_sk
ORDER BY
    pickup_date;


-- =========================================================
-- Shipment SLA Compliance Summary
-- =========================================================
SELECT
    COUNT(*) AS total_shipments,

    SUM(
        CASE
            WHEN transit_duration_hours <= 72 THEN 1
            ELSE 0
        END
    ) AS sla_met_shipments,

    CAST(
        100.0 * SUM(
            CASE
                WHEN transit_duration_hours <= 72 THEN 1
                ELSE 0
            END
        ) / COUNT(*) AS DECIMAL(5,2)
    ) AS sla_compliance_percentage
FROM gold.fact_sample_shipment;


-- =========================================================
-- Revenue by Payment Method and Status
-- =========================================================
SELECT
    p.payment_method,
    p.payment_status,

    COUNT(DISTINCT f.invoice_id) AS invoice_count,
    SUM(f.net_revenue_usd)       AS total_revenue_usd
FROM gold.fact_billing_revenue f
JOIN gold.dim_payment p
    ON f.payment_sk = p.payment_sk
GROUP BY
    p.payment_method,
    p.payment_status
ORDER BY
    total_revenue_usd DESC;
