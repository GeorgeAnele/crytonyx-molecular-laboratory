/*
==============================================================================
Script: Executive KPI Summary
Project: crytonyx_enteprice_dw
Author: George Anele
Date: 27-Dec-2025

Purpose:
    Provides a consolidated, high-level KPI snapshot across revenue,
    operations, and logistics using Gold Layer fact tables.

    Intended for executive dashboards and management reporting.
==============================================================================
*/

SELECT
    -- =====================================================
    -- Revenue KPIs
    -- =====================================================
    COUNT(DISTINCT fbr.invoice_id)     AS total_invoices,
    COUNT(DISTINCT fbr.sample_id)      AS total_samples_billed,

    SUM(fbr.gross_revenue_usd)         AS gross_revenue_usd,
    SUM(fbr.tax_amount_usd)            AS tax_amount_usd,
    SUM(fbr.discount_amount_usd)       AS discount_amount_usd,
    SUM(fbr.net_revenue_usd)           AS net_revenue_usd,


    -- =====================================================
    -- Operational KPIs
    -- =====================================================
    COUNT(DISTINCT flt.lab_test_id)     AS total_tests_performed,


    -- =====================================================
    -- Logistics / SLA KPIs
    -- =====================================================
    COUNT(fss.shipment_reference_id)    AS total_shipments,

    SUM(
        CASE
            WHEN fss.transit_duration_hours <= 72 THEN 1
            ELSE 0
        END
    ) AS sla_met_shipments,

    CAST(
        100.0
        * SUM(
            CASE
                WHEN fss.transit_duration_hours <= 72 THEN 1
                ELSE 0
            END
        )
        / NULLIF(COUNT(fss.shipment_reference_id), 0)
        AS DECIMAL(5,2)
    ) AS sla_compliance_percentage

FROM gold.fact_billing_revenue fbr
LEFT JOIN gold.fact_lab_test flt
    ON fbr.sample_id = flt.sample_id       -- Link billing to tests
LEFT JOIN gold.fact_sample_shipment fss
    ON fbr.sample_id = fss.sample_id;      -- Link billing to shipments
