Crytonyx Enterprise Data Warehouse
Data Catalog
1. Overview

The Crytonyx Enterprise Data Warehouse (EDW) is a centralized analytical platform designed to support laboratory operations, billing analytics, logistics monitoring, and executive reporting.
The warehouse follows a Medallion Architecture (Bronze → Silver → Gold) to ensure data reliability, auditability, and scalability.

This catalog documents the Gold Layer business objects and provides architectural context across all layers to support analysts, data engineers, and stakeholders.

2. Architectural Summary
Medallion Layering Strategy
Layer	Purpose
Bronze	Raw ingestion layer preserving source-system fidelity with full lineage and audit metadata
Silver	Cleansed, standardized, deduplicated, and conformed operational data
Gold	Business-ready dimensional and fact views optimized for analytics and reporting
Audit	Data quality logging, ingestion tracking, and operational diagnostics
Source Systems

CRYTONYX_LIMS – Laboratory Information Management System

CRYTONYX_BILLING – Billing and invoicing platform

CRYTONYX_LOGISTICS – Sample shipment and courier tracking system

3. Gold Layer Overview

The Gold Layer represents the semantic business model of the enterprise.
It exposes conformed dimensions and analytical fact views derived exclusively from Silver-layer tables.

Key design principles:

Star-schema orientation

Surrogate keys generated via ROW_NUMBER()

Explicit grain definitions

Read-optimized, non-mutating views

Safe for direct BI and dashboard consumption

4. Gold Layer Dimensions
4.1 gold.dim_patient

Purpose:
Stores patient demographic attributes derived from laboratory test activity.
Acts as a conformed dimension across clinical, billing, and logistics facts.

Grain: One row per unique patient (pt_id)

Column Name	Data Type	Description
patient_sk	INT	Surrogate key uniquely identifying each patient
patient_id	VARCHAR	Business identifier for the patient (source pt_id)
first_name	VARCHAR	Patient first name
last_name	VARCHAR	Patient last name
gender	CHAR	Normalized gender code (M, F, U)
age	INT	Patient age at time of test
marital_status	VARCHAR	Patient marital status

Source: silver.lab_test

4.2 gold.dim_test

Purpose:
Represents the catalog of laboratory tests performed.

Grain: One row per unique laboratory test name

Column Name	Data Type	Description
test_sk	INT	Surrogate key for the test
test_name	VARCHAR	Name of the laboratory test

Source: silver.lab_test

4.3 gold.dim_date

Purpose:
Standard calendar dimension supporting all date-based analysis across facts.

Grain: One row per calendar date

Column Name	Data Type	Description
date_sk	INT	Surrogate date key
date	DATE	Calendar date
year	INT	Calendar year
month_number	INT	Month number (1–12)
month_name	VARCHAR	Month name
day_of_month	INT	Day of month
day_of_week_number	INT	Weekday number
day_of_week_name	VARCHAR	Weekday name
is_weekend	BIT	Weekend indicator (1 = weekend)

Sources:

silver.lab_test.test_date

silver.billing_invoice.invoice_date

silver.sample_shipment.pickup_datetime

4.4 gold.dim_location

Purpose:
Provides geographic context for patient and shipment analysis.

Grain: One row per country

Column Name	Data Type	Description
location_sk	INT	Surrogate location key
country	VARCHAR	Country name
continent	VARCHAR	Continent name

Source: silver.lab_test

4.5 gold.dim_payment

Purpose:
Normalizes payment methods and statuses for revenue analytics.

Grain: One row per unique payment method + payment status combination

Column Name	Data Type	Description
payment_sk	INT	Surrogate payment key
payment_method	VARCHAR	Method of payment
payment_status	VARCHAR	Payment status

Source: silver.billing_invoice

4.6 gold.dim_courier

Purpose:
Represents courier providers used for sample shipment logistics.

Grain: One row per courier

Column Name	Data Type	Description
courier_sk	INT	Surrogate courier key
courier_name	VARCHAR	Courier company name

Source: silver.sample_shipment

5. Gold Layer Fact Views
5.1 gold.fact_lab_test

Purpose:
Captures laboratory test execution events and associated revenue potential.

Grain: One row per lab test per patient per date

Column Name	Data Type	Description
lab_test_id	INT	Surrogate identifier from Silver
patient_sk	INT	FK to dim_patient
test_sk	INT	FK to dim_test
test_date_sk	INT	FK to dim_date
location_sk	INT	FK to dim_location
sample_id	VARCHAR	Sample identifier
test_amount_usd	DECIMAL	Price of the lab test

Sources:

silver.lab_test

Conformed dimensions

5.2 gold.fact_billing_revenue

Purpose:
Stores billing and revenue metrics for financial analysis and reporting.

Grain: One row per invoice

Column Name	Data Type	Description
billing_id	INT	Surrogate billing identifier
patient_sk	INT	FK to dim_patient
invoice_date_sk	INT	FK to dim_date
payment_sk	INT	FK to dim_payment
invoice_id	VARCHAR	Invoice number
sample_id	VARCHAR	Sample identifier
billing_currency	CHAR	Currency code
gross_revenue_usd	DECIMAL	Gross revenue
tax_amount_usd	DECIMAL	Tax amount
discount_amount_usd	DECIMAL	Discount applied
net_revenue_usd	DECIMAL	Net revenue

Source: silver.billing_invoice

5.3 gold.fact_sample_shipment

Purpose:
Tracks logistics performance and shipment costs for laboratory samples.

Grain: One row per shipment

Column Name	Data Type	Description
shipment_sk	INT	Surrogate shipment identifier
patient_sk	INT	FK to dim_patient
pickup_date_sk	INT	FK to dim_date
courier_sk	INT	FK to dim_courier
origin_location_sk	INT	FK to dim_location
shipment_reference_id	VARCHAR	Shipment identifier
sample_id	VARCHAR	Sample identifier
transit_duration_hours	INT	Transit duration
shipping_cost_usd	DECIMAL	Shipment cost

Source: silver.sample_shipment

6. Data Quality & Governance Notes

Bronze and Silver layers enforce mandatory field validation, range checks, and duplicate detection

All Silver tables contain:

ingestion_id

source_system

ingested_at

Gold views inherit trusted, validated data only

Silver data quality results are logged to silver.dq_log

7. Intended Usage

This Gold layer is designed for:

BI dashboards (Power BI, Tableau, Looker)

Executive KPI reporting

Revenue, operational, and logistics analytics

Ad-hoc SQL analysis by analysts and data scientists

8. Ownership

Author: George Anele
Role: Senior / Principal Data Engineer
Project: Crytonyx Enterprise Data Warehouse

  | Column Name            | Data Type | Description                   |
| ---------------------- | --------- | ----------------------------- |
| shipment_sk            | INT       | Surrogate shipment identifier |
| patient_sk             | INT       | FK to `dim_patient`           |
| pickup_date_sk         | INT       | FK to `dim_date`              |
| courier_sk             | INT       | FK to `dim_courier`           |
| origin_location_sk     | INT       | FK to `dim_location`          |
| shipment_reference_id  | VARCHAR   | Shipment identifier           |
| sample_id              | VARCHAR   | Sample identifier             |
| transit_duration_hours | INT       | Transit duration              |
| shipping_cost_usd      | DECIMAL   | Shipment cost                 |
