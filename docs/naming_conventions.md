# Crytonyx Enterprise Data Warehouse  
## Naming Conventions

---

## 1. Purpose

This document defines the official naming standards for all database objects within the Crytonyx Enterprise Data Warehouse (EDW).  
The objective is to ensure clarity, consistency, scalability, and long-term maintainability across data engineering, analytics, and business intelligence workloads.

These conventions apply to all current and future development.

---

## 2. General Naming Principles

All database objects must adhere to the following rules:

- Use lowercase naming for all schemas and objects
- Use snake_case (underscores) to separate words
- Avoid abbreviations unless they are industry-standard
- Names must be descriptive, deterministic, and unambiguous
- Avoid reserved SQL keywords
- Avoid environment-specific identifiers in object names

---

## 3. Database and Schema Naming

### 3.1 Database Naming

| Object | Convention | Example |
|------|------------|---------|
| Data warehouse database | `<domain>_dw` | `crytonyx_dw` |

---

### 3.2 Schema Naming

| Schema | Purpose |
|------|--------|
| staging | Temporary ingestion and validation tables |
| bronze | Raw, immutable source-aligned data |
| silver | Cleansed, standardized, and conformed data |
| gold | Business-ready analytical views |
| audit | Ingestion, validation, and quality logs |
| util | Utility and helper objects |

---

## 4. Table Naming Conventions

### 4.1 Bronze Layer Tables

**Pattern**  
`bronze.<source_entity>_raw`

**Examples**
- `bronze.lab_test_raw`
- `bronze.billing_invoice_raw`
- `bronze.sample_shipment_raw`

**Rules**
- Preserve source-system structure
- No derived or calculated columns
- Append-only or truncate-and-reload patterns only

---

### 4.2 Silver Layer Tables

**Pattern**  
`silver.<business_entity>`

**Examples**
- `silver.lab_test`
- `silver.billing_invoice`
- `silver.sample_shipment`

**Rules**
- Apply standardized column names
- Deduplicate using business keys
- Enforce data types and constraints
- Include lineage and audit metadata

---

### 4.3 Gold Layer Objects

#### 4.3.1 Dimension Views

**Pattern**  
`gold.dim_<entity>`

**Examples**
- `gold.dim_patient`
- `gold.dim_test`
- `gold.dim_date`
- `gold.dim_location`
- `gold.dim_payment`
- `gold.dim_courier`

---

#### 4.3.2 Fact Views

**Pattern**  
`gold.fact_<business_process>`

**Examples**
- `gold.fact_lab_test`
- `gold.fact_billing_revenue`
- `gold.fact_sample_shipment`

---

## 5. Column Naming Conventions

### 5.1 Primary Keys

| Type | Pattern | Example |
|----|--------|---------|
| Surrogate key | `<entity>_sk` | `patient_sk` |
| Business / natural key | `<entity>_id` | `patient_id` |

---

### 5.2 Foreign Keys

**Pattern**  
`<referenced_entity>_sk`

**Examples**
- `patient_sk`
- `date_sk`
- `test_sk`
- `location_sk`

---

### 5.3 Date and Time Columns

| Usage | Convention | Example |
|----|----------|---------|
| Date dimension key | `<event>_date_sk` | `invoice_date_sk` |
| Date value | `<event>_date` | `test_date` |
| Timestamp | `<event>_datetime` | `pickup_datetime` |

---

### 5.4 Boolean Columns

**Pattern**  
`is_<condition>`

**Examples**
- `is_weekend`
- `is_active`
- `is_valid`

---

### 5.5 Measure Columns

**Rules**
- Must be numeric
- Must include unit or currency
- Must be additive unless documented otherwise

**Examples**
- `test_amount_usd`
- `gross_revenue_usd`
- `net_revenue_usd`
- `shipping_cost_usd`
- `transit_duration_hours`

---

## 6. Stored Procedure Naming

### 6.1 General Pattern

`sp_<layer>_<action>_<entity>`

---

### 6.2 Bronze Ingestion Procedures

**Pattern**  
`sp_bronze_ingest_<entity>`

**Examples**
- `sp_bronze_ingest_lab_test`
- `sp_bronze_ingest_billing_invoice`
- `sp_bronze_ingest_sample_shipment`

---

### 6.3 Silver Transformation Procedures

**Pattern**  
`sp_silver_transform_<entity>`

**Examples**
- `sp_silver_transform_lab_test`
- `sp_silver_transform_billing_invoice`
- `sp_silver_transform_sample_shipment`

---

### 6.4 Gold Build Procedures

**Pattern**  
`sp_gold_build_<object>`

**Examples**
- `sp_gold_build_dim_patient`
- `sp_gold_build_fact_lab_test`
- `sp_gold_build_fact_billing_revenue`

---

## 7. View Naming Conventions

**Pattern**  
`vw_<layer>_<entity>`

**Examples**
- `vw_gold_fact_lab_test`
- `vw_gold_dim_patient`

---

## 8. Index and Constraint Naming

### 8.1 Primary Keys

`pk_<table_name>`

### 8.2 Foreign Keys

`fk_<table_name>_<referenced_table>`

### 8.3 Indexes

`idx_<table_name>_<column_list>`

---

## 9. Audit and Logging Tables

**Pattern**  
`audit.<process>_log`

**Examples**
- `audit.ingestion_log`
- `audit.transformation_log`
- `audit.dq_log`

---

## 10. Temporary Objects

### 10.1 Temporary Tables

`#tmp_<purpose>`

### 10.2 Common Table Expressions

`cte_<description>`

---

## 11. Deprecated Objects

**Pattern**  
`<object_name>_deprecated`

**Rules**
- Must include a comment explaining:
  - Reason for deprecation
  - Replacement object
  - Planned removal date

---

## 12. Enforcement

- All pull requests must comply with this standard
- Deviations require explicit architectural approval
- Automated reviews and linting should enforce compliance

---

## 13. Ownership

**Author:** George Anele  
**Role:**   Data Engineer  
**Project:** Crytonyx Enterprise Data Warehouse
