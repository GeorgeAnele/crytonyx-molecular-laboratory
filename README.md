# 🧠 Crytonyx Enterprise Data Warehouse & Analytics Project

Welcome to the **Crytonyx Enterprise Data Warehouse and Analytics Project** — an enterprise-grade data platform designed to demonstrate **end-to-end data engineering excellence** within a healthcare and laboratory analytics context.

This repository captures the **architecture, design, and implementation** of a modern **data warehouse and analytics solution** supporting laboratory operations, billing, logistics, and performance reporting.

It reflects my commitment to building **auditable, scalable, and business-aligned data systems** using industry best practices.

---

## 🏗️ Data Architecture

This project adopts the **Medallion Architecture** (Bronze → Silver → Gold), a proven enterprise framework that enforces **data reliability, lineage, and analytical performance**.

| **Layer**     | **Purpose**         | **Description**                                                                 |
|--------------|---------------------|---------------------------------------------------------------------------------|
| 🥉 **Bronze** | Raw Data            | Ingests unaltered laboratory, billing, and logistics data from CSV-based sources |
| 🥈 **Silver** | Cleansed Data       | Applies data cleansing, standardization, deduplication, and validation rules    |
| 🥇 **Gold**   | Business-Ready Data | Curated star-schema views optimized for analytics, KPIs, and BI consumption     |

📊 *Architecture Diagram: `docs/data_architecture.drawio`*

---

## 📖 Project Overview

The Crytonyx EDW integrates **data engineering**, **data modeling**, and **analytics engineering** into a single cohesive ecosystem designed for healthcare laboratory intelligence.

### 🔹 Core Deliverables

1. **Enterprise Data Architecture**
   - Implemented a layered Medallion architecture ensuring clear separation of concerns.
   - Enforced strict data lineage from source systems to analytics outputs.

2. **Stored Procedure–Driven ETL Pipelines**
   - Modular SQL-based ingestion, transformation, and load procedures.
   - Deterministic, restartable, and auditable pipelines across all layers.

3. **Analytical Data Modeling**
   - Designed **Star Schema models** with conformed dimensions and atomic fact tables.
   - Optimized for reporting on test volumes, turnaround times, revenue, and logistics.

4. **Data Quality & Governance**
   - Embedded validation checkpoints, audit logging, and referential integrity controls.
   - Ensured traceability and compliance readiness.

5. **Business & Operational Analytics**
   - Enabled insights into laboratory performance, revenue generation, and operational efficiency.
   - Delivered business-ready datasets for dashboards and executive reporting.

---

## ⚙️ Tools & Technologies

| **Category**                 | **Tools / Technologies**                |
|------------------------------|-----------------------------------------|
| **Database & Querying**      | Microsoft SQL Server, SQL Server Studio  |
| **Data Modeling**            | Draw.io                                 |
| **ETL & Transformation**     | SQL Stored Procedures                   |
| **Version Control**          | Git & GitHub                            |
| **Data Sources**             | Laboratory, Billing, Logistics (CSV)    |
| **Documentation & Planning** | Notion                                  |
| **Architecture Framework**   | Medallion (Bronze → Silver → Gold)      |

---

## 📂 Repository Structure

crytonyx-enterprise-dw/
│
├── datasets/                     # Raw source data (CSV extracts)
│
├── docs/                         # Architecture & design documentation
│   ├── data_architecture.drawio  # Medallion architecture overview
│   ├── data_models.drawio        # Star schema & analytical models
│   ├── naming-conventions.md     # Enterprise naming standards
│   └── data_catalog.md           # Gold-layer metadata definitions
│
├── scripts/                      # SQL scripts and stored procedures
│   ├── bronze/                   # Raw ingestion and source-aligned loads
│   ├── silver/                   # Data cleansing, standardization, validation
│   └── gold/                     # Dimensions, facts, and analytics views
│
├── tests/                        # Data quality and validation scripts
│
├── README.md                     # Project overview and documentation entry point
├── LICENSE                       # License information
└── .gitignore                    # Git ignore rules


---

## 🔍 Data Quality Validation

| **Validation Type**       | **Purpose**                   | **Example / Implementation**             |
|---------------------------|-------------------------------|------------------------------------------|
| **Null Checks**           | Ensures completeness          | `WHERE patient_id IS NULL`               |
| **Data Type Validation**  | Schema enforcement            | CAST strings → DATE / INT / DECIMAL      |
| **Duplicate Detection**   | Prevents double counting      | Business-key deduplication               |
| **Referential Integrity** | Ensures relational accuracy   | Fact → Dimension foreign keys            |
| **Business Rule Checks**  | Logical correctness           | `amount > 0`, `test_date <= GETDATE()`   |
| **Audit Columns**         | ETL traceability              | `dwh_load_datetime`, `dwh_batch_id`      |

Each layer enforces **quality gates before promotion**, ensuring only trusted data reaches the Gold layer.

---

## 📘 Documentation Highlights

- **`naming-conventions.md`**  
  Enterprise-standard naming rules for schemas, tables, columns, and procedures.

- **`data_catalog.md`**  
  Business and technical metadata for Gold-layer dimensions and facts.

- **`data_models.drawio`**  
  Logical and physical star schema diagrams.

---

## 👨🏽‍💻 About Me

Hi, I’m **George (Chinedu) Anele**, a **Medical Laboratory Scientist in love with data** with a strong passion for building **robust, scalable, and insight-driven data platforms**, particularly in healthcare and enterprise analytics.

I specialize in:

- Data Warehousing & Architecture Design  
- SQL & Analytical Data Modeling  
- Stored Procedure–Driven ETL Pipelines  
- Data Governance, Auditing & Quality Assurance  
- Analytics Engineering & KPI Design  

My background allows me to bridge **clinical domain expertise** with **engineering rigor**, delivering systems that are both technically sound and business-relevant.

📍 Based in **Nigeria** | 🌍 Delivering global data solutions

---

## 🔗 Connect with Me

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/chinedu-anele-b46464194)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/GeorgeAnele)
[![WhatsApp](https://img.shields.io/badge/WhatsApp-25D366?style=for-the-badge&logo=whatsapp&logoColor=white)](https://wa.me/2348123001381)

---

## 🛡️ License

This project is licensed under the **MIT License**.  
You are free to use, modify, and reference this work with proper attribution.

---

## 🙏 Appreciation

Special appreciation goes to **Baraa Khatib Salkini**, whose educational resources and open-source contributions provided valuable guidance and inspiration throughout the design and implementation of this data warehouse.

---

## 🌟 Closing Note

This project reflects how I approach **data engineering as both a discipline and a craft** — combining **architectural discipline, technical depth, and business empathy** to transform raw data into strategic value.

> “Great data engineering isn’t just about moving data — it’s about moving organizations toward better decisions.”
