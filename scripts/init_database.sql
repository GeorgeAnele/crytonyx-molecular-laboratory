/*
==============================================================================
Project: crytonyx_enteprice_dw
Script: Database and Schema Initialization
Author: George Anele
Date: 26-Dec-2025

Purpose:
    This script initializes the Crytonyx Enterprise Data Warehouse by:
    - Dropping and recreating the database (non-production only)
    - Establishing schema layers aligned to the medallion architecture

Usage Notes:
    - Intended strictly for development, testing, or portfolio environments.
    - Executing this script will permanently delete all existing data.
==============================================================================
*/

USE master;
GO

-- NON-PRODUCTION ONLY: Drop and recreate database to ensure a clean baseline
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'crytonyx_enterprise_dw')
BEGIN
    ALTER DATABASE crytonyx_enterprise_dw
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    DROP DATABASE crytonyx_enterprise_dw;
END;
GO

-- Create Crytonyx Enterprise Data Warehouse
CREATE DATABASE crytonyx_enterprise_dw;
GO

USE crytonyx_enterprise_dw;
GO

-- Schema layering following the medallion architecture pattern

-- Raw ingestion and landing zone
CREATE SCHEMA bronze;
GO

-- Cleansed, standardized, and conformed data
CREATE SCHEMA silver;
GO

-- Business-ready analytics and reporting layer
CREATE SCHEMA gold;
GO

-- Data quality checks, lineage, and load auditing
CREATE SCHEMA audit;
GO

-- Reference and static lookup datasets
CREATE SCHEMA ref;
GO
