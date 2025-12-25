USE master;
GO

/* =========================================================
   NON-PRODUCTION ONLY
   This script DROPS and RECREATES the database.
   Intended for DEV / PORTFOLIO environments only.
   ========================================================= */

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

/* =========================================================
   Schema Layering (Medallion Architecture)
   ========================================================= */

-- Raw ingestion layer
CREATE SCHEMA bronze;
GO

-- Cleansed and conformed layer
CREATE SCHEMA silver;
GO

-- Analytics and reporting layer
CREATE SCHEMA gold;
GO

-- Data quality, lineage, and load tracking
CREATE SCHEMA audit;
GO

-- Reference and static datasets
CREATE SCHEMA ref;
GO
