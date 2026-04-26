/*
===========================================================================================================================
Database and Schema

Caution: This statement will drop and recreate the database
===========================================================================================================================
*/


USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases
			WHERE name = 'fintech_datawarehouse')
BEGIN
	ALTER DATABASE fintech_datawarehouse
	SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE fintech_datawarehouse;
END;
GO

-- Create fintech_datawarehouse Database

CREATE DATABASE fintech_datawarehouse
GO

USE fintech_datawarehouse

GO

-- Create Schema
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO