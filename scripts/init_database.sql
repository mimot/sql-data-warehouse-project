--Create new database "DataWarehouse" and connect to it
USE master;
IF DB_ID('AWorks_DW') IS NOT NULL
BEGIN
	DROP DATABASE AWorks_DW;
END 
BEGIN
	CREATE DATABASE AWorks_DW;
END 
GO

USE AWorks_DW;

--Create schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

--
