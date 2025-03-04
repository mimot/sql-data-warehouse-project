--Create new database "DataWarehouse" and connect to it
USE master;
IF DB_ID('DataWarehouse') IS NOT NULL
BEGIN
	DROP DATABASE DataWarehouse;
END 
BEGIN
	CREATE DATABASE DataWarehouse;
END 

USE DataWarehouse;

--Create schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

--
