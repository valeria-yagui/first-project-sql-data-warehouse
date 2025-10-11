/*
=============================================================================================
Script Purpose:
	- This script creates the database 'DataWarehouse' and schemas 'bronze', 'silver', 'gold'.
=============================================================================================
*/

USE master;

CREATE DATABASE DataWarehouse

USE DataWarehouse;

-- We create the SCHEMAS that are a logical collection of database objects like tables or views.
CREATE SCHEMA bronze;
GO  -- It's a separator. Tells SQL to completely execute the first command before going to the next one.
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

