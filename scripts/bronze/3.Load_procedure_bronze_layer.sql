/*
======================================================================================================
Stored Procedure: Load Bronze Layer

Script Purpose:
	- This script creates the stored procedure that loads the data into the 'bronze' schema.
	- Before uploading the information, the tables will be truncated (load method: truncate & insert).
	  Otherwise, every time information is inserted, it will be duplicated.
	- 'BULK INSERT' will be used to load the data from csv files to the 'bronze' tables
	- The time that the procedure lasts will be calculated by declaring DATETIME variables.
	- For error handling 'BEGIN TRY...END TRY' and 'BEGIN CATCH...END CATCH' will be used.
		Example:
		BEGIN TRY
			-- Code you want to run
			-- If an error happens, control jumps to the CATCH block
		END TRY
		BEGIN CATCH
			-- Code that runs if an error occurs in the TRY block
		END CATCH
======================================================================================================

*/
EXECUTE bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,
			@end_time DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME
	
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '==================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==================================================';


		PRINT '--------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------';

--==================================================== CRM_CUST_INFO ====================================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\VALERIA\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- The first row is the header so we skip it.
			FIELDTERMINATOR = ',',
			TABLOCK -- This makes the bulk insert run faster because it locks the entire table (nobody else can read or write to the table during the load).
			);
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

	PRINT '-----------------------------------------------------------------------------------------------------'

--==================================================== CRM_PRD_INFO ====================================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\VALERIA\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';


--==================================================== CRM_SLS_DETAILS ====================================================
	PRINT '-----------------------------------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\VALERIA\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

	PRINT '-----------------------------------------------------------------------------------------------------'




		PRINT '--------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------';

-- ==================================================== ERP_CUST_AZ12 ====================================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\VALERIA\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

	PRINT '-----------------------------------------------------------------------------------------------------'

--==================================================== ERP_LOC_A101 ====================================================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\VALERIA\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

	PRINT '-----------------------------------------------------------------------------------------------------'

--==================================================== ERP_PX_CAT_G1V2 ====================================================
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\VALERIA\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

	PRINT '==================================================';
	PRINT 'Bronze Layer has been loaded';
	SET @batch_end_time = GETDATE();
	PRINT '-> Bronze Layer Load Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '==================================================';

	END TRY

	BEGIN CATCH
		PRINT '--------------------------------------------------';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------------------';
	END CATCH

END
GO
