/*
======================================================================================================
Stored Procedure: Load Silver Layer (bronze to silver)

Script Purpose:
	- This script creates the stored procedure that loads the cleaned and standarized data into the 'bronze' schema (ETL).
	- Before uploading the information, the tables will be truncated (load method: truncate & insert).
	  Otherwise, everythime information is inserted, it will get duplicated.
	- The time that the procedure last will be calculated by declaring DATETIME variables.
	- For error handeling 'BEGIN TRY...END TRY' and 'BEGIN CATCH...END CATCH' will be used.
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

USE DataWarehouse
GO

EXECUTE silver.load_silver;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,
			@end_time DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME

	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '==================================================';
	PRINT 'Loading Silver Layer';
	PRINT '==================================================';	
		
	PRINT '--------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '--------------------------------------------------';
				
--==================================================== CRM_CUST_INFO ====================================================
	
	SET @start_time = GETDATE();
	PRINT '>> Truncating Data Into: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr,
		cst_create_date
			)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE UPPER(TRIM(cst_marital_status))
			WHEN 'S' THEN 'Single' -- Adding UPPER(TRIM()) ensures that even lowercase values of values with extra spaces will get replaced.
			WHEN 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE UPPER(TRIM(cst_gndr)) 
			WHEN 'F' THEN 'Female'
			WHEN 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr, -- Normalize gender values to readable format
		cst_create_date
	FROM (
		SELECT*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
			) AS t
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

	PRINT '-----------------------------------------------------------------------------------------------------'

--==================================================== CRM_PRD_INFO ====================================================


	SET @start_time = GETDATE();
	PRINT '>> Truncating Data Into: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info
	PRINT '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
			)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Using LEN() because not all the prd_key have the same lenght
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost, -- For this project, the NULL values will be replaced with a 0
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt -- "PARTITION BY prd_key" makes sure the next date is from the same prd_key
	FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';


--==================================================== CRM_SLS_DETAILS ====================================================
	PRINT '-----------------------------------------------------------------------------------------------------'

	SET @start_time = GETDATE();
	PRINT '>> Truncating Data Into: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)

	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) <> 8 THEN NULL -- Checking the lenght of the date columns (should have 8 characters)
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales <> sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price) -- Absolut
			ELSE sls_sales
		END AS sls_sales,
		CASE
			WHEN sls_quantity IS NULL OR sls_quantity <=0 THEN sls_sales/ABS(sls_price)
			ELSE sls_quantity
		END AS sls_quantity,
		CASE
			WHEN sls_price IS NULL OR sls_price <=0 THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price	
	FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

	PRINT '-----------------------------------------------------------------------------------------------------'

	PRINT '--------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '--------------------------------------------------';

-- ==================================================== ERP_CUST_AZ12 ====================================================
	SET @start_time = GETDATE();
	PRINT '>> Truncating Data Into: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT
		CASE 
			WHEN SUBSTRING(cid,1,3) = 'NAS' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

	PRINT '-----------------------------------------------------------------------------------------------------'

--==================================================== ERP_LOC_A101 ====================================================
	SET @start_time = GETDATE();
	PRINT '>> Truncating Data Into: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE 
			WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
			WHEN TRIM(UPPER(cntry)) IN ('USA','US') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '-> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

	PRINT '-----------------------------------------------------------------------------------------------------'

--==================================================== ERP_PX_CAT_G1V2 ====================================================
	SET @start_time = GETDATE();
	PRINT '>> Truncating Data Into: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;

	PRINT '==================================================';
	PRINT 'Silver Layer has been loaded';
	SET @batch_end_time = GETDATE();
	PRINT '-> Silver Layer Load Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '==================================================';

	END TRY

	BEGIN CATCH
		PRINT '--------------------------------------------------';
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------------------';
	END CATCH

END
GO