/*
==========================================================================================================
Data Quality Check
	- These are the scripts that were used to analyze the data in the 'bronze' layer before inserting it 
	into the 'silver' layer.
	- Checks include:
		* NULL or duplicates.
		* Unwanted spaces.
		* Data standardization and consistency.
		* Invalid date ranges and orders.
==========================================================================================================
*/

--==================================================== CRM_CUST_INFO ====================================================
	-->> Checking for nulls or duplicates in primary key
		-- Expectation: No results
		-- Process: Evaluate which cst_id are duplicated by counting them.

		SELECT cst_id,
		COUNT(*) AS 'total duplicates'
		FROM bronze.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(*) > 1 OR cst_id IS NULL;

		-- Fixing the duplicated ids
		-- First, the duplicated ids will be first ranked by date DESC:
	   
		SELECT*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last -- 'PARTITION BY cst_id' ensures that 
		FROM bronze.crm_cust_info
		WHERE cst_id IN (
			SELECT cst_id
			FROM bronze.crm_cust_info
			GROUP BY cst_id
			HAVING COUNT(*) > 1
					)
		OR cst_id IS NULL;  -- Adding "OR cst_id IS NULL" ensures that even when there is only a single NULL, this value will be counted.

	   -- Then, only the results that are ranked 1 will be selected:
		SELECT*
		FROM (
			SELECT*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
					) AS t
		WHERE flag_last = 1;
		
	-->> Checking for unwanted spaces
		--   Expectation: No results
		
		SELECT cst_key
		FROM bronze.crm_cust_info
		WHERE cst_key <> TRIM(cst_key);

		SELECT cst_firstname
		FROM bronze.crm_cust_info
		WHERE cst_firstname <> TRIM(cst_firstname);

		SELECT cst_lastname
		FROM bronze.crm_cust_info
		WHERE cst_lastname <> TRIM(cst_lastname);

		SELECT cst_gndr
		FROM bronze.crm_cust_info
		WHERE cst_gndr <> TRIM(cst_gndr);

		SELECT cst_marital_status
		FROM bronze.crm_cust_info
		WHERE cst_marital_status <> TRIM(cst_marital_status);

	-->> Data Standardization & Consistency
		-- For the 'gender' and 'marital status' CASE...WHEN will be used to convert the values.
		-- 'F' = 'Female'
		-- 'M' = 'Male'
		-- 'S' = 'Single'
		-- 'M' = 'Married'
		-- ELSE 'n/a'

		SELECT DISTINCT cst_gndr
		FROM bronze.crm_cust_info;

		SELECT DISTINCT cst_marital_status
		FROM bronze.crm_cust_info;


--==================================================== CRM_PRD_INFO ====================================================
	-->> Checking for nulls or duplicates in primary key
		-- Expectation: No results
		-- Process: Evaluate which cst_id are duplicated by counting them.

		SELECT prd_id,
		COUNT(*) AS 'total duplicates'
		FROM bronze.crm_prd_info
		GROUP BY prd_id
		HAVING COUNT(*) > 1 OR prd_id IS NULL;

	-->> Checking for unwanted spaces
		-- Expectation: No results

		SELECT prd_key
		FROM bronze.crm_prd_info
		WHERE prd_key <> TRIM(prd_key);

		SELECT prd_nm
		FROM bronze.crm_prd_info
		WHERE prd_nm <> TRIM(prd_nm);
	
	-->> Checking for negative or NULL.
		--   Expectation: No results
		--   There are NULL costs

		SELECT prd_cost
		FROM bronze.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL;

	-->> Data Standardization & Consistency
		-- For the 'product' CASE...WHEN will be used to convert the values.
		-- 'M' = 'Mountain'
		-- 'R' = 'Road'
		-- 'S' = 'Other sales'
		-- 'T' = 'Touring'
		-- ELSE 'n/a'

		SELECT DISTINCT prd_line
		FROM bronze.crm_prd_info;

	-->> Checking for invalid orders: Checking if there are cases where the start date of a price record is after the end date.
		--   Expectation: No results

		SELECT*
		FROM bronze.crm_prd_info
		WHERE prd_end_dt < prd_start_dt;

		-- Fixing date inconsistencies:
		-- In some cases, the start date of a price record is later than its end date.
		-- To correct this, the end date of each price record will be adjusted to match
		-- the start date of the next record for the same product, minus one day.
		-- This ensures that price periods do not overlap and remain in chronological order.

		-- Testing to fix the end dates:

		SELECT
		prd_id,
		prd_key,
		prd_nm,
		prd_start_dt,
		prd_end_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS prd_end_dt_test -- "PARTITION BY prd_key" makes sure the next date is from the same prd_key
		FROM bronze.crm_prd_info
		WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


--==================================================== CRM_SLS_DETAILS ====================================================
	-->> Check for nulls or duplicates in primary key
		--   Expectation: No results

		SELECT sls_ord_num
		FROM bronze.crm_sales_details
		WHERE sls_ord_num <> TRIM(sls_ord_num);

		SELECT sls_prd_key
		FROM bronze.crm_sales_details
		WHERE sls_prd_key <> TRIM(sls_prd_key);

	-->> Cheking prd_key and cst_id
		--   Expectation: No results

		SELECT *
		FROM bronze.crm_sales_details
		WHERE sls_prd_key NOT IN (
			SELECT prd_key
			FROM silver.crm_prd_info -- Using the silver layer that is already created and clean information
			);

		SELECT *
		FROM bronze.crm_sales_details
		WHERE sls_cust_id NOT IN (
			SELECT cst_id
			FROM silver.crm_cust_info
			);

	--> Checking for Invalid Dates
		--   Expectation: No results
		
		SELECT
		NULLIF(sls_order_dt,0) AS sls_order_dt -- NULLIF: if sls_order_dt = 0, then NULL
		FROM bronze.crm_sales_details
		WHERE 
			sls_order_dt <= 0
			OR LEN(sls_order_dt) <> 8 -- Checking the length of the date columns (should have 8 characters)
			OR sls_order_dt > 20500101
			OR sls_order_dt < 19900101;-- For this project, the maxium date should be 01-01-2025 and minimun date should be 01-01-1990

		SELECT
		NULLIF(sls_ship_dt,0) AS sls_ship_dt 
		FROM bronze.crm_sales_details
		WHERE 
			sls_ship_dt <= 0
			OR LEN(sls_ship_dt) <> 8
			OR sls_ship_dt > 20500101
			OR sls_ship_dt < 19900101;
	
		SELECT
		NULLIF(sls_due_dt,0) AS sls_due_dt
		FROM bronze.crm_sales_details
		WHERE 
			sls_due_dt <= 0
			OR LEN(sls_due_dt) <> 8
			OR sls_due_dt > 20500101
			OR sls_due_dt < 19900101;

	--> Checking the order date is earlier than the shipping or due date.
		--   Expectation: No results

		SELECT *
		FROM bronze.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
 
 	--> Checking for data inconsistencies among sales, quantity, and price.
		--   Expectation: No results

		SELECT 
		sls_sales,
		sls_quantity,
		sls_price
		FROM bronze.crm_sales_details
		WHERE sls_sales <> sls_quantity * sls_price
			OR sls_sales IS NULL
			OR sls_quantity IS NULL
			OR sls_price IS NULL
			OR sls_sales <= 0
			OR sls_quantity <= 0
			OR sls_price <= 0
		ORDER BY sls_sales,	sls_quantity,sls_price

		-- - Fixing inconsistencies:
		-- For this project, the following solutions will be applied:
		-- if the sls_sales is negative, zero, or null, it will be calculated by  multiplying sls_quantity * sls_price
		-- if the price is zero or null, it will be calculated as sls_sales/sls_quantity
		-- if the price < 0, it will be converted to a positive value.
		-- Test to fix inconsistencies:

 		SELECT
		sls_sales AS OLD_SALES,
		sls_price AS OLD_PRICE,
		sls_quantity AS OLD_QUANTITIES,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales <> sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price) -- Absolut
			ELSE sls_sales
		END AS sls_sales,
		CASE
			WHEN sls_price IS NULL OR sls_price <=0 THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price,
		CASE
			WHEN sls_quantity IS NULL OR sls_quantity <=0 THEN sls_sales/ABS(sls_price)
			ELSE sls_quantity
		END AS sls_quantity
		FROM bronze.crm_sales_details
	
-- ==================================================== ERP_CUST_AZ12 ====================================================
	--> Checking ids match
		-- By observing the data, it is clear that Cid does not match cst_key in the crm_cust_info because it has extra characters at the beginning.
		-- To fix this CASE... WHEN will be used.

		SELECT
		cid
		FROM bronze.erp_cust_az12;

		SELECT 
		cst_key
		FROM bronze.crm_cust_info;

	--> Identifying Out-of-Range Dates
		-- For this project, when the birthdate day is older than today's date, the date will be changed to NULL.

		SELECT
		bdate
		FROM bronze.erp_cust_az12
		WHERE bdate > GETDATE();

	-->> Data Standardization & Consistency
		-- For the 'gender' CASE...WHEN will be used to convert the values.
		-- 'F' = 'Female'
		-- 'M' = 'Male'

		SELECT DISTINCT
		gen
		FROM bronze.erp_cust_az12;

	
--==================================================== ERP_LOC_A101 ====================================================
	--> Checking ids match
		-- Cid does not match cst_key in the crm_cust_info because it has extra '-'
		-- REAPLCE will be used to fix this issue.

		SELECT cid
		FROM bronze.erp_loc_a101;

	--> Checking matching data with silver.crm_cust_info
		-- Expectation: No results

		SELECT 
		REPLACE(cid,'-','') AS cid
		FROM bronze.erp_loc_a101
		WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info);

	-->> Data Standardization & Consistency
		-- Different values for the same country
		-- For the 'country' CASE TRIM(UPPER())...WHEN will be used to convert the values.

		SELECT DISTINCT cntry
		FROM bronze.erp_loc_a101;

--==================================================== ERP_PX_CAT_G1V2 ====================================================
	-->> Checking for unwanted spaces
		--   Expectation: No results
		
		SELECT cat
		FROM bronze.erp_px_cat_g1v2
		WHERE cat <> TRIM(cat);

		SELECT subcat
		FROM bronze.erp_px_cat_g1v2
		WHERE subcat <> TRIM(subcat);

		SELECT maintenance
		FROM bronze.erp_px_cat_g1v2
		WHERE maintenance <> TRIM(maintenance);

	-->> Data Standardization & Consistency
		-- For this project, the category, subcategory, and maintenance will be left the same.

		SELECT DISTINCT cat
		FROM bronze.erp_px_cat_g1v2;

		SELECT DISTINCT subcat
		FROM bronze.erp_px_cat_g1v2;
	
		SELECT DISTINCT maintenance
		FROM bronze.erp_px_cat_g1v2;
	
