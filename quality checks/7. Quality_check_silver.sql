/*
==========================================================================================================
Data Quality Check
	- These are the scripts that were used to analyze the data in the 'silver' layer after it was inserted
	from the 'bronze' layer
	- Checks include:
		* NULL or duplicates.
		* Unwanted spaces.
		* Data standardization and consistency.
		* Invalid date ranges and orders.
==========================================================================================================
*/	
	
--==================================================== CRM_CUST_INFO ====================================================

	-->> Check for nulls or duplicates in primary key
		-- Expectation: No results

		SELECT cst_id,
		COUNT(*) AS 'total duplicates'
		FROM silver.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(*) > 1 OR cst_id IS NULL;

	-->> Check for unwanted spaces
		-- Expectation: No results

		SELECT cst_key
		FROM silver.crm_cust_info
		WHERE cst_key <> TRIM(cst_key);

		SELECT cst_firstname
		FROM silver.crm_cust_info
		WHERE cst_firstname <> TRIM(cst_firstname);

		SELECT cst_lastname
		FROM silver.crm_cust_info
		WHERE cst_lastname <> TRIM(cst_lastname);

		SELECT cst_gndr
		FROM silver.crm_cust_info
		WHERE cst_gndr <> TRIM(cst_gndr);

		SELECT cst_marital_status
		FROM silver.crm_cust_info
		WHERE cst_marital_status <> TRIM(cst_marital_status);

	-->> Data Standardization & Consistency

		SELECT DISTINCT cst_gndr
		FROM silver.crm_cust_info;

		SELECT DISTINCT cst_marital_status
		FROM silver.crm_cust_info;

		SELECT*
		FROM silver.crm_cust_info;


--==================================================== CRM_PRD_INFO ====================================================

	-->> Check for nulls or duplicates in primary key
		--	 Expectation: No results
		-- Evaluate which cst_id are duplicated by counting them.

		SELECT prd_id,
		COUNT(*) AS 'total duplicates'
		FROM silver.crm_prd_info
		GROUP BY prd_id
		HAVING COUNT(*) > 1 OR prd_id IS NULL;


	-->> Check for unwanted spaces
		--   Expectation: No results

		SELECT prd_key
		FROM silver.crm_prd_info
		WHERE prd_key <> TRIM(prd_key);

		SELECT prd_nm
		FROM silver.crm_prd_info
		WHERE prd_nm <> TRIM(prd_nm);
	
	-->> Check for negative cost.
		--   Expectation: No results

		SELECT prd_cost
		FROM silver.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL;

	-->> Data Standardization & Consistency

		SELECT DISTINCT prd_line
		FROM silver.crm_prd_info;


	-->> Check for invalid orders
		--   Checking if there are cases where the start date is after the end date.

		SELECT*
		FROM silver.crm_prd_info
		WHERE prd_end_dt < prd_start_dt;

		SELECT*
		FROM silver.crm_prd_info;
			
-->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> CRM_SLS_INFO <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	-->> Check for nulls or duplicates in primary key
		--   Expectation: No results
	
		SELECT sls_ord_num
		FROM silver.crm_sales_details
		WHERE sls_ord_num <> TRIM(sls_ord_num);

		SELECT sls_prd_key
		FROM silver.crm_sales_details
		WHERE sls_prd_key <> TRIM(sls_prd_key);

	-->> Cheking prd_key and cst_id
		--   Expectation: No results

		SELECT *
		FROM silver.crm_sales_details
		WHERE sls_prd_key NOT IN (
			SELECT prd_key
			FROM silver.crm_prd_info -- Using the silver layer that is already created and clean information
			)

		SELECT *
		FROM silver.crm_sales_details
		WHERE sls_cust_id NOT IN (
			SELECT cst_id
			FROM silver.crm_cust_info
			)

	--> Checking for Invalid Dates (Not necessary to check again because the column dates have been converted to DATE)
		--   Expectation: No results

	--> Checking the order date is earlier than the shipping or due date.
		--   Expectation: No results

		SELECT *
		FROM silver.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
 

	--> Checking for data inconsistencies among sales, quantity and price.
		--   Expectation: No results

		SELECT 
		sls_sales,
		sls_quantity,
		sls_price
		FROM silver.crm_sales_details
		WHERE sls_sales <> sls_quantity * sls_price
			OR sls_sales IS NULL
			OR sls_quantity IS NULL
			OR sls_price IS NULL
			OR sls_sales <= 0
			OR sls_quantity <= 0
			OR sls_price <= 0
		ORDER BY sls_sales,	sls_quantity,sls_price;

--==================================================== ERP_CUST_AZ12 ====================================================

	-- Checking Out-of-Range Dates

	SELECT
	bdate
	FROM silver.erp_cust_az12
	WHERE bdate > GETDATE();

	-->> Data Standardization & Consistency

	SELECT DISTINCT
	gen
	FROM silver.erp_cust_az12


--====================================================ERP_LOC_A101 ====================================================
	
	SELECT cid
	FROM silver.erp_loc_a101;

	-->> Data Standardization & Consistency

	SELECT DISTINCT cntry
	FROM silver.erp_loc_a101;

--==================================================== ERP_PX_CAT_G1V2 ====================================================
	
	SELECT *
	FROM silver.erp_px_cat_g1v2;