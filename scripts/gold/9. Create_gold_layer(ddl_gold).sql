/*
======================================================================================================
DDL Script: Create Gold views

Script Purpose:
	- This script creates the views for the 'gold' layer in the data warehouse.
	- For every dimension a surrogate key will be created. Surrogates keys are important because business keys
	 (product_id,customer_id) can change over time in the source system. This is way is safer to use a surrogate key.
	- The data from the 'silver' layer will be used to create this layer.
	- Data transformations and combinations will be performed to produce clean and ready-for-analysis data.
	- By joining different tables two dimensions (dim_customer, dim_products) and a fact table (fact_sales) 
	 will be created (see Integration Model, Data Model and Data Flow graphics).
======================================================================================================

*/

--==================================================== DIM_CUSTOMERS ====================================================


	CREATE VIEW gold.dim_customers AS 
	SELECT
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,-- creating a surrogate key
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		CASE 
		WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'n/a')
		END AS gender,
		ca.bdate AS birthdate,
		loc.cntry AS country,
		ci.cst_marital_status AS marital_status,
		ci.cst_create_date AS create_date
	FROM  silver.crm_cust_info AS ci
		LEFT JOIN silver.erp_cust_az12 AS ca 
		ON ci.cst_key = ca.cid
		LEFT JOIN silver.erp_loc_a101 AS loc
		ON ci.cst_key = loc.cid;
	GO


--==================================================== DIM_PRODUCTS ====================================================
	
	-- Final version

	CREATE VIEW gold.dim_products AS 

	SELECT
		ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt,pr.prd_key) AS product_key, -- creating a surrogate key
		pr.prd_id AS product_id,
		pr.prd_key AS product_number,
		pr.prd_nm AS product_name,
		pr.cat_id AS category_id,
		px.cat AS category,
		px.subcat AS subcategory,
		px.maintenance,
		pr.prd_cost AS cost,
		pr.prd_line AS product_line,
		pr.prd_start_dt AS start_date
	FROM silver.crm_prd_info pr
		LEFT JOIN silver.erp_px_cat_g1v2 px
		ON pr.cat_id = px.id
		WHERE prd_end_dt IS NULL;-- We filter where end date is NULL becuase that means that price is still valid today.
	GO

--==================================================== FACT_SALES====================================================

	CREATE VIEW gold.fact_sales AS
	SELECT
		sl.sls_ord_num AS order_number,
		pr.product_key,
		cu.customer_key,
		sl.sls_order_dt AS order_date,
		sl.sls_ship_dt AS shippin_date,
		sl.sls_due_dt AS due_date,
		sl.sls_sales sales_amount,
		sl.sls_quantity AS quantity,
		sl.sls_price AS price
	FROM silver.crm_sales_details sl
		LEFT JOIN gold.dim_products pr
		ON sl.sls_prd_key = pr.product_number
		LEFT JOIN gold.dim_customers cu
		ON sl.sls_cust_id = cu.customer_id;
	GO

	--Verifying the Foreging Key Integriting
	-- Expectations: No results

	SELECT *
	FROM gold.fact_sales f
		LEFT JOIN gold.dim_customers c
		ON c.customer_key = f.customer_key
		LEFT JOIN gold.dim_products p
		ON p.product_key = f.product_key
		WHERE p.product_number IS NULL;
	GO