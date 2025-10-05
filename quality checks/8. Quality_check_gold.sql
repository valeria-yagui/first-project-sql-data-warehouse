/*
==========================================================================================================
Data Quality Check
	- These are the scripts that were used to analyze the data in the 'gold' layer before creating the dimensions
	and fact table.
==========================================================================================================
*/	

--==================================================== DIM_CUSTOMERS ====================================================

	-- Marking sure there are no duplicates

	SELECT cst_id,COUNT(*) AS duplicates
	FROM (
		SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		loc.cntry
		FROM  silver.crm_cust_info AS ci
		LEFT JOIN silver.erp_cust_az12 AS ca --- If inner join is used, lose data might be lost. This is why LEFT JOIN is used with the master table as reference
		ON ci.cst_key = ca.cid
		LEFT JOIN silver.erp_loc_a101 AS loc
		ON ci.cst_key = loc.cid
	) t
	GROUP BY cst_id
	HAVING COUNT(*) >1;
	GO

	--Fixing gender inconsistencies

	SELECT DISTINCT
		ci.cst_gndr,
		ca.gen,
		CASE 
		WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'n/a') -- If ca.gen has any value, that value will be returned. If not, then 'n/a' will be returned.
		END AS new_gen
	FROM  silver.crm_cust_info ci
		LEFT JOIN silver.erp_cust_az12 ca --- If we use inner join we might lose data. We use the master table as reference.
		ON ci.cst_key = ca.cid
		LEFT JOIN silver.erp_loc_a101 loc
		ON ci.cst_key = loc.cid
		ORDER BY ci.cst_gndr,ca.gen;
	GO

	
--==================================================== DIM_PRODUCTS ====================================================
	-- Marking sure there is no duplicates

	SELECT prd_key, COUNT(*) AS duplicates
	FROM (
		SELECT
		pr.prd_id,
		pr.cat_id,
		pr.prd_key,
		pr.prd_nm,
		pr.prd_cost,
		pr.prd_line,
		pr.prd_start_dt,
		px.cat,
		px.subcat,
		px.maintenance
	FROM silver.crm_prd_info pr
		LEFT JOIN silver.erp_px_cat_g1v2 px
		ON pr.cat_id = px.id
		WHERE prd_end_dt IS NULL) t
		GROUP BY prd_key
		HAVING COUNT(*)>1;
	GO
