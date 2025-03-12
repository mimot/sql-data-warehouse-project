IF OBJECT_ID ('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS 
SELECT ROW_NUMBER() OVER(ORDER BY CI.cst_id) AS customer_key
	  , CI.cst_id AS customer_id
      , CI.cst_key AS customer_number
      , CI.cst_firstname AS first_name
      , CI.cst_lastname AS last_name
	  , LA.CNTRY AS country
	  , CASE WHEN CI.cst_gndr <> 'Unknown' THEN CI.cst_gndr
			ELSE ISNULL(CA.gen,'Unknown') END AS gender
      , CI.cst_marital_status AS marital_status
	  , CA.BDATE AS birth_date
      , CI.cst_create_date AS create_date
FROM silver.crm_cust_info AS CI
LEFT JOIN silver.erp_cust_az12 AS CA
ON CI.cst_key = CA.CID
LEFT JOIN silver.erp_loc_a101 AS LA
ON CI.cst_key = LA.CID;
GO

IF OBJECT_ID ('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products
GO
CREATE VIEW gold.dim_products AS  
SELECT ROW_NUMBER() OVER(ORDER BY PRI.prd_id) AS product_key
	  , PRI.prd_id AS product_id
      , PRI.prd_key AS product_number
      , PRI.prd_nm AS product_name
      , PRI.cat_id AS category_id
	  , PCG.CAT AS category
	  , PCG.SUBCAT AS subcategory
	  , PCG.MAINTENANCE AS maintenance
      , PRI.prd_cost AS cost
      , PRI.prd_line AS product_line
      , PRI.prd_start_dt AS product_start_date
FROM silver.crm_prd_info AS PRI
LEFT JOIN silver.erp_px_cat_g1v2 AS PCG
ON PRI.cat_id = PCG.ID
WHERE prd_end_dt IS NULL;
GO 

IF OBJECT_ID ('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.fact_sales AS  
SELECT SD.sls_ord_num AS order_number
	, DP.product_key 
	, DC.customer_key
	, SD.sls_order_dt AS order_date
	, SD.sls_ship_dt AS ship_date
	, SD.sls_due_dt AS due_date
	, SD.sls_sales AS sales
	, SD.sls_quantity AS quantity
	, SD.sls_price AS price
FROM silver.crm_sales_details AS SD
LEFT JOIN gold.dim_products AS DP
ON SD.sls_prd_key = DP.product_number
LEFT JOIN gold.dim_customers AS DC
ON SD.sls_cust_id = DC.customer_id


