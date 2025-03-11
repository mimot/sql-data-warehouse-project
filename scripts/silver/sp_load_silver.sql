USE AWorks_DW;
GO

CREATE OR ALTER PROC silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@total_start_time DATETIME, @total_end_time DATETIME

	BEGIN TRY
		PRINT '----------------------------------------------------------------------------------------'
		PRINT 'Loading Silver Layer...'
		PRINT '----------------------------------------------------------------------------------------'

		PRINT 'Loading CRM Tables...'
		SET @total_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting data into Table: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info
		(
			cst_id
			, cst_key
			, cst_firstname
			, cst_lastname
			, cst_marital_status
			, cst_gndr
			, cst_create_date
		)
		SELECT cst_id
			, cst_key
			, TRIM(cst_firstname) AS cst_firstname
			, TRIM(cst_lastname) AS cst_lastname
			, CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'Unknown' END AS cst_marital_status
			, CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'Unknown' END AS cst_gndr
			, cst_create_date
		FROM 
		(
			SELECT ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_rec
				, *
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) AS A
		WHERE latest_rec = 1 
				SET @end_time = GETDATE();
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '============='

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting data into Table: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info
		(
			prd_id
			, prd_key 
			, cat_id
			, prd_nm
			, prd_cost
			, prd_line
			, prd_start_dt
			, prd_end_dt
		)
		SELECT prd_id
			, SUBSTRING(prd_key,CHARINDEX('-',prd_key,CHARINDEX('-',prd_key)+1)+1,LEN(prd_key)) AS prd_key
			, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id
			, prd_nm
			, ISNULL(prd_cost,0) AS prd_cost
			, CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Tools'
				ELSE 'Unknown' END AS prd_line
			, prd_start_dt
			, DATEADD(day,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '============='

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting data into Table: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details
		(
			sls_ord_num
			, sls_prd_key
			, sls_cust_id
			, sls_order_dt
			, sls_ship_dt
			, sls_due_dt
			, sls_sales
			, sls_quantity
			, sls_price
		)
		SELECT sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id
			  ,CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt
			  ,CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt
			  ,CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt
			  , CASE WHEN sls_sales IS NULL 
						OR sls_sales <> (sls_quantity * ABS(sls_price))
					THEN (sls_quantity * ABS(sls_price))
				ELSE sls_sales END AS sls_sales
			  ,sls_quantity
			  , CASE WHEN sls_price IS NULL
						OR sls_price <= 0 
					THEN (ABS(sls_sales) / NULLIF(sls_quantity,0))
				ELSE sls_price END AS sls_price
		  FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '============='

		PRINT 'CRM Tables Loading DONE...'

		-----------------------------------------------------------------------------------------------------------------------------
		PRINT 'Loading ERP Tables...'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting data into Table: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12
		(
			CID
			, BDATE
			, GEN
		)
		SELECT CASE WHEN CID LIKE 'NAS%' THEN TRIM(SUBSTRING(CID,4,LEN(CID)))
				ELSE TRIM(CID) END AS CID
			, CASE WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE END AS BDATE
			, CASE WHEN TRIM(UPPER(GEN)) IN ('MALE','M') THEN 'Male'
				WHEN TRIM(UPPER(GEN)) IN ('FEMALE','F') THEN 'Female'
				ELSE 'Unknown' END AS GEN
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '============='

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting data into Table: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101
		(
			CID
			, CNTRY
		)
		SELECT REPLACE(CID,'-','') AS CID
			, CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
				WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY) IS NULL THEN 'Unknown'
				ELSE TRIM(CNTRY) END AS CNTRY
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '============='

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting data into Table: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2
		(
			ID
			, CAT
			, SUBCAT
			, MAINTENANCE
		)
		SELECT ID
			, CAT
			, SUBCAT
			, MAINTENANCE
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '============='

		PRINT '>>Total load duration: ' + CAST(DATEDIFF(second, @total_start_time, @total_end_time) AS NVARCHAR) + ' seconds';
		PRINT '============='
		PRINT 'ERP Tables Loading DONE...'
	END TRY
	BEGIN CATCH
		PRINT '----------------------------------------------------------------------------------------'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------------------------------------------------'

	END CATCH

END
