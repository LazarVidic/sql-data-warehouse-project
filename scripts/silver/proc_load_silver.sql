/*
Stored Procedure: Load Silver Layer (Bronze -› Silver)
Script Purpose:
  This stored procedure performs the ETL process to populate the 'silver' schema tables from the 'bronze' schema tables.
  It performs the following actions:
  - Truncates the silver tables.
  - Load cleaned and transformed data from 'bronze' schema to 'silver' schema.
I
Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC silver.load_silver;
*/


create or alter procedure silver.load_silver as
begin
	declare @start_time datetime
	declare @end_time datetime;
	declare @procedure_start_time datetime;
	declare @procedure_end_time datetime;

	set @procedure_start_time = getdate()

	begin try
		print '================================='
		print 'Loading Silver Layer'
		print '================================='

		print '---------------------------------'
		print 'Loading CRM Tables'
		print '---------------------------------'

		-- CRM CUSTOMER
		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_cust_info'
		truncate table silver.crm_cust_info

		print '>> Inserting Data Table: silver.crm_cust_info'
		insert into silver.crm_cust_info (
			cst_id, cst_key, cst_firstname, cst_lastname,
			cst_material_status, cst_gndr, cst_create_date
		)
		select
			cst_id,
			cst_key,
			trim(cst_firstname),
			trim(cst_lastname),
			case when upper(trim(cst_material_status)) = 'S' then 'Single'
				 when upper(trim(cst_material_status)) = 'M' then 'Married'
				 else 'n/a' end,
			case when upper(trim(cst_gndr)) = 'F' then 'Female'
				 when upper(trim(cst_gndr)) = 'M' then 'Male'
				 else 'n/a' end,
			cst_create_date
		from (
			select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t
		where flag_last = 1;

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		-- CRM PRODUCT
		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_prd_info'
		truncate table silver.crm_prd_info

		print '>> Inserting Data Table: silver.crm_prd_info'
		insert into silver.crm_prd_info (
			prd_id, cat_id, prd_key, prd_nm,
			prd_cost, prd_line, prd_start_dt, prd_end_dt
		)
		select
			prd_id,
			replace(substring(prd_key, 1, 5), '-', '_'),
			substring(prd_key, 7, len(prd_key)),
			prd_nm,
			isnull(prd_cost, 0),
			case when upper(trim(prd_line)) = 'M' then 'Mountaint'
				 when upper(trim(prd_line)) = 'R' then 'Road'
				 when upper(trim(prd_line)) = 'S' then 'Other Sales'
				 when upper(trim(prd_line)) = 'T' then 'Touring'
				 else 'n/a' end,
			cast(prd_start_dt as date),
			cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date)
		from bronze.crm_prd_info;

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		-- CRM SALES
		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_sales_details'
		truncate table silver.crm_sales_details

		print '>> Inserting Data Table: silver.crm_sales_details'
		insert into silver.crm_sales_details (
			sls_ord_num, sls_prd_key, sls_cust_id,
			sls_ord_dt, sls_ship_dt, sls_due_dt,
			sls_sales, sls_quantity, sls_price
		)
		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_ord_dt = 0 or len(sls_ord_dt) != 8 then null
				 else cast(cast(sls_ord_dt as varchar) as date) end,
			case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				 else cast(cast(sls_ship_dt as varchar) as date) end,
			case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
				 else cast(cast(sls_due_dt as varchar) as date) end,
			case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				 then sls_quantity * abs(sls_price)
				 else sls_sales end,
			sls_quantity,
			case when sls_price is null or sls_price <= 0
				 then sls_sales / nullif(sls_quantity, 0)
				 else sls_price end
		from bronze.crm_sales_details;

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		-- ERP
		print '---------------------------------'
		print 'Loading ERP Tables'
		print '---------------------------------'

		-- ERP CUSTOMER
		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_cust_az12'
		truncate table silver.erp_cust_az12

		print '>> Inserting Data Table: silver.erp_cust_az12'
		insert into silver.erp_cust_az12(cid, bdate, gen)
		select 
			case when cid like 'NAS%' then substring(cid, 4, len(cid)) else cid end,
			case when bdate > getdate() then null else bdate end,
			case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
				 when upper(trim(gen)) in ('M','MALE') then 'Male'
				 else 'n/a' end
		from bronze.erp_cust_az12;

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		-- ERP LOCATION
		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_loc_a101'
		truncate table silver.erp_loc_a101

		print '>> Inserting Data Table: silver.erp_loc_a101'
		insert into silver.erp_loc_a101(cid, cntry)
		select 
			replace(cid, '-', ''),
			case when trim(cntry) ='DE' then 'Germany'
				 when trim(cntry) in ('US','USA') then 'United States'
				 when trim(cntry) = '' or cntry is null then 'n/a'
				 else trim(cntry) end
		from bronze.erp_loc_a101;

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		-- ERP CATEGORY
		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_px_cat_g1v2'
		truncate table silver.erp_px_cat_g1v2

		print '>> Inserting Data Table: silver.erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		select id, cat, subcat, maintenance
		from bronze.erp_px_cat_g1v2;

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		set @procedure_end_time = getdate();
		print '>>Executing procedure duration: ' + cast(datediff(second, @procedure_start_time, @procedure_end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';

	end try
	begin catch
		print '==============================================='
		print 'Error occured during loading silver layer'
		print 'Error message: ' + error_message();
		print 'Error number: ' + cast(error_number() as nvarchar);
		print 'Error state: ' + cast(error_state() as nvarchar);
		print '==============================================='
	end catch

end
