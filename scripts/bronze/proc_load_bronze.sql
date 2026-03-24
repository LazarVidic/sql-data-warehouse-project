/*

Stored Procedure: Load Bronze Layer (Source -› Bronze)
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from existing CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load files from CSV to bronze tables
I
Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;

*/



create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime
	declare @end_time datetime;
	declare @procedure_start_time datetime;
	declare @procedure_end_time datetime;


	set @procedure_start_time = getdate()
	begin try
		print '================================='
		print 'Loading Bronze Layer'
		print '================================='

		print '---------------------------------'
		print 'Loading CRM Tables'
		print '---------------------------------'



		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_cust_info'
		truncate table bronze.crm_cust_info

		print '>> Inserting Data Table: bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Laki\Desktop\DatawarehouseData\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock

		);

		set @end_time = getdate();

		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';



		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_prd_info'
		truncate table bronze.crm_prd_info

		print '>> Inserting Data Table: bronze.crm_prd_info'

		bulk insert bronze.crm_prd_info
		from 'C:\Users\Laki\Desktop\DatawarehouseData\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock

		);
		set @end_time = getdate();

		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		


		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_sales_details'
		truncate table bronze.crm_sales_details

		print '>> Inserting Data Table: bronze.crm_sales_details'

		bulk insert bronze.crm_sales_details
		from 'C:\Users\Laki\Desktop\DatawarehouseData\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock

		);
		set @end_time = getdate();

		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		




		--ERP SOURCES 

		print '---------------------------------'
		print 'Loading ERP Tables'
		print '---------------------------------'


		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12

		print '>> Inserting Data Table: bronze.erp_cust_az12'
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Laki\Desktop\DatawarehouseData\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock

		);
		set @end_time = getdate();

		print '>> Procedure executing duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		


		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_loc_a101'
		truncate table bronze.erp_loc_a101

		print '>> Inserting Data Table: bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Laki\Desktop\DatawarehouseData\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock

		);
		set @end_time = getdate();

		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		


		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_px_cat_G1V2'
		truncate table bronze.erp_px_cat_G1V2

		print '>> Inserting Data Table: bronze.erp_px_cat_G1V2'
		bulk insert bronze.erp_px_cat_G1V2
		from 'C:\Users\Laki\Desktop\DatawarehouseData\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock

		);
		set @end_time = getdate();

		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';


		set @procedure_end_time = getdate();

		print '>>Executing procedure duration: ' + cast(datediff(second, @procedure_start_time, @procedure_end_time) as varchar) + ' seconds ';
		print '---------------------------------------------';




	end try
	begin catch
		print '==============================================='
		print 'Error occured during loading bronze layer'
		print 'Error message' + error_message();
		print 'Error message' + cast(error_number() as nvarchar);
		print 'Error message' + cast(error_state() as nvarchar);
		print '==============================================='
	end catch



end



