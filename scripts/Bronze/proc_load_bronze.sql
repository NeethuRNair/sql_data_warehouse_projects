/*
=====================================================================================================================
Stored Procedure : Load Bronze Layer
=====================================================================================================================
This stored procedure loads data from thre csv file into the bronze schema.
Truncates the table before loading the data.
Uses bulk insert command to load data from the source to the bronze tables.
*/

exec bronze.load_bronze ;
create or alter procedure bronze.load_bronze as

begin
	declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
	begin try
set @batch_start_time=GETDATE();
/* load data from the CRM source file */
print '=================================';
print 'Loading Bronze Layer';
print '=================================';

print '---------------------------------';
print 'Loading CRM Tables';
print '---------------------------------';
print '*** truncating table:bronze.crm_cust_info';
print '*** inserting table:bronze.crm_cust_info';


set @start_time=GETDATE();

truncate table bronze.crm_cust_info
bulk insert bronze.crm_cust_info
from 'C:\Users\dhyan\Desktop\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

with (

	firstrow=2,
	fieldterminator = ',',
	tablock
	);

set @end_time=GETDATE();
print'>> Duration time : ' +cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
print'>> ----------------------------------------------------------------------';



print '*** truncating table:bronze.crm_prd_info';
print '*** inserting table:bronze.crm_prd_info';

set @start_time=GETDATE();

truncate table bronze.crm_prd_info
bulk insert bronze.crm_prd_info
from 'C:\Users\dhyan\Desktop\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);

set @end_time=GETDATE();
print'>> Duration time : ' +cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
print'>> ----------------------------------------------------------------------';


print '*** truncating table:bronze.crm_sales_details';
print '*** inserting table:bronze.crm_sales_details';

set @start_time=getdate();

truncate table bronze.crm_sales_details
bulk insert bronze.crm_sales_details
from 'C:\Users\dhyan\Desktop\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
set @end_time=GETDATE();
print'>> Duration time : ' +cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';


/* load data from the ERP source file */
print '---------------------------------';
print 'Loading ERP Tables';
print '---------------------------------';

print '*** truncating table:bronze.erp_CUST_AZ12';
print '*** inserting table:bronze.erp_CUST_AZ12';

set @start_time=GETDATE();


truncate table bronze.erp_CUST_AZ12
bulk insert bronze.erp_CUST_AZ12
from 'C:\Users\dhyan\Desktop\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);


select count(*) from bronze.erp_CUST_AZ12
set @end_time=GETDATE();
print'>> Duration time : ' +cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';


print '*** truncating table:bronze.erp_LOC_A101';
print '*** inserting table:bronze.erp_LOC_A101';

set @start_time=GETDATE();


truncate table bronze.erp_LOC_A101
bulk insert bronze.erp_LOC_A101
from 'C:\Users\dhyan\Desktop\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
select count(*) from bronze.erp_LOC_A101

set @end_time=GETDATE();
print'>> Duration time : ' +cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';


print '*** truncating table:bronze.erp_PX_CAT_G1V2';
print '*** inserting table:bronze.erp_PX_CAT_G1V2';

set @start_time=GETDATE();

truncate table bronze.erp_PX_CAT_G1V2
bulk insert bronze.erp_PX_CAT_G1V2
from 'C:\Users\dhyan\Desktop\SQL\data_warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
select count(*) from bronze.erp_PX_CAT_G1V2
print'>> Duration time : ' +cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
print'>> ----------------------------------------------------------------------';

set @batch_end_time=GETDATE();
Print '==============================================';
Print 'Loading bronze layer is completed';

print'>> Total duration:'+cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar) +' seconds';
print'>> ----------------------------------------------------------------------';

	end try

	begin catch

		Print '==============================================';
		Print 'Error occurred while loading the bronze layer';
		Print 'Error message' + error_message();
		Print 'Error message' + cast(error_number() as nvarchar);
		Print 'Error message' + cast(error_state() as nvarchar);
		Print '==============================================';


	end catch
end
