use DataWarehouse;

if OBJECT_ID('bronze.crm_cust_info', 'u') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(20),
	cst_firstname nvarchar(20),
	cst_lastname nvarchar(20),
	cst_marital_status nvarchar(20),
	cst_gndr nvarchar(50),
	cst_create_date date
);

if OBJECT_ID('bronze.crm_prd_info', 'u') is not null
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime
);

if OBJECT_ID('bronze.crm_sales_details', 'u') is not null
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);

if OBJECT_ID('bronze.erp_cust_az12', 'u') is not null
	drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);

if OBJECT_ID('bronze.erp_px_cat_g1v2', 'u') is not null
	drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintainance nvarchar(50)
);

if OBJECT_ID('bronze.erp_loc_a101', 'u') is not null
	drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50)
);


create or alter procedure bronze.load_bronze as
begin
	begin try
		print'============================';
		print'Loading bronze layer';
		print '============================';
		print('Loading Data From CRM');
		print'============================';


		print'Truncating bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print'inserting data into bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\sarvesh jathar\Desktop\Sales Analytics Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		print'Truncating bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print 'inserting data into bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\sarvesh jathar\Desktop\Sales Analytics Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		print 'Truncating bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print 'inserting data into bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\sarvesh jathar\Desktop\Sales Analytics Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		print'============================';
		print('Loading Data From CRM')
		print '============================';

		print'Truncating bronze.crm_sales_details';
		truncate table bronze.erp_cust_az12;
		print'inserting data into bronze.crm_sales_details';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\sarvesh jathar\Desktop\Sales Analytics Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		print'Truncating bronze.crm_sales_details';
		truncate table bronze.erp_px_cat_g1v2;
		print'inserting data into bronze.crm_sales_details';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\sarvesh jathar\Desktop\Sales Analytics Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		print'Truncating bronze.crm_sales_details';
		truncate table bronze.erp_loc_a101;
		print'inserting data into bronze.crm_sales_details';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\sarvesh jathar\Desktop\Sales Analytics Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
	end try
	begin catch
		print '================================';
		print 'Error occured during loading bronze';
		print '================================';
	end catch
end 
exec bronze.load_bronze;



print 'DDL scripts for silver layer'

if OBJECT_ID('silver.crm_cust_info', 'u') is not null
	drop table silver.crm_cust_info;
create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(20),
	cst_firstnam nvarchar(20),
	cst_lastname nvarchar(20),
	cst_marital_status nvarchar(20),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh__create_date datetime default getdate()
);

if OBJECT_ID('silver.crm_prd_info', 'u') is not null
	drop table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh__create_date datetime default getdate()
);

if OBJECT_ID('silver.crm_sales_details', 'u') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh__create_date datetime default getdate()
);

if OBJECT_ID('silver.erp_cust_az12', 'u') is not null
	drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh__create_date datetime default getdate()
);

if OBJECT_ID('silver.erp_px_cat_g1v2', 'u') is not null
	drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintainance nvarchar(50),
	dwh__create_date datetime default getdate()
);

if OBJECT_ID('silver.erp_loc_a101', 'u') is not null
	drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh__create_date datetime default getdate()
);

print 'Data Cleaning for silver layer'

insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstnam,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status)) = 'S' then 'Single'
	 when upper(trim(cst_marital_status))  = 'M' then 'Married'
	 else 'n/a'
end as cst_marital_status,
case when upper(trim(cst_gndr)) = 'F' then 'Female'
	 when upper(trim(cst_gndr))  = 'M' then 'Male'
	 else 'n/a'
end as cst_gndr,
cst_create_date
from (
	select *,
	row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
) t where flag_last = 1
select * from silver.crm_cust_info;

insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select
prd_id,
replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
substring(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost, 0) as prd_cost,
case when upper(trim(prd_line)) = 'M' then 'Mountain'
	 when upper(trim(prd_line)) = 'R' then 'Road'
	 when upper(trim(prd_line)) = 'S' then 'Other Sales'
	 when upper(trim(prd_line)) = 'T' then 'Touring'
	 else 'Not Available'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.crm_prd_info;
select * from silver.crm_prd_info;



insert into silver.crm_sales_details(
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
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
	 else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
	 else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
	 else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
	 then sls_quantity * abs(sls_price)
	 else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0
	 then sls_sales / nullif(sls_quantity, 0)
else sls_price
end as sls_sales
from bronze.crm_sales_details;
select * from silver.crm_sales_details;



select * from bronze.erp_cust_az12;



