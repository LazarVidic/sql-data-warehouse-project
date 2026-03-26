/*

======================================================
  DDL SCRIPT: Create Gold Views
======================================================
This script create Gold layer in datawarehouse system.
The Gold layer represents Dimensions and Fact table (Star Schema).

*/


-- ======================================================
-- Create Dimension: gold.dim_customer 
-- ======================================================

if object_id('gold.dim_customer', 'V') is not null
  drop view gold.dim_customer
go

create view gold.dim_customer as 
select
	row_number() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_material_status as material_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr
			else coalesce(ca.gen, 'n/a')
	end as gender,
	ca.bdate as birthday,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on     ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

-- ======================================================
-- Create Dimension: gold.dim_product
-- ======================================================

if object_id('gold.dim_product', 'V') is not null
  drop view gold.dim_product
go
  
create view gold.dim_product as 
select
    row_number() over (order by pn.prd_start_dt, pn.prd_key ) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt is null


-- ======================================================
-- Create Fact: gold.fact_sales 
-- ======================================================

if object_id('gold.fact_sales', 'V') is not null
  drop view gold.fact_sales
go
  
create view gold.fact_sales as
select
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_ord_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price
from silver.crm_sales_details sd
left join gold.dim_product pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id = cu.customer_id





