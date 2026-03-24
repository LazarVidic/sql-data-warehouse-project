
/*

Create Database and Schemas

	This script creates a new database named DataWarehouse after checking if it already exists.



*/


use master;
go


--Drop and recreate the 'DataWarehouse' database

if exists(select 1 from sys.database where name = 'DataWarehouse')
begin 
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse
end;
go


--create te 'DataWarehouse' database
create database DataWarehouse;
go

use DataWarehouse;
go


--create schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
