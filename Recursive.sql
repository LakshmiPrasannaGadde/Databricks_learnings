-- Databricks notebook source
create table employee(emp_id int,emp_name varchar(100),age int,manager_id int);
insert into employee values (1,"abc",23,null),(2,"bcd",24,1),(3,"cde",20,2),(4,"def",21,3);

-- COMMAND ----------

with recursive hierarchydetails as (
  select emp_id,emp_name,age,manager_id from employee where manager_id is null
  union all
  select emp_id,emp_name,age,manager_id from employee as emp join hierarchydetails hd on emp.emp_id=hd.manager_id
)
select * from hierarchydetails

-- COMMAND ----------

# tried in azure sql 
#linked in post url: https://www.linkedin.com/feed/update/urn:li:activity:7118321873452449792/
with hierarchydetails as (
  select emp_id,emp_name,age,manager_id,0 as hierarchy_level from employee where manager_id is null
  union all
  select emp.emp_id,emp.emp_name,emp.age,emp.manager_id,hierarchy_level+1 from employee as emp join hierarchydetails hd on emp.manager_id=hd.emp_id
)
select * from hierarchydetails

