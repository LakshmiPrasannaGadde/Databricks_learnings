# Databricks notebook source
# MAGIC %md
# MAGIC In Databricks, you can use the MERGE statement to perform upserts (a combination of INSERT and UPDATE) into a target table based on a condition. The MERGE statement is particularly useful when you want to synchronize data between two tables or when you need to handle insert, update, and delete operations in a more controlled manner.
# MAGIC
# MAGIC Syntax:
# MAGIC MERGE INTO target_table AS target
# MAGIC USING source_table AS source
# MAGIC ON condition
# MAGIC WHEN MATCHED THEN
# MAGIC  UPDATE SET column1 = value1, column2 = value2, ...
# MAGIC WHEN NOT MATCHED THEN
# MAGIC  INSERT (column1, column2, ...) VALUES (value1, value2, ...);
# MAGIC
# MAGIC Let's break down the components and provide an example:
# MAGIC
# MAGIC target_table: The table you want to modify.
# MAGIC source_table: The table you want to use as a source for the changes.
# MAGIC condition: The condition used to match rows between the target and source tables.
# MAGIC WHEN MATCHED: Specifies what to do when a match is found based on the condition.
# MAGIC UPDATE SET: Defines how to update the columns in the target table when a match is found.
# MAGIC WHEN NOT MATCHED: Specifies what to do when no match is found based on the condition.
# MAGIC INSERT: Defines how to insert new rows into the target table when no match is found.
# MAGIC
# MAGIC Suppose you have a target table called "Sales" and a source table called "NewSales," and you want to update existing sales records and insert new ones based on a matching "OrderID." Here's how you can write a MERGE query in Databricks:

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table sales (OrderID int, Product int, Quantity int);
# MAGIC insert into sales values(1,1,2),(1,2,1),(1,3,1);
# MAGIC create or replace table newsales(OrderID int, Product int, Quantity int);
# MAGIC insert into newsales values(2,1,2),(1,2,2),(3,1,1);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into sales
# MAGIC using newsales
# MAGIC on sales.OrderId==newsales.OrderId
# MAGIC when Matched
# MAGIC then update set sales.quantity=newsales.quantity
# MAGIC when not matched
# MAGIC then insert  (OrderId,Product,Quantity) values (newsales.OrderID,newsales.Product,newsales.Quantity)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales order by orderid
