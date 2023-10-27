-- Databricks notebook source
-- MAGIC %md
-- MAGIC Utilizing structured streaming to read the change data feed from your #databricks Delta table empowers you to execute incremental simple streaming aggregations, such as counting and summing.      
-- MAGIC linkedIn Post Link: https://www.linkedin.com/feed/update/urn:li:activity:7118177887005892608/

-- COMMAND ----------

create table transaction (
  transaction_id int,
  product_id int,
  units int,
  sales float
);
insert into transaction values(1,1,2,1),(1,2,2,2),(2,2,2,2),(3,1,1,1);

-- COMMAND ----------

create table trans_count (
  product_id int,
  trans_count int
);
insert into trans_count values(1,10),(2,15),(3,9);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC updateddf=(spark.readStream.format('delta').option("readChangeFeed","True").table("transaction").writeStream.trigger(availableNow=True).foreachBatch(upsertToDelta).start())
-- MAGIC updateddf.show()
