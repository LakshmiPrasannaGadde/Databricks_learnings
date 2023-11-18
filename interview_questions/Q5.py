# Databricks notebook source
# MAGIC %md
# MAGIC Q 25: Suppose you have a dataset containing information about orders from an online store. You need to solve scenario based questions using pyspark.
# MAGIC
# MAGIC 🔹Total Revenue Analysis   
# MAGIC 🔹Highest Value Order   
# MAGIC 🔹Top-Spending Customer   
# MAGIC 🔹Order Count per Customer   
# MAGIC
# MAGIC Sample Data:
# MAGIC
# MAGIC order_id,customer_id,order_date,total_amount   
# MAGIC 1,Customer1,2023-01-10,100   
# MAGIC 2,Customer2,2023-01-11,150   
# MAGIC 3,Customer1,2023-01-12,80   
# MAGIC 4,Customer3,2023-01-12,200   
# MAGIC 5,Customer2,2023-01-13,50   
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntergerType,StringType,DateType
order_data=[(1,'Customer1','2023-01-10',100),
            (2,'Customer2','2023-01-11',150),
             (3,'Customer1','2023-01-12',80),
             (4,'Customer3','2023-01-12',200),
             (5,'Customer2','2023-01-13',50)]
schema=StrucType([StructField("order_id",IntegerType(),false),
                  StructField("customer_id",IntegerType(),false),
                   StructField("order_date",DateType(),false),
                   StructField("total_amount",integerType(),false)])

order_df=spark.createDataFrame(data=order_data,schemaschema)

# COMMAND ----------

order_df.select(sum("total_amount")).show()

#total_revenue_analysis.show()
