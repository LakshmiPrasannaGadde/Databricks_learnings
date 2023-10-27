# Databricks notebook source
data=[("firstname","lastname",20),("firstname","lastname",20),("firstname","lastname",20),("firstname1","lastname1",20)]
df=spark.createDataFrame(data,["fname","lname","age"])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC remove duplicate records

# COMMAND ----------

from pyspark.sql.functions import row_number,rank
from pyspark.sql.window import Window
windowspec=Window.partitionBy("fname","lname","age").orderBy("fname","lname","Age")
df=df.withColumn("order_number",row_number().over(windowspec))
df.show()
df.filter(df.order_number==1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC remove duplicate records using distinct and drop_duplicate functions

# COMMAND ----------

df.distinct().show()
df.drop_duplicates(subset=["fname","lname","age"]).show()
