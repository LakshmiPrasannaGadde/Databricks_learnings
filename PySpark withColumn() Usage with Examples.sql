-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC website link: https://sparkbyexamples.com/pyspark/pyspark-withcolumn/
-- MAGIC
-- MAGIC PySpark withColumn() is a transformation function of DataFrame which is used to change the value, convert the datatype of an existing column, create a new column, and many more. In this post, I will walk you through commonly used PySpark DataFrame column operations using withColumn() examples.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [('James','','Smith','1991-04-01','M',3000),
-- MAGIC   ('Michael','Rose','','2000-05-19','M',4000),
-- MAGIC   ('Robert','','Williams','1978-09-05','M',4000),
-- MAGIC   ('Maria','Anne','Jones','1967-12-01','F',4000),
-- MAGIC   ('Jen','Mary','Brown','1980-02-17','F',-1)
-- MAGIC ]
-- MAGIC
-- MAGIC columns = ["firstname","middlename","lastname","dob","gender","salary"]
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
-- MAGIC df = spark.createDataFrame(data=data, schema = columns)
-- MAGIC
-- MAGIC #df:pyspark.sql.dataframe.DataFrame
-- MAGIC #firstname:string
-- MAGIC #middlename:string
-- MAGIC #lastname:string
-- MAGIC #dob:string
-- MAGIC #gender:string
-- MAGIC #salary:long"""
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 1. Change DataType using PySpark withColumn()   
-- MAGIC
-- MAGIC By using PySpark withColumn() on a DataFrame, we can cast or change the data type of a column. In order to change data type, you would also need to use cast() function along with withColumn(). The below statement changes the datatype from String to Integer for the salary column.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StringType,IntegerType
-- MAGIC # cast using integerType()
-- MAGIC #df=df.withColumn("salary",df.salary.cast(IntegerType()))
-- MAGIC #cast using cast("int")
-- MAGIC df=df.withColumn("salary",df.salary.cast("integer"))
-- MAGIC df.show()
-- MAGIC #we can use any of following cast syntax, cast("int") , cast(IntegerType()) or cast("interger"). all  gives same output.
-- MAGIC #df:pyspark.sql.dataframe.DataFrame
-- MAGIC #firstname:string
-- MAGIC #middlename:string
-- MAGIC #lastname:string
-- MAGIC #dob:string
-- MAGIC #gender:string
-- MAGIC #salary:integer
-- MAGIC #+---------+----------+--------+----------+------+------+
-- MAGIC #|firstname|middlename|lastname|       dob|gender|salary|
-- MAGIC #+---------+----------+--------+----------+------+------+
-- MAGIC #|    James|          |   Smith|1991-04-01|     M|  3000|
-- MAGIC #|  Michael|      Rose|        |2000-05-19|     M|  4000|
-- MAGIC #|   Robert|          |Williams|1978-09-05|     M|  4000|
-- MAGIC #|    Maria|      Anne|   Jones|1967-12-01|     F|  4000|
-- MAGIC #|      Jen|      Mary|   Brown|1980-02-17|     F|    -1|
-- MAGIC #+---------+----------+--------+----------+------+------+
-- MAGIC

-- COMMAND ----------


