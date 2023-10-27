# Databricks notebook source
# MAGIC %md
# MAGIC web url: https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/
# MAGIC Note: If you try to perform operations on empty RDD you going to get ValueError("RDD is empty").
# MAGIC

# COMMAND ----------

#Create an empty RDD by using emptyRDD()
rdd=spark.sparkContext.emptyRDD()
#Creates Empty RDD using parallelize
rdd=spark.sparkContext.parallelize([])
print(rdd)

# COMMAND ----------

#creating dataframe from empty rdd
df=rdd.toDF()
df.printSchema()
df.show()

# COMMAND ----------

#Create Empty DataFrame from empty RDD with Schema (StructType)

#Create Schema
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])

#Now use the empty RDD created above and pass it to createDataFrame() of SparkSession along with the schema for column names & data types.

#Create empty DataFrame from empty RDD
df = spark.createDataFrame(rdd,schema)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Convert Empty RDD to DataFrame
# MAGIC You can also create empty DataFrame by converting empty RDD to DataFrame using toDF().

# COMMAND ----------

df1=rdd.toDF(schema)
df1.printSchema()
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Empty DataFrame with Schema.
# MAGIC So far I have covered creating an empty DataFrame from RDD, but here will create it manually with schema and without RDD.

# COMMAND ----------

df2=spark.createDataFrame([],schema)
df2.printSchema()
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Empty DataFrame without Schema (no columns)
# MAGIC To create empty DataFrame with out schema (no columns) just create a empty schema and use it while creating PySpark DataFrame.

# COMMAND ----------

df3=spark.createDataFrame([],StructType([]))
df3.printSchema()
df3.show()
