# Databricks notebook source
# MAGIC %md
# MAGIC In PySpark, toDF() function of the RDD is used to convert RDD to DataFrame. We would need to convert RDD to DataFrame as DataFrame provides more advantages over RDD. For instance, DataFrame is a distributed collection of data organized into named columns similar to Database tables and provides optimization and performance improvements.
# MAGIC
# MAGIC link refered: https://sparkbyexamples.com/pyspark/convert-pyspark-rdd-to-dataframe/

# COMMAND ----------

# MAGIC %md
# MAGIC Create PySpark RDD
# MAGIC First, let’s create an RDD by passing Python list object to sparkContext.parallelize() function. We would need this rdd object for all our examples below.
# MAGIC
# MAGIC In PySpark, when you have data in a list meaning you have a collection of data in a PySpark driver memory when you create an RDD, this collection is going to be parallelized.

# COMMAND ----------

dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
print(rdd)    #ParallelCollectionRDD[41] at readRDDFromInputStream at PythonRDD.scala:435
rdd.collect()  #Out[2]: [('Finance', 10), ('Marketing', 20), ('Sales', 30), ('IT', 40)]

# COMMAND ----------

# MAGIC %md
# MAGIC Convert PySpark RDD to DataFrame
# MAGIC Converting PySpark RDD to DataFrame can be done using toDF(), createDataFrame(). In this section, I will explain these two methods.
# MAGIC
# MAGIC Using rdd.toDF() function
# MAGIC PySpark provides toDF() function in RDD which can be used to convert RDD into Dataframe.
# MAGIC By default, toDF() function creates column names as “_1” and “_2”. This snippet yields below schema.

# COMMAND ----------

df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC toDF() has another signature that takes arguments to define column names as shown below.

# COMMAND ----------

deptColumns = ["dept_name","dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Using PySpark createDataFrame() function
# MAGIC SparkSession class provides createDataFrame() method to create DataFrame and it takes rdd object as an argument.

# COMMAND ----------

deptDF = spark.createDataFrame(rdd, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Using createDataFrame() with StructType schema
# MAGIC When you infer the schema, by default the datatype of the columns is derived from the data and set’s nullable to true for all columns. We can change this behavior by supplying schema using StructType – where we can specify a column name, data type and nullable for each field/column.
# MAGIC
# MAGIC If you wanted to know more about StructType, please go through how to use StructType and StructField to define the custom schema.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType
deptSchema = StructType([       
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema = deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)
