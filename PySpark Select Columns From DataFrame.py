# Databricks notebook source
# MAGIC %md
# MAGIC website url: https://sparkbyexamples.com/pyspark/select-columns-from-pyspark-dataframe/                      
# MAGIC In PySpark, select() function is used to select single, multiple, column by index, all columns from the list and the nested columns from a DataFrame, PySpark select() is a transformation function hence it returns a new DataFrame with the selected columns.

# COMMAND ----------


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Select Single & Multiple Columns From PySpark                                   
# MAGIC You can select the single or multiple columns of the DataFrame by passing the column names you wanted to select to the select() function. Since DataFrame is immutable, this creates a new DataFrame with selected columns. show() function is used to show the Dataframe contents.                               
# MAGIC Below are ways to select single, multiple or all columns.                

# COMMAND ----------

df.select("firstname","lastname").show()
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()
from pyspark.sql.functions import col
df.select(col("firstname"),col("lastname")).show()

#Select columns by regular expression
df.select(df.colRegex("`^.*properties*`")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Select All Columns From List
# MAGIC Sometimes you may need to select all DataFrame columns from a Python list. In the below example, we have all columns in the columns list object.

# COMMAND ----------

# Select All columns from List
df.select(*columns).show()

# Select All columns
df.select([col for col in df.columns]).show()
df.select("*").show()

# COMMAND ----------

#Using a python list features, you can select the columns by index.

#Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)

#Selects columns 2 to 4  and top 3 rows
df.select(df.columns[2:4]).show(3)


# COMMAND ----------

# MAGIC %md
# MAGIC Select Nested Struct Columns from PySpark
# MAGIC If you have a nested struct (StructType) column on PySpark DataFrame, you need to use an explicit column qualifier in order to select. If you are new to PySpark and you have not learned StructType yet, I would recommend skipping the rest of the section or first Understand PySpark StructType before you proceed.
# MAGIC
# MAGIC First, let’s create a new DataFrame with a struct type.

# COMMAND ----------

data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False) # shows all columns

# COMMAND ----------

# MAGIC %md
# MAGIC Yields below schema output. If you notice the column name is a struct type which consists of columns firstname, middlename, lastname.                              
# MAGIC Now, let’s select struct column.

# COMMAND ----------

df2.select("name").show(truncate=False)
#This returns struct column name as is.

# COMMAND ----------

#In order to select the specific column from a nested struct, you need to explicitly qualify the nested struct column name.
df2.select("name.firstname","name.lastname").show(truncate=False)
#This outputs firstname and lastname from the name struct column.

# COMMAND ----------

#In order to get all columns from struct column.
df2.select("name.*").show(truncate=False)
