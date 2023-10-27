# Databricks notebook source
# MAGIC %md
# MAGIC 1. StructType – Defines the structure of the Dataframe
# MAGIC PySpark provides from pyspark.sql.types import StructType class to define the structure of the DataFrame.
# MAGIC
# MAGIC StructType is a collection or list of StructField objects.
# MAGIC
# MAGIC PySpark printSchema() method on the DataFrame shows StructType columns as struct.
# MAGIC
# MAGIC 2. StructField – Defines the metadata of the DataFrame column
# MAGIC PySpark provides pyspark.sql.types import StructField class to define the columns which include column name(String), column type (DataType), nullable column (Boolean) and metadata (MetaData)%md
# MAGIC website url: https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/
# MAGIC
# MAGIC PySpark StructType & StructField classes are used to programmatically specify the schema to the DataFrame and create complex columns like nested struct, array, and map columns. StructType is a collection of StructField’s that defines column name, column data type, boolean to specify if the field can be nullable or not and metadata.
# MAGIC
# MAGIC In this article, I will explain different ways to define the structure of DataFrame using StructType with PySpark examples. Though PySpark infers a schema from data, sometimes we may need to define our own column names and data types and this article explains how to define simple, nested, and complex schemas.
# MAGIC
# MAGIC 3. Using PySpark StructType & StructField with DataFrame
# MAGIC While creating a PySpark DataFrame we can specify the structure using StructType and StructField classes. As specified in the introduction, StructType is a collection of StructField’s which is used to define the column name, data type, and a flag for nullable or not. Using StructField we can also add nested struct schema, ArrayType for arrays, and MapType for key-value pairs which we will discuss in detail in later sections.
# MAGIC
# MAGIC The below example demonstrates a very simple example of how to create a StructType & StructField on DataFrame and it’s usage with sample data to support it.

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Adding & Changing struct of the DataFrame
# MAGIC Using PySpark SQL function struct(), we can change the struct of the existing DataFrame and add a new StructType to it. The below example demonstrates how to copy the columns from one structure to another and adding a new column. PySpark Column Class also provides some functions to work with the StructType column.

# COMMAND ----------

from pyspark.sql.functions import col,struct,when
updated_df=df2.withColumn("OtherInfo",struct(df2.id.alias("id"),df2.gender.alias("sex"),df2.salary.alias("salary"),when(df2.salary<=2000,'Low').when(df2.salary<=4000,"Medium").otherwise("High").alias("salary_grade")))
updated_df.printSchema()
updated_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,BooleanType,DoubleType,StructField,StructType,ArrayType,MapType
schema=StructType([StructField("name",StructType([
    StructField("firstName",StringType(),True),
    StructField("middleName",StringType(),True),
    StructField("lastName",StringType(),True)
])),
StructField("languages",ArrayType(StringType())),
StructField("properties",MapType(StringType(),StringType()))])
data=[(("abc","","def"),["java","sql","python"],{"hair":"black","skin":"fair"}),
      (("bcd","","efgh"),["java","databricks","python"],{"hair":"white","skin":"fair"})
]
df=spark.createDataFrame(data,schema)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Creating StructType object struct from JSON file
# MAGIC If you have too many columns and the structure of the DataFrame changes now and then, it’s a good practice to load the SQL StructType schema from JSON file. You can get the schema by using df2.schema.json() , store this in a file and will use it to create a the schema from this file.

# COMMAND ----------


print(df.schema.json())
