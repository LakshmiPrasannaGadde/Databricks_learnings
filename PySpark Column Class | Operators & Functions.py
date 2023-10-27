# Databricks notebook source
# MAGIC %md
# MAGIC website link: https://sparkbyexamples.com/pyspark/pyspark-column-functions/
# MAGIC pyspark.sql.Column class provides several functions to work with DataFrame to manipulate the Column values, evaluate the boolean expression to filter rows, retrieve a value or part of a value from a DataFrame column, and to work with list, map & struct columns.
# MAGIC
# MAGIC In this article, I will cover how to create Column object, access them to perform operations, and finally most used PySpark Column Functions with Examples.
# MAGIC
# MAGIC Related Article: PySpark Row Class with Examples
# MAGIC
# MAGIC Key Points:
# MAGIC
# MAGIC PySpark Column class represents a single Column in a DataFrame.
# MAGIC It provides functions that are most used to manipulate DataFrame Columns & Rows.
# MAGIC Some of these Column functions evaluate a Boolean expression that can be used with filter() transformation to filter the DataFrame Rows.
# MAGIC Provides functions to get a value from a list column by index, map value by key & index, and finally struct nested column.
# MAGIC PySpark also provides additional functions pyspark.sql.functions that take Column object and return a Column type.
# MAGIC Note: Most of the pyspark.sql.functions return Column type hence it is very important to know the operation you can perform with Column type.

# COMMAND ----------

# MAGIC %md
# MAGIC One of the simplest ways to create a Column class object is by using PySpark lit() SQL function, this takes a literal value and returns a Column object.

# COMMAND ----------

from pyspark.sql.functions import lit
colObj = lit("sparkbyexamples.com")

# COMMAND ----------

# MAGIC %md
# MAGIC You can also access the Column from DataFrame by multiple ways.

# COMMAND ----------

from pyspark.sql.functions import col
data=[("James",23),("Ann",40)]
df=spark.createDataFrame(data,["name.fname","age"])
df.printSchema()

#Using dataframe object
df.select(df.age).show()
df.select(df["age"]).show()

#Using sql col function
df.select(col("age")).show()
df.select(col("`name.fname`")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Below example demonstrates accessing struct type columns. Here I have use PySpark Row class to create a struct type. Alternatively you can also create it by using PySpark StructType & StructField classes

# COMMAND ----------

#Create DataFrame with struct using Row class
from pyspark.sql import Row
data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="blue"))]
df=spark.createDataFrame(data)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Column Operators
# MAGIC PySpark column also provides a way to do arithmetic operations on columns using operators.

# COMMAND ----------

data = [(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data,["col1","col2","col3"])

#Arthmetic operations
df.select(df.col1+df.col2).show()
df.select(df.col1-df.col2).show()
df.select(df.col1*df.col2).show()
df.select(df.col1/df.col2).show()
df.select(df.col1%df.col2).show()

df.select(df.col1>df.col2).show()
df.select(df.col1<df.col2).show()
df.select(df.col1 == df.col2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. PySpark Column Functions Examples
# MAGIC Let’s create a simple DataFrame to work with PySpark SQL Column examples. For most of the examples below, I will be referring DataFrame object name (df.) to get the column.

# COMMAND ----------

data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)

# COMMAND ----------

# MAGIC %md
# MAGIC On below example df.fname refers to Column object and alias() is a function of the Column to give alternate name. Here, fname column has been changed to first_name & lname to last_name.
# MAGIC
# MAGIC On second example I have use PySpark expr() function to concatenate columns and named column as fullName.

# COMMAND ----------

df.select(df.fname.alias("first_name"),df.lname.alias("last_name")).show()

# COMMAND ----------

from pyspark.sql.functions import expr
df.select(expr("fname||','|| lname").alias("full_name")).show()

# COMMAND ----------

# name() returns same as alias()
df.select(df.fname.name("first_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC asc():                    Returns ascending order of the column.                      
# MAGIC asc_nulls_first():        Returns null values first then non-null values.                       
# MAGIC asc_nulls_last():	       Returns null values after non-null values.

# COMMAND ----------

df.sort(df.lname.asc()).show()
df.sort(df.lname.asc_nulls_first()).show()
df.sort(df.lname.asc_nulls_last()).show()

# COMMAND ----------

df.sort(df.lname.desc()).show()
df.sort(df.lname.desc_nulls_first()).show()
df.sort(df.lname.desc_nulls_last()).show()

# COMMAND ----------

#cast() & astype() – Used to convert the data Type.
#cast
df.select(df.id.cast("string")).show()
#astype
df.select(df.id.astype("string")).show()

# COMMAND ----------

#between() – Returns a Boolean expression when a column values in between lower and upper bound.(including lower and upper values)
df.filter(df.id.between(100,200)).show()

# COMMAND ----------

#contains() – Checks if a DataFrame column value contains a a value specified in this function.
df.filter(df.fname.contains("Tom")).show()

# COMMAND ----------

#startswith() & endswith() – Checks if the value of the DataFrame Column starts and ends with a String respectively.
df.filter(df.fname.startswith("Tom")).show()
df.filter(df.fname.startswith("J")).show()
df.filter(df.lname.endswith("nd")).show()
df.filter(df.lname.startswith("J") | df.lname.endswith("nd") ).show()

# COMMAND ----------

# isNull & isNotNull() – Checks if the DataFrame column has NULL or non NULL values.
#isNull & isNotNull
df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()

# COMMAND ----------

#like() & rlike() – Similar to SQL LIKE expression
df.filter(df.fname.like("%B%")).show()
# website link: https://sparkbyexamples.com/spark/spark-rlike-regex-matching-examples/

df.filter(df.id.rlike("^[0-9]*$")).show()
df.filter(df.fname.rlike("^*Cruise$")).show()
# Filter rows by cheking value contains in anohter column by ignoring case
df.filter(df.fname.rlike("(?i)^*brand$")).show()

# COMMAND ----------

#substr() – Returns a Column after getting sub string from the Column
df.select(df.fname.substr(1,5).alias("sub_string")).show()
#website link: https://www.geeksforgeeks.org/how-to-get-substring-from-a-column-in-pyspark-dataframe/

# COMMAND ----------

#when() & otherwise() – It is similar to SQL Case When, executes sequence of expressions until it matches the condition and returns a value when match.
from pyspark.sql.functions import when
df.select(when(df.lname.isNull(),"").otherwise(df.lname).alias("name")).show()
df.select(df.fname,df.lname,when(df.gender=='M',"Male").when(df.gender=='F',"Female").when(df.gender==None,"").otherwise(df.gender).alias("Gender")).show()

# COMMAND ----------

#isin() – Check if value presents in a List.
df.filter(df.id.isin(100,200,300,500)).show()
df.filter(df.id.isin(100,200,300,400)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC getField() – To get the value by key from MapType column and by stuct child name from StructType column
# MAGIC Rest of the below functions operates on List, Map & Struct data structures hence to demonstrate these I will use another DataFrame with list, map and struct columns. For more explanation how to use Arrays refer to PySpark ArrayType Column on DataFrame Examples & for map refer to PySpark MapType Examples.                                
# MAGIC
# MAGIC getItem() – To get the value by index from MapType or ArrayTupe & ny key for MapType column.
# MAGIC

# COMMAND ----------


#Create DataFrame with struct, array & map
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType
data=[(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
      (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
      (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
      (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

schema = StructType([
        StructField('name', StructType([
            StructField('fname', StringType(), True),
            StructField('lname', StringType(), True)])),
        StructField('languages', ArrayType(StringType()),True),
        StructField('properties', MapType(StringType(),StringType()),True)
     ])
df=spark.createDataFrame(data,schema)
df.printSchema()

# COMMAND ----------

df.select(df.name.getField("fname").alias("name1"),df.name.getItem("fname").alias("name2")).show()
df.select(df.properties.getField("hair"),df.properties.getItem("eye")).show()
df.select(df.languages.getItem(0).alias("primary_language")).show()
