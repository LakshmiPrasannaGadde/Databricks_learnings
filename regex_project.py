# Databricks notebook source
# MAGIC %md
# MAGIC Topic :🚀 Real-World Data Preprocessing with PySpark! 🌟
# MAGIC
# MAGIC As a data engineer I recently encountered a common data wrangling challenge: messy product descriptions!
# MAGIC
# MAGIC Our dataset had product descriptions filled with special characters and formatting issues, making it hard to extract meaningful insights. But PySpark came to the rescue with its regex_replace function! 💡✨
# MAGIC
# MAGIC Just a example of such cases where you use in your project too.

# COMMAND ----------


from pyspark.sql.functions import regexp_replace,col
data = [("Product A: $19.99!",),
  ("Special Offer on Product B - $29.95",),
  ("Product C (Limited Stock)",)]
df = spark.createDataFrame(data, ["description"])
# Clean and preprocess the descriptions using regex_replace
cleaned_df = df.withColumn("cleaned_description",
       regexp_replace(col("description"), '[^a-zA-Z\s]', ''))
cleaned_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC web link:  https://sparkbyexamples.com/pyspark/pyspark-replace-column-values/#regexp_replace-replace-string-columns
# MAGIC
# MAGIC linkedIn interview post: https://www.linkedin.com/feed/update/urn:li:activity:7115644296111783936/
# MAGIC     
# MAGIC You can replace column values of PySpark DataFrame by using SQL string functions regexp_replace(), translate(), and overlay() with Python examples.
# MAGIC
# MAGIC In this article, I will cover examples of how to replace part of a string with another string, replace all columns, change values conditionally, replace values from a python dictionary, replace column value from another DataFrame column e.t.c
# MAGIC
# MAGIC First, let’s create a PySpark DataFrame with some addresses and will use this DataFrame to explain how to replace column values.

# COMMAND ----------


address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Replace String Column Values
# MAGIC By using PySpark SQL function regexp_replace() you can replace a column value with a string for another string/substring. regexp_replace() uses Java regex for matching, if the regex does not match it returns an empty string, the below example replace the street name Rd value with Road string on address column.

# COMMAND ----------

df=df.withColumn('address', regexp_replace('address', 'Rd', 'Road'))
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Replace Column Values Conditionally
# MAGIC In the above example, we just replaced Rd with Road, but not replaced St and Ave values, let’s see how to replace column values conditionally in PySpark Dataframe by using when().otherwise() SQL condition function.

# COMMAND ----------

from pyspark.sql.functions import col,regexp_replace,when
df.show(truncate=False)
df=df.withColumn("address",
                 when(df.address.endswith("Rd"),regexp_replace("address","Rd","Road"))
                 .when(df.address.endswith("St"),regexp_replace("address","St","Street"))
                 .when(df.address.endswith("Ave"),regexp_replace("address","Ave","Avenue"))
                 .otherwise(df.address)
)
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Replace Column Value with Dictionary (map)
# MAGIC You can also replace column values from the python dictionary (map). In the below example, we replace the string value of the state column with the full abbreviated name from a dictionary key-value pair, in order to do so I use PySpark map() transformation to loop through each row of DataFrame.

# COMMAND ----------

#Replace values from Dictionary
stateDic={'CA':'California','NY':'New York','DE':'Delaware'}
df2=df.rdd.map(lambda x:(x.id,x.address,stateDic[x.state])).toDF(["id","address","state"])
#df2=df.rdd.map(lambda x: 
    #(x.id,x.address,stateDic[x.state]) 
    #).toDF(["id","address","state"])
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace Column Value Character by Character
# MAGIC By using translate() string function you can replace character by character of DataFrame column value. In the below example, every character of 1 is replaced with A, 2 replaced with B, and 3 replaced with C on the address column.

# COMMAND ----------

from pyspark.sql.functions import translate
df=df.withColumn("address",translate("address","123","ABC"))
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Replace Column with Another Column Value
# MAGIC By using expr() and regexp_replace() you can replace column value with a value from another DataFrame column. In the below example, we match the value from col2 in col1 and replace with col3 to create new_column. Use expr() to provide SQL like expressions and is used to refer to another column to perform operations.

# COMMAND ----------


#Replace column with another column
from pyspark.sql.functions import expr
df = spark.createDataFrame(
   [("ABCDE_XYZ", "XYZ","FGH")], 
    ("col1", "col2","col3")
  )
df.withColumn("new_column",
              expr("regexp_replace(col1, col2, col3)")
              .alias("replaced_value")
              ).show()

#+---------+----+----+----------+
#|     col1|col2|col3|new_column|
#+---------+----+----+----------+
#|ABCDE_XYZ| XYZ| FGH| ABCDE_FGH|
#+---------+----+----+----------+


# COMMAND ----------

# MAGIC %md
# MAGIC  Replace All or Multiple Column Values
# MAGIC If you want to replace values on all or selected DataFrame columns, refer to How to Replace NULL/None values on all column in PySpark or How to replace empty string with NULL/None value
# MAGIC
# MAGIC 7. Using overlay() Function
# MAGIC Replace column value with a string value from another column.
# MAGIC
# MAGIC
# MAGIC https://sparkbyexamples.com/pyspark/pyspark-fillna-fill-replace-null-values/
# MAGIC
# MAGIC https://sparkbyexamples.com/pyspark/pyspark-replace-empty-value-with-none-on-dataframe/

# COMMAND ----------


#Overlay
from pyspark.sql.functions import overlay
df = spark.createDataFrame([("ABCDE_XYZ", "FGHI")], ("col1", "col2"))
df.select(overlay("col1", "col2", 6).alias("overlayed")).show()

#+---------+
#|overlayed|
#+---------+
#|ABCDE_FGH|
#+---------+
