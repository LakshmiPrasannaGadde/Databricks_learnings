# Databricks notebook source
# MAGIC %md
# MAGIC website link: https://sparkbyexamples.com/pyspark/pyspark-show-display-dataframe-contents-in-table/
# MAGIC PySpark DataFrame show() is used to display the contents of the DataFrame in a Table Row and Column Format. By default, it shows only 20 Rows, and the column values are truncated at 20 characters.

# COMMAND ----------

# MAGIC %md
# MAGIC Quick Example of show()
# MAGIC Following are quick examples of how to show the contents of DataFrame.
# MAGIC # Default - displays 20 rows and 
# MAGIC # 20 charactes from column value 
# MAGIC df.show()
# MAGIC
# MAGIC #Display full column contents
# MAGIC df.show(truncate=False)
# MAGIC
# MAGIC # Display 2 rows and full column contents
# MAGIC df.show(2,truncate=False) 
# MAGIC
# MAGIC # Display 2 rows & column values 25 characters
# MAGIC df.show(2,truncate=25) 
# MAGIC
# MAGIC # Display DataFrame rows & columns vertically
# MAGIC df.show(n=3,truncate=25,vertical=True)
# MAGIC
# MAGIC # Syntax
# MAGIC def show(self, n=20, truncate=True, vertical=False):
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]
df = spark.createDataFrame(data,columns)
df.show()

# Output
#+-----+--------------------+
#|Seqno|               Quote|
#+-----+--------------------+
#|    1|Be the change tha...|
#|    2|Everyone thinks o...|
#|    3|The purpose of ou...|
#|    4|            Be cool.|
#+-----+--------------------+

# COMMAND ----------

# MAGIC %md
# MAGIC As you see above, values in the Quote column are truncated at 20 characters, Let’s see how to display the full column contents.

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC By default show() method displays only 20 rows from PySpark DataFrame. The below example limits the rows to 2 and full column contents. Our DataFrame has just 4 rows hence I can’t demonstrate with more than 4 rows. If you have a DataFrame with thousands of rows try changing the value from 2 to 100 to display more than 20 rows.

# COMMAND ----------

df.show(2,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Show() with Truncate Column Values
# MAGIC You can also truncate the column value at the desired length. By default it truncates after 20 characters however, you can display all contents by using truncate=False. If you wanted to truncate at a specific length use truncate=n.

# COMMAND ----------

df.show(2,truncate=25)

# COMMAND ----------

# MAGIC %md
# MAGIC Display Contents Vertically
# MAGIC Finally, let’s see how to display the DataFrame vertically record by record.

# COMMAND ----------

# Display DataFrame rows & columns vertically
df.show(n=3,truncate=50,vertical=True)

#-RECORD 0--------------------------
# Seqno | 1                         
# Quote | Be the change that you... 
#-RECORD 1--------------------------
# Seqno | 2                         
# Quote | Everyone thinks of cha... 
#-RECORD 2--------------------------
# Seqno | 3                         
# Quote | The purpose of our liv... 
