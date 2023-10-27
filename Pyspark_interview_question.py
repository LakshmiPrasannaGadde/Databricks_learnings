# Databricks notebook source
# MAGIC %md
# MAGIC My Data Engineer Interview Coding Question : (PySpark)
# MAGIC =============================
# MAGIC Let's Start.. 🎯
# MAGIC
# MAGIC 🏹 CHALLENGE : We have input_df -> emp_name, emp_id, year, month, base salary, bonus
# MAGIC Output should be total salary bracket, count of employees in salary bracket
# MAGIC
# MAGIC Approach :
# MAGIC ✅ Creating a DataFrame          
# MAGIC ✅ Calculating Monthly_Salary by adding two columns (Basic_Salary, Bonus)                
# MAGIC ✅ Calculating Total_Salary for year by grouping with emp_name and year             
# MAGIC ✅ Calculating Salary_Bracket based on the Total_Salary                   
# MAGIC ✅ Calculating no.of employees under each Salary_Bracket                      
# MAGIC ✅ Done 😊                  
# MAGIC website link: https://www.linkedin.com/feed/update/urn:li:activity:7117103379687821313/

# COMMAND ----------

data=[("Rajesh", 1001 ,"January", 2022, 100000, 0 ),
("Rajesh", 1001 ,"February",2022, 100000, 0 ),
("Rajesh", 1001 ,"March",2022, 100000, 0 ),
("Rajesh", 1001 ,"April",2022, 100000, 50000 ),
("Rajesh", 1001 ,"May",2022, 100000, 0 ),
("Rajesh", 1001 ,"June",2022, 100000, 0 ),
("Rajesh", 1001 ,"July",2022, 100000, 0 ),
("Rajesh", 1001 ,"August",2022, 100000, 50000 ),
("Rajesh", 1001 ,"September",2022, 100000, 0 ),
("Rajesh", 1001 ,"October",2022, 100000, 0 ),
("Rajesh", 1001 ,"November",2022, 100000, 0 ),
("Rajesh", 1001 ,"December",2022, 100000, 50000 ),
("devid", 1002 ,"January", 2022, 10000, 0 ),
("devid", 1002 ,"February",2022, 10000, 0 ),
("devid", 1002 ,"March",2022, 10000, 0 ),
("devid", 1002 ,"April",2022, 10000, 5000 ),
("devid", 1002 ,"May",2022, 10000, 0 ),
("devid", 1002 ,"June",2022, 10000, 0 ),
("devid", 1002 ,"July",2022, 10000, 0 ),
("devid", 1002 ,"August",2022, 10000, 5000 ),
("devid", 1002 ,"September",2022, 10000, 0 ),
("devid", 1002 ,"October",2022, 10000, 0 ),
("devid", 1002 ,"November",2022, 10000, 0 ),
("devid", 1002 ,"December",2022, 10000, 5000 ),
("sam", 1003 ,"January", 2022, 50000, 0 ),
("sam", 1003 ,"February",2022, 50000, 0 ),
("sam", 1003 ,"March",2022, 50000, 0 ),
("sam", 1003 ,"April",2022, 50000, 5000 ),
("sam", 1003 ,"May",2022, 50000, 0 ),
("sam", 1003 ,"June",2022, 50000, 0 ),
("sam", 1003 ,"July",2022, 50000, 0 ),
("sam", 1003 ,"August",2022, 50000, 5000 ),
("sam", 1003 ,"September",2022, 50000, 0 ),
("sam", 1003 ,"October",2022, 50000, 0 ),
("sam", 1003 ,"November",2022, 50000, 0 ),
("sam", 1003 ,"December",2022, 50000, 5000 )
]
columns = ["emp_name", "emp_id", "Month","Year", "Base_Salary", "Bonus"]
df = spark.createDataFrame(data,columns)
df.printSchema()
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, lit, countDistinct
df.withColumn("monthly_salary",df["Base_Salary"]+df["Bonus"]).groupBy("emp_name","Year").agg(sum(col("monthly_salary")).alias("sum_salary")).withColumn("salary_bracket",when(col("sum_salary")<=500000,"less").when(col("sum_salary")>500000,"high pay")).groupBy(col("salary_bracket")).count().show()
