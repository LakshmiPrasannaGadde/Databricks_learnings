# Databricks notebook source
# MAGIC %md
# MAGIC #bigdata #sql #interviewquestions
# MAGIC
# MAGIC postlink: https://www.linkedin.com/posts/venkateshh-chitteti-50b62316b_bigdata-sql-interviewquestions-activity-7117297691230957568-2Jfa?utm_source=share&utm_medium=member_desktop
# MAGIC
# MAGIC Write a pyspark query to find the team size of each of the employees.               
# MAGIC
# MAGIC Return the result table in any order.                 
# MAGIC
# MAGIC  The query result format is in the following example:               
# MAGIC
# MAGIC  Employee Table:                    
# MAGIC  +-------------+------------+                 
# MAGIC  | employee_id | team_id  |                   
# MAGIC  +-------------+------------+                       
# MAGIC  |  1   |  8   |                     
# MAGIC  |  2   |  8   |                     
# MAGIC  |  3   |  8   |                  
# MAGIC  |  4   |  7   |                   
# MAGIC  |  5   |  9   |                       
# MAGIC  |  6   |  9   |                         
# MAGIC  +-------------+------------+                      
# MAGIC                                             
# MAGIC  Result table:                       
# MAGIC +-------------+------------+           
# MAGIC | employee_id | team_size |                  
# MAGIC +-------------+------------+               
# MAGIC |  1   |  3   |                          
# MAGIC |  2   |  3   |                       
# MAGIC |  3   |  3   |                     
# MAGIC |  4   |  1   |                        
# MAGIC |  5   |  2   |                        
# MAGIC |  6   |  2   |                               
# MAGIC +-------------+------------+                     

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import count
emp_data=[(1,8),(2,8),(3,8),(4,7),(5,9),(6,9)]
emp_df=spark.createDataFrame(emp_data,['employee_id','team_id'])
windowspec=Window.partitionBy(emp_df.team_id)
emp_df.withColumn("team_size",count(emp_df.employee_id).over(windowspec)).select("employee_id","team_size").sort(emp_df.employee_id.asc()).show()
