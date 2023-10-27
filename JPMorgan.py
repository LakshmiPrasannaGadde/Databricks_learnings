# Databricks notebook source
# MAGIC %md
# MAGIC #bigdata #sql #interviewquestions #jpmorgan
# MAGIC
# MAGIC https://www.linkedin.com/feed/update/urn:li:activity:7118037578146070528?updateEntityUrn=urn%3Ali%3Afs_updateV2%3A%28urn%3Ali%3Aactivity%3A7118037578146070528%2CFEED_DETAIL%2CEMPTY%2CDEFAULT%2Cfalse%29
# MAGIC
# MAGIC Write a Pyspark query to report, How much cubic feet of volume the inventory occupies in each warehouse.
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The query result format is in the following example.
# MAGIC
# MAGIC Warehouse table:                    
# MAGIC +------------+--------------+-------------+                
# MAGIC | name | product_id | units |               
# MAGIC +------------+--------------+-------------+                        
# MAGIC | LCHouse1 | 1 | 1 |         
# MAGIC | LCHouse1 | 2 | 10 |              
# MAGIC | LCHouse1 | 3 | 5 |              
# MAGIC | LCHouse2 | 1 | 2 |                
# MAGIC | LCHouse2 | 2 | 2 |                     
# MAGIC | LCHouse3 | 4 | 1 |                 
# MAGIC +------------+--------------+-------------+               
# MAGIC
# MAGIC Products table:                           
# MAGIC +------------+--------------+------------+----------+-----------+             
# MAGIC | product_id | product_name | Width | Length | Height |                         
# MAGIC +------------+--------------+------------+----------+-----------+                
# MAGIC | 1 | LC-TV | 5 | 50 | 40 |                               
# MAGIC | 2 | LC-KeyChain | 5 | 5 | 5 |                               
# MAGIC | 3 | LC-Phone | 2 | 10 | 10 |                                 
# MAGIC | 4 | LC-T-Shirt | 4 | 10 | 20 |                          
# MAGIC +------------+--------------+------------+----------+-----------+                  
# MAGIC
# MAGIC Result table:                   
# MAGIC +----------------+------------+                   
# MAGIC | warehouse_name | volume |                         
# MAGIC +----------------+------------+                
# MAGIC | LCHouse1 | 12250 |                        
# MAGIC | LCHouse2 | 20250 |                    
# MAGIC | LCHouse3 | 800 |                          
# MAGIC +----------------+------------+                   
# MAGIC  
# MAGIC Volume:(length * width * height) 
# MAGIC

# COMMAND ----------

#warehouse dataset
wh_data=[('LCHouse1',1,1),('LCHouse1',2,10),('LCHouse1',3,5),('LCHouse2',1,2),('LCHouse2',2,2),('LCHouse3',4,1)]
wh_column=['name','product_id','units']
wh_df=spark.createDataFrame(wh_data,wh_column)
wh_df.show(truncate=False)

# COMMAND ----------

#Products dataframe:
product_data=[(1,'LC-TV',5,50,40),(2,'LC-KeyChain',5,5,5),(3,'LC-Phone',2,10,10),(4,'LC-T-Shirt',4,10,20)]
product_column=['product_id','product_name','Width','Length','Height']
product_df=spark.createDataFrame(product_data,product_column)
product_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import sum
wh_df.join(product_df,wh_df.product_id==product_df.product_id,how="inner").groupBy(wh_df.name).agg(sum(product_df.Width*product_df.Length*product_df.Height*wh_df.units).alias("volume")).sort(wh_df.name.asc()).show()
