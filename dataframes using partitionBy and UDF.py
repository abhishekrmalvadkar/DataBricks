# Databricks notebook source
simpledata =  [(1,"Sagar","CSE","UP",80),\
               (2,"Shivam","IT","MP",86),\
               (3,"Muni","MECH","AP",70),\
               (1,"Sagar","CSE","UP",80),\
               (3,"Muni","MECH","AP",70),\
              ]
columns = ["ID","Student_Name","Department","City","Marks"]
df = spark.createDataFrame(data=simpledata, schema=columns)
df.show()

# COMMAND ----------

df.write.option("header","true").partitionBy('ID','City').mode("overwrite").csv('/FileStore/tables/partitionBy_Output')
df.show()

# COMMAND ----------

df_op = spark.read.option("header","true").csv("/FileStore/tables/partitionBy_Output/ID=1")
df_op.show()
df_op1 = spark.read.option("header","true").csv('/FileStore/tables/partitionBy_Output/ID=1/City=UP')
df_op1.show()

# COMMAND ----------

def convertcase(st):
  a=""
  for i in st:
    if('a'<=i<='z'):
      a=a+i.upper()
    else:
      a=a+i.lower()
  return a

  

# COMMAND ----------

from pyspark.sql.functions import udf,col
convertcase_df = udf(convertcase)

# COMMAND ----------

df.select(col("ID"),convertcase_df(col("Student_Name")).alias("STUD_NAME"),convertcase_df(col("City")).alias("CITY")).show()

# COMMAND ----------


