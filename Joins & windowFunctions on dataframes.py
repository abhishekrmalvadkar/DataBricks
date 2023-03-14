# Databricks notebook source
data1 = [("Abhishek","RM","CSE","100"),\
         ("abc","RM","ISE","200"),\
         ("xyz","MR","Mech","50")
        ]
columns = ["fname","lname","dept","marks"]

df1=spark.createDataFrame(data = data1, schema = columns)
df1.show()

# COMMAND ----------

data2 = [("A1","RM","CSE","100"),\
         ("B1","RM","ISE","200"),\
         ("C1","RM","CSE","40")
        ]
columns = ["fname","lname","dept","marks"]

df2=spark.createDataFrame(data = data2, schema = columns)
df2.show()

# COMMAND ----------

#df1.join(df2,df1.dept==df2.dept,"outer").select(col("df1.fname"),col("df2.lname")).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# select *, row_number() over (PARTITION BY colName order by colName) AS aliasName from TableName
#in pyspark, 1st write the inner part of the query i.e. window function and then 2nd write the outer query by using withColumn

df=df1.union(df2)
window = Window.partitionBy('dept').orderBy("marks")
df.withColumn("AliasColumn",row_number().over(window)).show()
df.show()

# COMMAND ----------


