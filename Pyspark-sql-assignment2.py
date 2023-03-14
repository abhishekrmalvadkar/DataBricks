# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
  StructField ("Product",StringType(),True),
  StructField ("Amount",IntegerType(),True),
  StructField ("Country",StringType(),True)
  ])

data = [
  ("Banana",1000,'USA'),
  ("Carrots",1500,'INDIA'),
  ("Beans",1600,'Sweden'),
  ("Orange",2000,'UAE'),
  ("Orange",2000,'UK'),
  ("Banana",400,'China'),
  ("Banana",1200,'China'),
 ]

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

#The pivot() function in PySpark is used to transform rows into columns. It groups the DataFrame based on the values of a specified column and performs an aggregate operation (sum, count, etc.) on another column, and then pivots the results to show the aggregated values as columns.
#The pivot() function requires three parameters: the column to pivot on, the column to aggregate, and the aggregation function to apply.

df_pivot = df.groupBy('Product').pivot('Country').sum('Amount')
df_pivot.show()

