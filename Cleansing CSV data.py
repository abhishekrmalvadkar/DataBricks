# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

df1 = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/Cleansing.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

df2 = df1.select("id", when(col("ind")==lit("FN"),col("fname")).otherwise("null").alias("fname"),
            when(col("ind")==lit("LN"),col("fname")).otherwise("null").alias("lname"),
            when(col("ind")==lit("PH"),col("fname")).otherwise("null").alias("phone"),
            when(col("ind")==lit("AD"),concat_ws(",",col("fname"),col("lname"),col("apartment"),col("street"),col("city"),col("country"))).otherwise("null").alias("address"))

# COMMAND ----------

df2.display()

# COMMAND ----------

df3 = df2.groupBy("id").agg(min("fname").alias("fname"),min("lname").alias("lname"),min("phone").alias("phone"),min("address").alias("address"))
df3.display()

# COMMAND ----------

#df5 = df3.dropna("fname", how="all")

help('dropna')

# COMMAND ----------

df4 = df3.filter((col("fname")!=lit("null")) & (col("lname")!=lit("null")))
df4.display()

# COMMAND ----------

df5 = df4.withColumn("apartment", split("address",",").getItem(0))\
      .withColumn("street",split("address",",").getItem(1))\
        .withColumn("city",split("address",",").getItem(2))\
            .withColumn("country",split("address",",").getItem(3))

df5.select("id","fname","lname","apartment","street","city","country","phone").display()

# COMMAND ----------


