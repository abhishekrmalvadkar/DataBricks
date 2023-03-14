# Databricks notebook source
sample_data = [(1,"Abhishek","3000000"),\
               (1,"Abhi","null")]

columns = ["ID","NAME","SALARY"]

df = spark.createDataFrame(data = sample_data, schema=columns)
df.show()
df.printSchema()

# COMMAND ----------

#this will work if we are reading the data from a csv file 
df=df.na.fill("None","SALARY")
df.show()

# COMMAND ----------

df=df.withColumn("ID",df.ID.cast("int")).withColumn("SALARY",df.SALARY.cast("int"))
df.printSchema()

# COMMAND ----------


