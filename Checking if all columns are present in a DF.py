# Databricks notebook source
schema = StructType([
    StructField("Company",StringType(),True),
    StructField("Model",StringType(),True),
    StructField("Price",StringType(),True),
    StructField("Color",StringType(),True)
])

df_read = spark.read.format("csv").option('header',True).schema(schema).load('dbfs:/FileStore/Cars.csv')

# COMMAND ----------

df_read.display()

# COMMAND ----------

expected_col = set(['Company','Model','Price','Colors'])

# COMMAND ----------

if set(df_read.columns) == expected_col:
    print("All the columns are present")
else:
    missing_cols = expected_col - set(df_read.columns)
    print(f"The following columns are missing: {missing_cols}")

# COMMAND ----------


