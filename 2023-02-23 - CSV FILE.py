# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql.functions import col,lit
# File location and type
file_location = "/FileStore/tables/transaction__1_.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#withColumnRenamed is better method because we dont have to mention other column names for which we dont want to change the colum names

df = df.withColumnRenamed("product_id","Prod_ID").withColumnRenamed("userid","USER_ID").withColumnRenamed("price","PRICE")
#display(df)
#df2 = df.selectExpr("transaction_id as Trans_ID","Prod_ID")

#Adding columns 
#lit() function is used to add constant or literal value as a new column to the DataFrame
df = df.withColumn("Country",lit("India")).withColumn("Salary",col("PRICE")*col("USER_ID"))

# Filtering records 
df.filter(df.USER_ID==101).show()
df.filter((col("USER_ID")==102) | (col("Prod_ID")==1000008)).show()

# Sorting records
#df.sort(df.USER_ID).show()
df.orderBy(df.USER_ID.desc()).show()
#df.show()
#df2.show()

# COMMAND ----------

#removing duplicate records
#df1=df.distinct(["USER_ID"])
df.dropDuplicates(["USER_ID"]).show()
#df1.show()


# COMMAND ----------

#with clause
from pyspark.sql.functions import col, when
df.show()

df_with = df.withColumn('Country', when(col('USER_ID')=='101',"Bangalore-India").when(col('USER_ID')=='102',"Mumbai-India").otherwise("Unknown"))
df_with.show()

# COMMAND ----------

###########GROUP BY function
#df=df.groupBy('USER_ID').max('Salary')
#df1=df.groupBy('USER_ID').agg(max('Salary').alias('Max_Salary'),min('Salary').alias('Min_Salary'))
#df.show()
#df1.show()

# COMMAND ----------

#write CSV files: This can be done by 2 ways :-
# 1. Store it in the form of a table 
# 2. Store it as a csv file
df.write.mode('overwrite').csv("/FileStore/tables_Output/transaction_output_1.csv")

# COMMAND ----------

#df_op=spark.read.csv('/FileStore/tables_Output/transaction_output_1.csv/part-00000-tid-8525438354786511599-2b137bd7-5ace-497a-8c03-7fa05d2a3a22-102-1-c000.csv')
#df_op.show()

# COMMAND ----------

  df.printSchema()

# COMMAND ----------

#With clause
df.show()

# COMMAND ----------

# Create a view or table

df.createOrReplaceTempView("sampltable")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from sampltable

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "transaction__1__csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
