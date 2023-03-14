# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#spark  = SparkSession.builder.appName("InsertRecords").getOrCreate()

schema = StructType([
  StructField("name",StringType(), True),
   StructField("dob",StringType(), True),
   StructField("gender",StringType(), True),
   StructField("salary",IntegerType(), True)
]) 

data = [
  ("{\"firstname\": \"James;\", \"middlename\": \"\", \"lastname\": \"Smith\"}", "03011998", "M", 3000),
    ("{\"firstname\": \"Michael\", \"middlename\": \"Rose\", \"lastname\":\"\"}", "10111998", "M", 20000),
    ("{\"firstname\": \"Robert\", \"middlename\":\"\",\"lastname\": \"Williams\"}", "02012000", "M", 3000),
    ("{\"firstname\": \"Maria\", \"middlename\": \"Anne\", \"lastname\": \"Jones\"}", "03011998", "F", 11000),
    ("{\"firstname\": \"Jen\", \"middlename\": \"Mary\", \"lastname\": \"Brown\"}", "04101998", "F", 10000)
]

df = spark.createDataFrame (data, schema)

df.display()

# COMMAND ----------

#df.select("name.firstname", "name.lastname", "salary").show()

# error occurs when you try to access a nested field of a PySpark DataFrame using dot notation on a string column.

# COMMAND ----------

from pyspark.sql.functions import get_json_object
df.select(get_json_object("name","$.firstname").alias("firstname"),
         get_json_object("name","$.lastname").alias("lastname"),
         "salary").show()

# COMMAND ----------

from pyspark.sql.functions import lit, year 
df = df.withColumn("Country",lit("India"))
df = df.withColumn("department",lit("Tech"))
#df = df.withColumn("age", lit(24))
df = df.withColumn("age", lit(2023) - df["dob"])

df.display()

# COMMAND ----------

from pyspark.sql.functions import col
df=df.withColumn("salary", col("salary")*2)
df.display()

# COMMAND ----------

#df.printSchema()

df = df.withColumn("salary",col("salary").cast(StringType()))
df = df.withColumn("age",col("age").cast("Integer"))
df.printSchema()

# COMMAND ----------

df = df.withColumn("Variable_Pay", col("salary")*5)
df.display()

# COMMAND ----------

from pyspark.sql.functions import col, struct, get_json_object

df = df.select(
                          get_json_object(col("name"), "$.firstname").alias("firstposition"),
                          get_json_object(col("name"), "$.middlename").alias("secondposition"),
                          get_json_object(col("name"), "$.lastname").alias("lastposition"),
                          col("dob"),
                          col("salary"))
#df = df.drop("name")
df.display()


#df = df.withColumn("name", 
 #                  struct(col("name.firstname").alias("firstposition"), 
  #                        col("name.middlename").alias("secondposition"), 
   #                       col("name.lastname").alias("lastposition")))
#df = df.drop("name.firstname", "name.middlename", "name.lastname")
#df.show()



# COMMAND ----------

max_salary_row = df.orderBy(df.salary.asc()).first()
print(max_salary_row)


# COMMAND ----------

df = df.drop("department", "age")



# COMMAND ----------

df.display()

# COMMAND ----------

distinct_dob = df.select("dob").distinct()
distinct_salary = df.select("salary").distinct()

distinct_dob.show()
distinct_salary.show()

# COMMAND ----------


