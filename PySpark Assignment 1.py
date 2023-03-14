# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

#Defining the schema 
sch =  StructType([
  StructField("ProductName",StringType(), True),
  StructField("Issue Date",StringType(), True),
  StructField("Price",IntegerType(), True),
  StructField("Brand",StringType(), True),
  StructField("Country",StringType(), True),
  StructField("ProductNumber",StringType(), True)
])

#insert the data

data = [("Washing Machine","1648770933000",20000,"Samsung","India","0001"),
       ("Refrigerator","1648770999000",35000," LG",None,"0002"),
       ("Air Cooler","1648770948000",45000,"  Voltas",None,"0003")]
    
df = spark.createDataFrame(data, schema=sch)
df.show()

# COMMAND ----------

#Alternate way to insert create schema and insert data 

# Create a PySpark DataFrame with the given data
data = [("Washing Machine", 1648770933000, 20000, "Samsung   ", "India", "0001"),
        ("Refrigerator", 1648770999000, 35000, "  LG", None, "0002"),
        ("Air Cooler", 1648770948000, 45000, "   Voltas", None, "0003")]

columns = ["Product Name", "Issue Date", "Price", "Brand", "Country", "Product number"]
df = spark.createDataFrame(data, columns)

# Show the data in the DataFrame
df.show()


# COMMAND ----------

#What is Unix Time (Epoch Time)
#Unix time is also known as Epoch time which specifies the moment in time since 1970-01-01 00:00:00 UTC. It is the number of seconds passed since Epoch time. Epoch time is widely used in Unix like operating systems.
from pyspark.sql.functions import from_unixtime,trim

#here we are dividing by 1000 because the given time is in milliseconds
df = df.withColumn("Issue Date",from_unixtime(df["Issue Date"]/1000))
df.show()
# Convert the "Issue Date" column from timestamp to date format
df1 = df.withColumn("Issue Date", to_date(col("Issue Date")))

df1.show()

df2 = df.fillna({"Country": ''})
df2.show()

df3 = df.withColumn("Brand", trim(df.Brand))
df3.show()

# COMMAND ----------


#from pyspark.sql.functions import from_unixtime,to_utc_timestamp
#df = df.withColumn("IssueDate", to_utc_timestamp(from_unixtime(df["IssueDate"]/1000), "UTC"))
#df.show(truncate=False)

# COMMAND ----------


