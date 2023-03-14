# Databricks notebook source
#from pyspark.sql.functions import from_unixtime, to_utc_timestamp, to_date,col

# Create a PySpark DataFrame with the given data and schema, and convert the "Issue Date" column to timestamp format
data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001"),
        ("Refrigerator", 1648770999000, 35000, "LG", None, "0002"),
        ("Air Cooler", 1648770948000, 45000, "Voltas", None, "0003")]
schema = ["Product Name", "Issue Date", "Price", "Brand", "Country", "Product number"]
df = spark.createDataFrame(data, schema=schema)

df.show()



# COMMAND ----------

schema = ["SourceId","TransactionNumber","Language","ModelNumber","StartTime","ProductNumber"]

data = [(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000","0001"),
       (150439,234567,"UK",345678,"2021-12-27T08:20:29.842+0000","0001"),
       (150647,345678,"ES",234567,"2021-12-27T08:20:29.842+0000","0001")]

df1 = spark.createDataFrame(data, schema = schema)
df1.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,unix_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# define the schema for the table
schema = StructType([
    StructField("SourceId", IntegerType(), True),
    StructField("TransactionNumber", IntegerType(), True),
    StructField("Language", StringType(), True),
    StructField("ModelNumber", IntegerType(), True),
    StructField("StartTime", StringType(), True),
    StructField("ProductNumber", StringType(), True)
])

# create a PySpark DataFrame with the provided information and convert the StartTime field to a timestamp
data = [
    (150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", "0001"),
    (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", "0002"),
    (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", "0003")
]
df = spark.createDataFrame(data, schema)
df.display()

df2 = df1.withColumn("start_time_ms", unix_timestamp("StartTime", "yyyy-MM-dd'T'HH:mm:ss.SSSZ") * 1000)
df2.display()
#df = df.withColumn("StartTime", to_timestamp(df["StartTime"], "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

# create a temporary view for the DataFrame
#df.createOrReplaceTempView("transactions")

# query the table to verify it was created correctly
#spark.sql("SELECT * FROM transactions").show()


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col

schema1 = StructType([
    StructField('ProductName', StringType(), True),
    StructField('IssueDate', LongType(), True),
    StructField('Price', IntegerType(), True),
    StructField('Brand', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('ProductNumber', StringType(), True)
])

# create DataFrame
data1 = [
    ('Washing Machine', 1648770933000, 20000, 'Samsung', 'India', '0001'),
    ('Refrigerator', 1648770999000, 35000, 'LG', None, '0002'),
    ('Air Cooler', 1648770948000, 45000, 'Voltas', None, '0003')
]

df1 = spark.createDataFrame(data1, schema1)

# show DataFrame
df1.display()

schema2 = StructType([
    StructField('SourceId', IntegerType(), True),
    StructField('TransactionNumber', IntegerType(), True),
    StructField('Language', StringType(), True),
    StructField('ModelNumber', IntegerType(), True),
    StructField('StartTime', StringType(), True),
    StructField('ProductNumber', StringType(), True)
])

# create DataFrame
data2 = [
    (150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '0001'),
    (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', '0002'),
    (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', '0003')
]

df2 = spark.createDataFrame(data2, schema2)

# show DataFrame
df2.display()

# join DataFrames on ProductNumber
df3 = df1.join(df2, df1.ProductNumber == df2.ProductNumber, 'inner')

df3.display()
# filter rows with Country as 'EN'
df4 = df3.filter(col('Language') == 'EN')

df4.display()

# COMMAND ----------


