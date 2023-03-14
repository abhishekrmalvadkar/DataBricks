# Databricks notebook source
from pyspark.sql.functions import first,desc,rank,min,max,avg,sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window


schema = StructType ([
             StructField("employee_name",StringType(),True),
             StructField("department",StringType(),True),
             StructField("salary",IntegerType(),True)
          ])

data = [
   ("James","Sales",3000),
   ("Micheal","Sales",4600),
   ("Robert","Sales",4100),
   ("Maria","Finance",3000),
   ("Raman","Finance",3000),
   ("Scott","Finance",3300),
   ("Jen","Finance",3900),
   ("Jeff","Marketing",3000),
   ("Kumar","Marketing",2000)
]

df = spark.createDataFrame (data, schema)

df.show()

firstRow_df = df.groupBy("department").agg(first('employee_name').alias('EMP_NAME'),first('salary').alias('SALARY'))
firstRow_df.show()

highestSalary_df = df.orderBy(df.salary.desc()).limit(1)

highestSalary_df.show()


window = Window.partitionBy("department").orderBy(desc("salary"))
highest_paid_employees = df.select("*", rank().over(window).alias("rank")).filter("rank = 1")

highest_paid_employees.show()

salaryTypes_df = df.groupBy('department').agg(max('salary'),min('salary'),avg('salary'),sum('salary'))
salaryTypes_df.show()

# COMMAND ----------

#groupBy() method to group the data by department. 
#agg() method to select the first employee name and salary for each department group using the first() function.
