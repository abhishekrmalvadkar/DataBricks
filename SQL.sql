-- Databricks notebook source
show tables

-- COMMAND ----------

create database sparksql

-- COMMAND ----------

create table sparksql.customer ( id int, fname string, lname string)

-- COMMAND ----------

select * from sparksql.customer

-- COMMAND ----------

create view  sparksql.customer_view as select * from sparksql.customer where id > 3 

-- COMMAND ----------

select * from sparksql.customer_view

-- COMMAND ----------

show create table sparksql.customer_view

-- COMMAND ----------


CREATE global temporary VIEW global_customer_view as select * from sparksql.customer where id =1

-- COMMAND ----------

select * from global_temp.global_customer_view


-- COMMAND ----------

select * from temp_customer_view

-- COMMAND ----------


