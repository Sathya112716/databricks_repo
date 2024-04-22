# Databricks notebook source file
# MAGIC %run /Users/sathyapriya.r@diggibyte.com/src/assignment_1/source_to_bronze/utils

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS Employee_info")

# COMMAND ----------

# Read files located in DBFS location source_to_bronze with custom schema
employee_schema = "Employee_ID INT, Employee_Name STRING, Department STRING, Country STRING, Salary INT, Age INT, Load_Date DATE"
department_schema = "Department_ID STRING, Department_Name STRING, Load_Date DATE"
country_schema = "Country_Code STRING, Country_Name STRING, Load_Date DATE"


# COMMAND ----------

employee_df =spark.read.csv("/source_to_bronze/employee.csv", employee_schema)
department_df = spark.read.csv("/source_to_bronze/department.csv", department_schema)
country_df = spark.read.csv("/source_to_bronze/country.csv", country_schema)


# Convert CamelCase columns to snake_case using UDF
employee_df = convert_camel_to_snake_case(employee_df)
department_df = convert_camel_to_snake_case(department_df)
country_df = convert_camel_to_snake_case(country_df)

# Add load_date column with current date
from pyspark.sql.functions import current_date
employee_df = employee_df.withColumn("load_date", current_date())
department_df = department_df.withColumn("load_date", current_date())
country_df = country_df.withColumn("load_date", current_date())

# Write DataFrame as Delta table to DBFS location
write_delta_table(employee_df,"Employee_info","dim_employee", "EmployeeID", "/silver/Employee_info/dim_employee")

# COMMAND ----------

