# Databricks notebook source
# MAGIC %run /Users/sathyapriya.r@diggibyte.com/assignments/assignment_1/source_to_bronze/utils

# COMMAND ----------

employee_schema = "EmployeeID INT, EmployeeName STRING, Department STRING, Country STRING, Salary INT, Age INT"
department_schema = "DepartmentID STRING, DepartmentName STRING"
country_schema = "CountryCode STRING, CountryName STRING"


# COMMAND ----------

# Read employee dataset
employee_df = spark.read.csv("dbfs:/FileStore/assignments/assignment_1/resources/employee.csv", header=True, schema=employee_schema)
employee_df.head()

# COMMAND ----------

# Read department dataset
department_df = spark.read.csv("dbfs:/FileStore/assignments/assignment_1/resources/department.csv", header=True, schema=department_schema)
department_df.head()

# COMMAND ----------

# Read country dataset
country_df = spark.read.csv("dbfs:/FileStore/assignments/assignment_1/resources/country.csv", header=True, schema=country_schema)
country_df.head()


# COMMAND ----------

# Write DataFrames to DBFS location in CSV format
employee_df.write.csv("/source_to_bronze/employee.csv", header=True)
department_df.write.csv("/source_to_bronze/department.csv", header=True)
country_df.write.csv("/source_to_bronze/country.csv", header=True)

# COMMAND ----------

display(employee_df)

# COMMAND ----------

display(department_df)

# COMMAND ----------

display(country_df)