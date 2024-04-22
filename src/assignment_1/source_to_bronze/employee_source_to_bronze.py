# Databricks notebook source file
# MAGIC %run /Users/sathyapriya.r@diggibyte.com/src/assignment_1/source_to_bronze/utils

# Read employee dataset
employee_df = spark.read.csv("dbfs:/FileStore/src/assignment_1/resources/employee.csv", header=True, schema=employee_schema)

# Read department dataset
department_df = spark.read.csv("dbfs:/FileStore/src/assignment_1/resources/department.csv", header=True, schema=department_schema)

# Read country dataset
country_df = spark.read.csv("dbfs:/FileStore/src/assignment_1/resources/country.csv", header=True, schema=country_schema)

# Write DataFrames to DBFS location in CSV format
employee_df.write.csv("/source_to_bronze/employee.csv", header=True)
department_df.write.csv("/source_to_bronze/department.csv", header=True)
country_df.write.csv("/source_to_bronze/country.csv", header=True)

