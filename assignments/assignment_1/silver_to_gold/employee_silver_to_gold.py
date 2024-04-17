# Databricks notebook source
# MAGIC %run /Users/sathyapriya.r@diggibyte.com/assignments/assignments/source_to_bronze/utils

# COMMAND ----------

employee_df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/employee_info.db/dim_employee")

# COMMAND ----------

display(employee_df)

# COMMAND ----------

from pyspark.sql.functions import avg,desc,count

# COMMAND ----------

# Find salary of each department in descending order
salary_by_department = employee_df.groupBy("department").agg(avg("salary").alias("avg_salary")).orderBy(desc("avg_salary"))


# COMMAND ----------

employee_count = employee_df.groupBy("department", "country").agg(count("employee_id").alias("employee_count"))
display(employee_count)


# COMMAND ----------

