# Databricks notebook source file
# MAGIC %run /Users/sathyapriya.r@diggibyte.com/assignments/assignment_1/source_to_bronze/utils

# COMMAND ----------

# MAGIC %run /Users/sathyapriya.r@diggibyte.com/assignments/assignment_1/bronze_to_silver/employee_bronze_to_silver

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

#List the department names along with their corresponding country names. 
department_join_df = employee_df.join(department_df, employee_df.department == department_df.department_id, "inner")
country_join_df = department_df.join(country_df, department_df.country_name == country_with_date_df.country_code, "inner")
department_with_country = country_df.select('department_name', 'country_name')
display(department_with_country)

# COMMAND ----------

employee_count = employee_df.groupBy("department", "country").agg(count("employee_id").alias("employee_count"))
display(employee_count)


# COMMAND ----------

# What is the average age of employees in each department?
avg_age_by_department = employee_df.groupBy('department').agg(avg("age").alias('avg_age'))
display(avg_age_by_department)


# COMMAND ----------

#Add the at_load_date column to data frames. 


# COMMAND ----------


employee_df.write.format("parquet").mode("overwrite").option("replaceWhere","load_date = '2024-04-16'").save("/FileStore/assignments/assignment_1/gold/employee/table_name")
