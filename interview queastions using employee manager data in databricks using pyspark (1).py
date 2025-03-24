# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Interview_questions").getOrCreate()

# COMMAND ----------

data = [
    (101, "Mary", "Danner", "Account", 109, 80000, "Junior", "2024-01-03"),
    (102, "Ann", "Lynn", "Sales", 107, 140000, "Semisenior", "2024-01-03"),
    (103, "Peter", "Oconnor", "IT", 110, 130000, "Senior", "2025-02-04"),
    (106, "Sue", "Sanchez", "Sales", 107, 110000, "Junior", "2025-01-01"),
    (107, "Marta", "Doe", "Sales", 110, 180000, "Senior", "2025-02-04"),
    (109, "Ann", "Danner", "Account", 110, 90000, "Senior", "2025-01-01"),
    (110, "Simon", "Yang", "CEO", None, 250000, "Senior", "2025-01-01"),
    (111, "Juan", "Graue", "Sales", 102, 37000, "Junior", "2025-01-01")
]
schema =["employee_id","first_name","last_name","dept_id","manager_id","salary","expertise","date"]

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Retrieve the employees who report to the manager with ID 107 ?

# COMMAND ----------

final_df = df.alias("e").join(df.alias("m"),col("e.manager_id")==col("m.employee_id"),"left").select("e.employee_id","e.manager_id","e.first_name").filter(col("e.manager_id") == 107)

# COMMAND ----------

final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ## # Employees who are earning more than their manager's salary ?

# COMMAND ----------

final_df1 = df.alias("e").join(df.alias("m"),col("e.manager_id")==col("m.employee_id"),"left").select("e.employee_id","e.manager_id","e.first_name",col("e.salary") .alias("employee_salary"),col("m.salary").alias("manager_salary")).filter(col("employee_salary") < col("manager_salary")).select("employee_id","first_name")

# COMMAND ----------

final_df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Write a PySpark query to find all employees who do not have a manager?

# COMMAND ----------

employee_not_assign_toanymanager = df.alias("e").join(df.alias("m"),col("e.manager_id")==col("m.employee_id"),"left").select("e.employee_id","e.manager_id","e.first_name").filter(col("manager_id").isNull())

# COMMAND ----------

employee_not_assign_toanymanager.display()
