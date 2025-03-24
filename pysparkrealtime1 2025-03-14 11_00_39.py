# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Load in Databricks
# MAGIC # Source files added in github repo

# COMMAND ----------

df=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/FileStore/rawsourcedata/sales_data_first.csv")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Autoloader

# COMMAND ----------

df= spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemalocation","dbfs:/FileStore/rawdestination/checkpoint").load("dbfs:/FileStore/rawsourcedata/")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## # Third file having different schema

# COMMAND ----------

df.writeStream.format("delta").option("checkpointlocation","dbfs:/FileStore/rawdestination/checkpoint").trigger(processingTime= " 3 seconds").start("dbfs:/FileStore/rawdestination/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta. `dbfs:/FileStore/rawdestination/data`

# COMMAND ----------

# MAGIC %md
# MAGIC ## # You have a pipeline that frequently ingests data with a 'new schema', and you need to process it as soon as it arrives(Use mergeSchema)

# COMMAND ----------

df.writeStream.format("delta").option("checkpointlocation","dbfs:/FileStore/rawdestination/checkpoint").trigger(processingTime= " 3 seconds").option("mergeSchema",True).start("dbfs:/FileStore/rawdestination/data")

# COMMAND ----------


