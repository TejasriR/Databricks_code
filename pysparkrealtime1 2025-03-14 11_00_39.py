# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Load in Databricks
# MAGIC # Source files added in github repo

# Reading CSV file from DBFS

df=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/FileStore/rawsourcedata/sales_data_first.csv")
df.display()

# Using Autoloader for continuous processing of files from DBFS.
# MAGIC %md
# MAGIC # Autoloader

# Streaming Data Ingestion Using Auto Loader
#The given PySpark Auto Loader code is used for continuous ingestion of CSV files from DBFS (Databricks File System) into a structured streaming DataFrame.

df= spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemalocation","dbfs:/FileStore/rawdestination/checkpoint").load("dbfs:/FileStore/rawsourcedata/")

# This PySpark Structured Streaming code writes data to a Delta Lake table on DBFS (Databricks FileStore) using triggered processing.

df.writeStream.format("delta").option("checkpointlocation","dbfs:/FileStore/rawdestination/checkpoint").trigger(processingTime= " 3 seconds").start("dbfs:/FileStore/rawdestination/data")

# Data viewing in sql command

%sql
select * from delta. `dbfs:/FileStore/rawdestination/data`



# MAGIC %md
# MAGIC ## # You have a pipeline that frequently ingests data with a 'new schema', and you need to process it as soon as it arrives(Use mergeSchema)
##The mergeSchema option in Delta Lake is used when writing data to a Delta table to automatically update the schema if new columns appear in the incoming data.

df.writeStream.format("delta").option("checkpointlocation","dbfs:/FileStore/rawdestination/checkpoint").trigger(processingTime= " 3 seconds").option("mergeSchema",True).start("dbfs:/FileStore/rawdestination/data")




