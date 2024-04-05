# Databricks notebook source
# MAGIC %md
# MAGIC ## Function to add the ingesiton date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

def add_ingestion_date(dataframe):
    new_dataframe = dataframe.withColumn("ingestion_date", current_timestamp())\

    return new_dataframe
