# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingesting qualifying multi line JSON folder

# COMMAND ----------

dbutils.widgets.text("Data_source","")
v_data_source = dbutils.widgets.get("Data_source")

# COMMAND ----------

display(dbutils.fs.ls("mnt/formula1databricksdata/raw/qualifying"))

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(spark.read.option('multiline',True).json(f"{raw_layer}qualifying/qualifying_split_*.json"))

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType,IntegerType, LongType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("constructorId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),False),
                                      StructField("number",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("q1",StringType(),True),
                                      StructField("q2",StringType(),True),
                                      StructField("q3",StringType(),True),
                                      StructField("qualifyId",IntegerType(),True),
                                      StructField("raceId",IntegerType(),True)
                                      ])

# COMMAND ----------

qualifying_df = spark.read.option('multiline',True).json(f"{raw_layer}qualifying/qualifying_split_*.json", schema = qualifying_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Ingestion_date, Extraction_file columns

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp,col

# COMMAND ----------

qualifying_df_add =add_ingestion_date(qualifying_df)

# COMMAND ----------

qualifying_df_add = qualifying_df_add.withColumn("extraction_file", lit("Qualifying"))\
                                        .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

qualifying_final = qualifying_df_add.select(col("constructorId").alias("constructor_id"),col("driverId").alias("driver_id"),col("number"),col("position"),col("q2"),col("q1"),col("q3"),col("qualifyId").alias("qualify_id"),col("raceId").alias("race_id"),col("ingestion_date"),col("extraction_file"),col("data_source"))

# COMMAND ----------

display(qualifying_final)

# COMMAND ----------

qualifying_final.write.parquet(f"{processed_layer}qualifying/",mode="overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{processed_layer}qualifying"))

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")
