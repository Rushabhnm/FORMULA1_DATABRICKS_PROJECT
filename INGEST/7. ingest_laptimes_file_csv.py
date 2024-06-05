# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingesting Lap Times csv, multiple files within a folder

# COMMAND ----------

dbutils.widgets.text("Data_source","")
v_data_source = dbutils.widgets.get("Data_source")

# COMMAND ----------

display(dbutils.fs.ls("mnt/formula1databricksdata/raw/lap_times/"))

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(spark.read.csv(f"{raw_layer}lap_times/lap_times_split_*.csv"))

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType,IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId",IntegerType(),True),
                                      StructField("driverId",IntegerType(),False),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
                                      ])

# COMMAND ----------

lap_times_df = spark.read.csv(f"{raw_layer}lap_times/lap_times_split_*.csv", schema = lap_times_schema)

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Ingestion_date, Extraction_file columns

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp,col

# COMMAND ----------

lap_times_df_add = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_df_add = lap_times_df_add.withColumn("extraction_file", lit("Lap Times"))\
                                    .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

lap_times_df_final = lap_times_df_add.select(col("raceId").alias("race_id"),col("driverId").alias("driver_id"),col("lap"),col("position"),col("time"),col("milliseconds"),col("ingestion_date"),col("extraction_file"),col("data_source"))

# COMMAND ----------

display(lap_times_df_final)

# COMMAND ----------

lap_times_df_final.write.parquet(f"{processed_layer}ap_times",mode="overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{processed_layer}lap_times"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random

# COMMAND ----------

lap_times_df_final.count()

# COMMAND ----------

dummy1= spark.read.parquet("/mnt/formula1databricksdata/processed/lap_times/")

# COMMAND ----------

display(dummy1.count())

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")