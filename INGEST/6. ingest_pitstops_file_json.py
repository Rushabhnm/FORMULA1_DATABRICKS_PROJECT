# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingesting pitstop multi line JSON file

# COMMAND ----------

dbutils.widgets.text("Data_source","")
v_data_source = dbutils.widgets.get("Data_source")

# COMMAND ----------

display(dbutils.fs.ls("mnt/formula1databricksdata/"))

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(spark.read.option('multiline',True).json(f"{raw_layer}pit_stops.json"))

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType,IntegerType, LongType

# COMMAND ----------

pitstop_schema = StructType(fields = [StructField("driverId",IntegerType(),False),
                                      StructField("duration",StringType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("milliseconds",IntegerType(),True),
                                      StructField("raceId",IntegerType(),True),
                                      StructField("stop",IntegerType(),True),
                                      StructField("time",StringType(),True)
                                      ])

# COMMAND ----------

pitstop_df = spark.read.option('multiline',True).json(f"{raw_layer}pit_stops.json", schema = pitstop_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Ingestion_date, Extraction_file columns

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp,col

# COMMAND ----------

pitstop_df_add = add_ingestion_date(pitstop_df)

# COMMAND ----------

pitstop_df_add = pitstop_df_add.withColumn("extraction_file", lit("Pit Stop"))\
                                .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

pit_stop_final = pitstop_df_add.select(col("driverId").alias("driver_id"),col("duration"),col("lap"),col("milliseconds"),col("stop"),col("raceId").alias("race_id"),col("time"),col("ingestion_date"),col("extraction_file"),col("data_source"))

# COMMAND ----------

display(pit_stop_final)

# COMMAND ----------

pit_stop_final.write.parquet(f"{processed_layer}pit_stops",mode="overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{processed_layer}pit_stops"))

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")