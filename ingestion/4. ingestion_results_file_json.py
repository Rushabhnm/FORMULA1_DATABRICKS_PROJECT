# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingestion of Results JSON file

# COMMAND ----------

dbutils.widgets.text("Data_source","")
v_data_source = dbutils.widgets.get("Data_source")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1databricksdata/"))

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Making the schema for results table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [StructField("driverId",IntegerType(),False),
                                      StructField("constructorId",IntegerType(),True),
                                      StructField("fastestLap",IntegerType(),False),
                                      StructField("fastestLapSpeed",StringType(),False),
                                      StructField("fastestLapTime",StringType(),False),
                                      StructField("grid", IntegerType(),False),
                                      StructField("laps", IntegerType(),False),
                                      StructField("milliseconds", IntegerType(),False),
                                      StructField("number", IntegerType(),False),
                                      StructField("points", FloatType(),False),
                                      StructField("position", IntegerType(),False),
                                      StructField("positionOrder", IntegerType(),False),
                                      StructField("positionText", StringType(),False),
                                      StructField("raceId", IntegerType(),False),
                                      StructField("rank", IntegerType(),False),
                                      StructField("resultId", IntegerType(),False),
                                      StructField("statusId", IntegerType(),False),
                                      StructField("time", StringType(),False)
])

# COMMAND ----------

results_df=spark.read.json(f"{raw_layer}results.json",schema = results_schema )

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Adding the required columns |

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col

# COMMAND ----------

results_df_add = add_ingestion_date(results_df)

# COMMAND ----------

results_df_add = results_df_add.withColumn("extraction_file",lit("Results"))\
                                .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(spark.read.json(f"{raw_layer}results.json"))

# COMMAND ----------

display(results_df_add)

# COMMAND ----------

results_final_df = results_df_add.select(col("constructorId").alias("constructor_id"),
                                          col("driverId").alias("driver_id"),
                                          col("fastestLap").alias("fastest_lap"),
                                          col("fastestLapSpeed").alias("fastest_lap_speed"),
                                          col("fastestLapTime").alias("fastest_lap_time"),
                                          col("grid"),
                                          col("laps"), 
                                          col("milliseconds"),
                                          col("number"),
                                          col("points"),
                                          col("position"),
                                          col("positionOrder").alias("position_order"), 
                                          col("positionText").alias("position_text"),
                                          col("raceId").alias("race_id"), 
                                          col("rank"), 
                                          col("resultId").alias("result_id"),
                                          col("time"),
                                          col("ingestion_date"),
                                          col("extraction_file"),
                                          col("data_source"))

# COMMAND ----------

results_final_df.write.parquet(f"{processed_layer}results",mode="overwrite",partitionBy = "race_id")

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")
