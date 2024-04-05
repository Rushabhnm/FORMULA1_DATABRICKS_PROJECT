# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Race CSV file

# COMMAND ----------

dbutils.widgets.text("Data_source","")

# COMMAND ----------

v_data_source = dbutils.widgets.get("Data_source")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1databricksdata/raw

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceID",IntegerType(),True ),
                                StructField("year",IntegerType(), False),
                                StructField("round",IntegerType(),False),
                                StructField("circuitID",IntegerType(), False),
                                StructField("name",StringType(),False),
                                StructField("date",DateType(),False),
                                StructField("time",StringType(),False),
                                StructField("url",StringType(),False)
                                ])

# COMMAND ----------

race_df = spark.read.csv(f'{raw_layer}races.csv', header =True, schema=race_schema)

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding columns race_timestamp, ingestion_date and extraction_file

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp, concat, col, current_timestamp, date_format

# COMMAND ----------

race_add_df = add_ingestion_date(race_df)

# COMMAND ----------

race_add_df = race_add_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('extraction_file',lit('Races'))\
    .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(race_add_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Formating the date columns (currently in ISO 8601 ['T','Z'])

# COMMAND ----------

race_add_df = race_add_df.withColumn("race_timestamp",date_format("race_timestamp", "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("ingestion_date",date_format("ingestion_date","yyyy-MM-dd HH:mm:ss"))


# COMMAND ----------

display(race_add_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecting only the required columns and renaming them

# COMMAND ----------

race_final_df = race_add_df.select(col("raceID").alias("race_ID"),col("year").alias("race_year"),col("round"), col("circuitID").alias("circuit_ID"),col("name"),col("race_timestamp"),col("ingestion_date"),col("extraction_file"), col("data_source"))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing the df to the processed layer in parquet format

# COMMAND ----------

race_final_df.write.parquet(f"{processed_layer}races",mode="overwrite",partitionBy="race_year")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying the code written in the processed layer

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1databricksdata/processed/races"))

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")
