# Databricks notebook source
dbutils.widgets.text("Data_source","")
v_data_source = dbutils.widgets.get("Data_source")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1databricksdata/"))

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(spark.read.json(f"{raw_layer}drivers.json"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, DateType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Schema development

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname", StringType(),True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId",IntegerType(),False),
                                     StructField("driverRef",StringType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("code",StringType(),True),
                                     StructField("name",name_schema),
                                     StructField("dob",DateType(),True),
                                     StructField("nationality",StringType(),True),
                                     StructField("url",StringType(),True)
])

# COMMAND ----------

driver_df = spark.read.json(f"{raw_layer}drivers.json", schema = drivers_schema)

# COMMAND ----------

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, concat, col

# COMMAND ----------

driver_df_add = add_ingestion_date(driver_df)

# COMMAND ----------

driver_df_add = driver_df_add.withColumn("name",concat("name.forename",lit(' '),"name.surname"))\
                        .withColumn("extraction_file",lit("Drivers"))\
                        .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

driver_final_df= driver_df_add.select(col("driverId").alias("driver_id"),col("driverRef").alias("driver_ref"),col("number"),col("code"),col("name"),col("dob"),col("nationality"),col("ingestion_date"),col("extraction_file"),col("data_source"))

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

driver_final_df.write.parquet(f'{processed_layer}drivers',mode="overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{processed_layer}drivers"))

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")