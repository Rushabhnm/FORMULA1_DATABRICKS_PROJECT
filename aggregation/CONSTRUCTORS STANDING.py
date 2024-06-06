# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_layer}constructors")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_layer}results")

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_layer}races")

# COMMAND ----------

display(race_df)

# COMMAND ----------

agg_df = results_df.join(constructor_df, "constructor_id", "inner" )\
                   .join(race_df, results_df.race_id==race_df.race_ID, "inner")\
                .select(constructor_df.constructor_ref,results_df.position,results_df.points, race_df.race_year, race_df.race_ID)
   


# COMMAND ----------

agg_df.createOrReplaceTempView("agg_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from agg_table
# MAGIC order by race_year desc, race_id desc, position asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select constructor_ref, sum(points) total_points, race_year
# MAGIC from agg_table
# MAGIC group by race_year, constructor_ref
# MAGIC order by race_year desc, total_points desc

# COMMAND ----------

display(agg_df.orderBy("race_year","race_ID", "points", ascending = False))

# COMMAND ----------

display(agg_df)

# COMMAND ----------

agg_df.dtypes

# COMMAND ----------

from pyspark.sql.functions import sum, count, col, when

# COMMAND ----------

results_df = agg_df.groupBy("race_year","constructor_ref")\
                .agg(sum("points").alias("total_points"),
                    count(when(col("position") == 1, True).alias("Total Wins")))

# COMMAND ----------

results_df = results_df.orderBy(col("race_year").desc(),col("total_points").desc())

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.write.parquet(f"{presentation_layer}Constructors Standing", partitionBy="race_year")