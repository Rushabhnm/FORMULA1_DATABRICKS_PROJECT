# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_layer}drivers")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_layer}constructors")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_layer}results").orderBy("race_id", ascending = False)

# COMMAND ----------

display(results_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_layer}races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

pitstop_df = spark.read.parquet(f"{processed_layer}pit_stops").orderBy("race_id","driver_id")
display(pitstop_df)

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

pitstop_group = pitstop_df.groupBy("race_id","driver_id").agg(count("stop").alias("stops"))

# COMMAND ----------

agg_df = results_df.join(races_df,results_df.race_id == races_df.race_ID, "inner")\
                .join(constructors_df,"constructor_id","inner")\
                .join(drivers_df,"driver_id","inner")\
                .join(pitstop_group,["race_id","driver_id"],"inner")\
                .select(races_df.race_year.alias("Race Year"),races_df.name.alias("Circuit Name"),drivers_df.name.alias("Driver"), drivers_df.number.alias("Number") ,constructors_df.name.alias("Team"), results_df.grid.alias("Grid"), pitstop_group.stops.alias("Pits"), results_df.fastest_lap_time.alias("Fastest Lap"),results_df.time.alias("Race Time"),results_df.points.alias("Points") )                

# COMMAND ----------

agg_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning the df by circuit name and ranking the winner in every circuit 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# COMMAND ----------

window = Window.partitionBy("Race Year","Circuit Name").orderBy(agg_df["Points"].desc())

# COMMAND ----------

rankings = agg_df.withColumn("rank",row_number().over(window))

# COMMAND ----------

final_df = rankings.orderBy("Race Year")

# COMMAND ----------

display(final_df.sort("Race Year", ascending = False))

# COMMAND ----------

final_df.write.parquet(f"{presentation_layer}Race Results", mode = "overwrite", partitionBy = "Race Year")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_layer}/Race Results/Race Year=2020/"))