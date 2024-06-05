# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the file using spark read configuration

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("Data_source","")

# COMMAND ----------

v_data_source = dbutils.widgets.get("Data_source")

# COMMAND ----------

# Checking the mounted adls and getting the path

display(dbutils.fs.ls('/mnt/formula1databricksdata/'))

# COMMAND ----------

# Read the data using the reader method API of spark 
# we are not infering the schema because of which all the datatypes would be shown as string and post execution we can see only 1 spark job getting executed



# circuits_df = spark.read.csv('/mnt/formula1databricksdata/raw/circuits.csv', header = True)

# COMMAND ----------

# Read the data using the reader method API of spark
# we are infering the schema here, so spark understand what datatype does each column hold. Here spark is reading through the dataframe and then applying the respected datatype to each column
# this solution takes up 2 spark job (not efficient)


# circuits_df = spark.read.csv('/mnt/formula1databricksdata/raw/circuits.csv', header = True, inferSchema= True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Incorporating other notebook to this using the %run option and then specifing the path 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitid", IntegerType(), False),
                                        StructField("circuitRef", StringType(), True),
                                        StructField("name",StringType(), True),
                                        StructField("location",StringType(), True),
                                        StructField("country",StringType(), True),
                                        StructField("lat",DoubleType(), True),
                                        StructField("lng",DoubleType(), True),
                                        StructField("alt",IntegerType(), True),
                                        StructField("url",StringType(), True)                                        
])

# COMMAND ----------


# Now instead of making spark read through the data and then determine the datatype of each column using the inferschema, we will be using our own predefined schema for the table (which is defined under circuits_schema variable) and then use it while reading the data
circuits_df = spark.read.csv(f'{raw_layer}circuits.csv', header = True, schema = circuits_schema)

# COMMAND ----------

# Display the data
display(circuits_df)

# COMMAND ----------

# Other actions on the df
circuits_df.dtypes

# COMMAND ----------

# prinitng the schema

circuits_df.printSchema()

# COMMAND ----------

# as the above printschema showed all the columns are strings which is not true, so we will describe the df to see several parameters

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Selecting only the required column

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Ways to write the select statement :

# COMMAND ----------

# METHOD 1
circuits_select_df = circuits_df.select("circuitid","circuitRef","name","location","country","lat","lng","alt"
)
display(circuits_select_df)

# COMMAND ----------

# METHOD 2
circuits_select_df = circuits_df.select(circuits_df.circuitid,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt
)
display(circuits_select_df)

# COMMAND ----------

# METHOD 3
circuits_select_df = circuits_df.select(circuits_df['circuitid'],circuits_df['circuitRef'],circuits_df['name'],circuits_df['location'],circuits_df['country'],circuits_df['lat'],circuits_df['lng'],circuits_df['alt'])

display(circuits_select_df)

# COMMAND ----------

#    METHOD 4
from pyspark.sql.functions import col

# COMMAND ----------

circuits_select_df = circuits_df.select(col("circuitid").alias("circuit_id"),col("circuitRef").alias("circuit_ref"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

display(circuits_select_df)

# COMMAND ----------

# another way to rename the columns

circuits_renamed_df = circuits_select_df.withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding a new column "ingestion_date" with the ingested date timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df = circuits_final_df.withColumn("extraction_file",lit("Circuits"))\
                                     .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing the data in parquet to the processed layer

# COMMAND ----------

circuits_final_df.write.parquet(f"{processed_layer}circuits", mode = "overwrite")

# COMMAND ----------

circuits_final_df.write.format("delta").save(f"{delta_tables}circuits")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying the written pqrquet file

# COMMAND ----------

display(spark.read.parquet(f"{processed_layer}circuits"))

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")