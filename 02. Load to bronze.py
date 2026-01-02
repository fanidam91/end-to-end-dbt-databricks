# Databricks notebook source
# MAGIC %md
# MAGIC %md“In Databricks, a checkpoint is a persisted state stored in ADLS that enables Spark streaming and long-running jobs to recover and resume processing from the last successful point without reprocessing data.”
# MAGIC Checkpoint stores metadata about data processing, NOT business data.
# MAGIC
# MAGIC It keeps:
# MAGIC
# MAGIC ✅ Which files are already read
# MAGIC
# MAGIC ✅ Which records / offsets are processed
# MAGIC
# MAGIC ✅ State of aggregations (count, sum, window)
# MAGIC
# MAGIC ✅ Commit information (what was successfully written)
# MAGIC

# COMMAND ----------

checkpoint = spark.sql(" DESCRIBE EXTERNAL LOCATION `checkpoints`").select("url").collect()[0][0]

# COMMAND ----------

print(checkpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 🚚 1. Streaming reader from landing/raw_traffic
# MAGIC python
# MAGIC Copy code
# MAGIC

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

checkpoint = spark.sql(" DESCRIBE EXTERNAL LOCATION `checkpoints`").select("url").collect()[0][0]
landing = spark.sql(" DESCRIBE EXTERNAL LOCATION `landing`").select("url").collect()[0][0]

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import current_timestamp

print("Reading the Raw Traffic Data :  ", end='')

schema = StructType([
    StructField("Record_ID", IntegerType()),
    StructField("Count_point_id", IntegerType()),
    StructField("Direction_of_travel", StringType()),
    StructField("Year", IntegerType()),
    StructField("Count_date", StringType()),
    StructField("hour", IntegerType()),
    StructField("Region_id", IntegerType()),
    StructField("Region_name", StringType()),
    StructField("Local_authority_name", StringType()),
    StructField("Road_name", StringType()),
    StructField("Road_Category_ID", IntegerType()),
    StructField("Start_junction_road_name", StringType()),
    StructField("End_junction_road_name", StringType()),
    StructField("Latitude", DoubleType()),
    StructField("Longitude", DoubleType()),
    StructField("Link_length_km", DoubleType()),
    StructField("Pedal_cycles", IntegerType()),
    StructField("Two_wheeled_motor_vehicles", IntegerType()),
    StructField("Cars_and_taxis", IntegerType()),
    StructField("Buses_and_coaches", IntegerType()),
    StructField("LGV_Type", IntegerType()),
    StructField("HGV_Type", IntegerType()),
    StructField("EV_Car", IntegerType()),
    StructField("EV_Bike", IntegerType())
])


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

rawTraffic_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/rawTrafficLoad/schemaInfer")
        .option("header", "true")
        .schema(schema)
        .load(f"{landing}/raw_traffic/")
        .withColumn("Extract_Time", current_timestamp())
        .withColumn("Source_File", input_file_name())
)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

rawTraffic_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/rawTrafficLoad/schemaInfer")
        .option("header", "true")
        .schema(schema)
        .load(f"{landing}/raw_traffic/")
        .withColumn("Extract_Time", current_timestamp())
        .withColumn("Source_File", col("_metadata.file_path"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating a read_Traffic_Data() Function

# COMMAND ----------

# MAGIC %md
# MAGIC 🚚 1. Streaming reader from landing/raw_traffic

# COMMAND ----------

def read_Traffic_Data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Traffic Data :  ", end='')
    schema = StructType([
    StructField("Record_ID",IntegerType()),
    StructField("Count_point_id",IntegerType()),
    StructField("Direction_of_travel",StringType()),
    StructField("Year",IntegerType()),
    StructField("Count_date",StringType()),
    StructField("hour",IntegerType()),
    StructField("Region_id",IntegerType()),
    StructField("Region_name",StringType()),
    StructField("Local_authority_name",StringType()),
    StructField("Road_name",StringType()),
    StructField("Road_Category_ID",IntegerType()),
    StructField("Start_junction_road_name",StringType()),
    StructField("End_junction_road_name",StringType()),
    StructField("Latitude",DoubleType()),
    StructField("Longitude",DoubleType()),
    StructField("Link_length_km",DoubleType()),
    StructField("Pedal_cycles",IntegerType()),
    StructField("Two_wheeled_motor_vehicles",IntegerType()),
    StructField("Cars_and_taxis",IntegerType()),
    StructField("Buses_and_coaches",IntegerType()),
    StructField("LGV_Type",IntegerType()),
    StructField("HGV_Type",IntegerType()),
    StructField("EV_Car",IntegerType()),
    StructField("EV_Bike",IntegerType())
    ])

    rawTraffic_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoint}/rawTrafficLoad/schemaInfer')
        .option('header','true')
        .schema(schema)
        .load(landing+'/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp()))
    
    print('Reading Succcess !!')
    print('*******************')

    return rawTraffic_stream


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating read_Road_Data() Function

# COMMAND ----------

def read_Road_Data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Roads Data :  ", end='')
    schema = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
        
        ])

    rawRoads_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoint}/rawRoadsLoad/schemaInfer')
        .option('header','true')
        .schema(schema)
        .load(landing+'/raw_roads/')
        )
    
    print('Reading Succcess !!')
    print('*******************')

    return rawRoads_stream


# COMMAND ----------

# MAGIC %md ## Creating write_Traffic_Data(StreamingDF,environment) Function

# COMMAND ----------

environment = "dev"
catalog     = "dev_catalog"


# COMMAND ----------

def write_Traffic_Data(StreamingDF,environment):
    print(f'Writing data to {environment}_catalog raw_traffic table', end='' )
    write_Stream = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",checkpoint + '/rawTrafficLoad/Checkpt')
                    .outputMode('append')
                    .queryName('rawTrafficWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_traffic`"))
    
    write_Stream.awaitTermination()
    print('Write Success')
    print("****************************")   

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating write_Road_Data(StreamingDF,environment) Function

# COMMAND ----------

def write_Road_Data(streaming_df, environment):
    catalog = f"{environment}_catalog"

    print(f"Writing data to {catalog}.bronze.raw_roads table...", end="")

    query = (
        streaming_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"{checkpoint}/rawRoadsLoad/Checkpt")
            .outputMode("append")
            .queryName(f"rawRoadsWriteStream_{environment}")
            .trigger(availableNow=True)
            .toTable(f"`{catalog}`.`bronze`.`raw_roads`")
    )

    query.awaitTermination()
    print(" Write Success")
    print("****************************")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling read and Write Functions

# COMMAND ----------

## Reading the raw_traffic's data from landing to Bronze
read_Df = read_Traffic_Data()

## Reading the raw_roads's data from landing to Bronze
read_roads = read_Road_Data()

## Writing the raw_traffic's data from landing to Bronze
write_Traffic_Data(read_Df,env)

## Writing the raw_roads's data from landing to Bronze
write_Road_Data(read_roads,env)