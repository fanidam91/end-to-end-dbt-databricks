# Databricks notebook source
# MAGIC %run "/Users/sekharsasi755@gmail.com/04. Common"

# COMMAND ----------

spark.sql("USE CATALOG dev_catalog")


# COMMAND ----------

bronze_path = spark.sql("""DESCRIBE EXTERNAL LOCATION `bronze`""").select("url").collect()[0][0]
silver_path = spark.sql("""DESCRIBE EXTERNAL LOCATION `silver`""").select("url").collect()[0][0]
gold_path  = spark.sql("""DESCRIBE EXTERNAL LOCATION `gold`""").select("url").collect()[0][0]


# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS Dev_catalog.silver
MANAGED LOCATION '{silver_path}/silver';
""")

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS Dev_catalog.gold
MANAGED LOCATION '{gold_path}/gold';
""")

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS Dev_catalog.bronze
MANAGED LOCATION '{bronze_path}/bronze';
""")


# COMMAND ----------

spark.sql("https://www.databricks.com/blog/pdfs-production-announcing-state-art-document-intelligence-databricks" use )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_catalog.silver
# MAGIC MANAGED LOCATION 'abfss://silver@datahubstoresf.dfs.core.windows.net/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_catalog.gold
# MAGIC MANAGED LOCATION 'abfss://gold@datahubstoresf.dfs.core.windows.net/';

# COMMAND ----------

def create_Bronze_Schema(catalog_name, path):
    print(f"Using catalog: {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")

    print(f"Creating Bronze schema in {catalog_name}")
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze
        MANAGED LOCATION '{path}';
    """)

    print("************************************")



# COMMAND ----------

def create_Silver_Schema(catalog_name, path):
    print(f"Using catalog: {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")

    print(f"Creating Silver schema in {catalog_name}")
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver
        MANAGED LOCATION '{path}';
    """)

    print("************************************")



# COMMAND ----------

def create_Gold_Schema(catalog_name, path):
    print(f"Using catalog: {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")

    print(f"Creating Gold schema in {catalog_name}")
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold
        MANAGED LOCATION '{path}';
    """)

    print("************************************")




# COMMAND ----------

create_Bronze_Schema ('dev_catalog',bronze_path)

# COMMAND ----------

create_Silver_Schema('dev_catalog',silver_path)

# COMMAND ----------

create_Gold_Schema('dev_catalog',gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating raw_traffic table

# COMMAND ----------

def createTable_rawTraffic(catalog_name):
    print(f"Creating raw_traffic table in {catalog_name}.bronze")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.raw_traffic
        (
            Record_ID INT,
            Count_point_id INT,
            Direction_of_travel STRING,
            Year INT,
            Count_date STRING,
            hour INT,
            Region_id INT,
            Region_name STRING,
            Local_authority_name STRING,
            Road_name STRING,
            Road_Category_ID INT,
            Start_junction_road_name STRING,
            End_junction_road_name STRING,
            Latitude DOUBLE,
            Longitude DOUBLE,
            Link_length_km DOUBLE,
            Pedal_cycles INT,
            Two_wheeled_motor_vehicles INT,
            Cars_and_taxis INT,
            Buses_and_coaches INT,
            LGV_Type INT,
            HGV_Type INT,
            EV_Car INT,
            EV_Bike INT,
            Extract_Time TIMESTAMP
        );
    """)

    print("************************************")



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating raw_roads Table

# COMMAND ----------

def createTable_rawRoad(catalog_name):
    print(f"Creating raw_roads table in {catalog_name}.bronze")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.raw_roads
        (
            Road_ID INT,
            Road_name STRING,
            Region_id INT,
            Region_name STRING,
            Local_authority_name STRING,
            Link_length_km DOUBLE,
            Link_length_miles DOUBLE,
            All_Motor_Vehicles DOUBLE
        );
    """)

    print("************************************")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling all functions

# COMMAND ----------

# Set environment name
env = "dev_catalog"

# Create schemas & tables
create_Bronze_Schema(env, bronze_path)
createTable_rawTraffic(env)
createTable_rawRoad(env)
create_Silver_Schema(env, silver_path)
create_Gold_Schema(env, gold_path)
