# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting messages from Azure IoT Hub Using the Event Hubs Connector
# MAGIC Using Databricks you can easily build highly scalable and reliable IoT data pipelines that power your ML models and data analytics.
# MAGIC 
# MAGIC There are several options - in this notebook we'll demonstrate how to use Microsoft's **Event Hubs streaming connector** with Python to pull data from the built-in Event Hubs-compatible endpoint on **Azure's IoT Hub** service. If you configure routing between IoT Hub and Azure Event Hubs you can pull messages from Event Hubs, but this is not required. [TODO-we could seed a Forum topic where customers can discuss pros/cons?] Also, in the companion notebook iot-e2e-kafka-ingest we'll use the Databricks Kafka steaming connector instead of the Event Hubs connector.
# MAGIC 
# MAGIC The purpose of this notebook is to familiarize you with:
# MAGIC * Generating simulated IoT messages using a Raspberry Pi node.js app
# MAGIC * Configuring Azure IoT Hub to capture messages from IoT devices
# MAGIC * Configuring Databricks to subscribe to an IoT message stream
# MAGIC * What IoT messages look like when using the Microsoft Event Hubs streaming connector.
# MAGIC 
# MAGIC This notebook is for illustrative purposes only. For production IoT ingestion we recommend using a continuous Delta Live Tables pipeline as shown in the companion notebook iot-e2e-dlt-pipeline. Let's get started!

# COMMAND ----------

# MAGIC %md
# MAGIC ###Configuration

# COMMAND ----------

# DBTITLE 1,Step 1 - Create an IoT Hub instance and a Device


# COMMAND ----------

# DBTITLE 1,Step 2 - Configure a Raspberry Pi simulator instance to send IoT messages
# MAGIC %md 
# MAGIC Make note of the **primary connection string** from the IoT Hub Device you created in Step 1 above (replace value below with yours)
# MAGIC * HostName=fe-shared-sa-mfg-iothub.azure-devices.net;DeviceId=my-device;SharedAccessKey=JWvFLnBri7qvKyj9fCWuJa1XjJ4+IrdXkZ61GutivMQ=
# MAGIC 
# MAGIC Next go to the following URL to open an instance of the Raspberry Pi simulator:
# MAGIC * https://azure-samples.github.io/raspberry-pi-web-simulator/#getstarted
# MAGIC 
# MAGIC Replace all of the code on the right side of the simulator screen with the code found in this Git Hub file. Copy/paste may bring in extra characters we don't want at the bottom. Be sure your simulator code has :
# MAGIC * https://github.com/tomatoTomahto/azure_databricks_iot/blob/master/azure_iot_simulator.js
# MAGIC 
# MAGIC Copy to line 15
# MAGIC 
# MAGIC then click Run

# COMMAND ----------

# DBTITLE 1,Step 3 - Create a Cluster that loads the Event Hubs connector


# COMMAND ----------

# DBTITLE 1,Step 4 - Configure your Event Hubs streaming connector

# change to your IoT Hub values
iot_hub_name = "fe-shared-sa-mfg-iot-hub"
iot_hub_consumer_group = "iot-e2e"
iot_hub_cs = "Endpoint=sb://iothub-ns-fe-shared-23133643-2e6a130cca.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=8q4Md67G3wl67oqGqTfv0RpAJlD1cKiZXFlSsSTtng0=;EntityPath=fe-shared-sa-mfg-iot-hub"

ehConf = { 
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(iot_hub_cs),
  'ehName': iot_hub_name,
  'eventhubs.consumerGroup': "iot-e2e"
}

# Pyspark and ML Imports DOUBLE CHECK NOT SURE ALL THIS IS NEEDED?
import os, json, requests
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

# MAGIC %md
# MAGIC ###Testing

# COMMAND ----------

# DBTITLE 1,Step 5 - Start your IoT stream
# For testing, read directly from IoT Hub using the EventHubs library for Databricks
iot_stream_test = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load() 
                                                                     # Load the data
)
display(iot_stream_test)


# COMMAND ----------

# DBTITLE 1,Casting the body field to string removes base64 encoding
iot_stream_decoded=iot_stream_test.withColumn("body",iot_stream_test["body"].cast("string"))        
display(iot_stream_decoded)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Let's Inspect the message body and properties

# COMMAND ----------

# MAGIC %sql
# MAGIC --create catalog dev;
# MAGIC --use catalog dev;
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC current_catalog()

# COMMAND ----------

storage_account = "fesharedsamfg"
storage_container = "iot-e2e-root"
adls_key = "HSqhnTJNvgWMz9yASkWW3yCymibTLLipEH0tW7kBsHfdpL+VNk+juj0xQjhPfmxS2BrsANum86kY+AStukDN6w=="
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", adls_key)

# Setup storage locations for all data
ROOT_PATH = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/"
BRONZE_PATH = ROOT_PATH + "bronze/"
#SILVER_PATH = ROOT_PATH + "silver/"
#GOLD_PATH = ROOT_PATH + "gold/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")

# Pyspark and ML Imports DOUBLE CHECK NOT SURE ALL THIS IS NEEDED?
import os, json, requests
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

# Schema of incoming data from IoT hub
schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"

# Read directly from IoT Hub using the EventHubs library for Databricks
iot_stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load()                                                                          # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))        # Extract the "body" payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
)

# Split our IoT Hub stream into separate streams and write them both into their own Delta locations
write_turbine_to_delta = (
  iot_stream.filter('temperature is null')                                           # Filter out turbine telemetry from other data streams
    .select('date','timestamp','deviceId','rpm','angle')                             # Extract the fields of interest
    .writeStream.format('delta')                                                     # Write our stream to the Delta format
    .partitionBy('date')                                                             # Partition our data by Date for performance
    .option("checkpointLocation", CHECKPOINT_PATH + "turbine_raw")                   # Checkpoint so we can restart streams gracefully
    .start(BRONZE_PATH + "turbine_raw")                                              # Stream the data into an ADLS Path
)

write_weather_to_delta = (
  iot_stream.filter(iot_stream.temperature.isNotNull())                              # Filter out weather telemetry only
    .select('date','deviceid','timestamp','temperature','humidity','windspeed','winddirection') 
    .writeStream.format('delta')                                                     # Write our stream to the Delta format
    .partitionBy('date')                                                             # Partition our data by Date for performance
    .option("checkpointLocation", CHECKPOINT_PATH + "weather_raw")                   # Checkpoint so we can restart streams gracefully
    .start(BRONZE_PATH + "weather_raw")                                              # Stream the data into an ADLS Path
)

# Create the external tables once data starts to stream in
while True:
  try:
    spark.sql(f'CREATE TABLE IF NOT EXISTS turbine_raw USING DELTA LOCATION "{BRONZE_PATH + "turbine_raw"}"')
    spark.sql(f'CREATE TABLE IF NOT EXISTS weather_raw USING DELTA LOCATION "{BRONZE_PATH + "weather_raw"}"')
    break
  except:
    pass

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from turbine_raw; 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_raw;

# COMMAND ----------

display(spark.readStream.format("delta").table("turbine_raw").groupBy("deviceid").count().orderBy("deviceid"))

# COMMAND ----------

# DBTITLE 1,TODO
# we can try to calculate a window value and group by it to show data coming in each 10 seconds; maybe color each of the devices?
# display(spark.readStream.format("delta").table("turbine_raw").groupBy("type", window("timestamp", "10 seconds")).count().orderBy("window"))

# COMMAND ----------

# DBTITLE 1,Utilities
# Make sure root path is empty
dbutils.fs.rm(ROOT_PATH, True)

# COMMAND ----------



# COMMAND ----------


