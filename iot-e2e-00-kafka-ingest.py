# Databricks notebook source
# MAGIC %md
# MAGIC ## Exploring Near Real-Time Ingestion from Azure IoT Hub with the Databricks Kafka Connector
# MAGIC Using Databricks you can easily build highly scalable and reliable near real-time IoT data pipelines to power your ML models and data analytics.
# MAGIC 
# MAGIC In this interactive notebook we'll show how to use the Databricks **Kafka streaming connector** with Python to pull data from the built-in Event Hubs-compatible endpoint on the **Azure IoT Hub** service. 
# MAGIC 
# MAGIC <img style="float:right" src="https://raw.githubusercontent.com/JimThorstad/iot-e2e-demo/images/iot-e2e-kafka-dlt.png"/>
# MAGIC 
# MAGIC * If you configure a *route* between IoT Hub and Azure Event Hubs you can pull messages from Event Hubs, but Databricks does not require this additional configuration. 
# MAGIC * You can also use the Microsoft *eventhubs streaming connector* in place of Kafka with either IoT Hubs or Event Hubs. To see how this is configured refer to the companion notebook `iot-e2e-00-eventhubs-ingest`.
# MAGIC 
# MAGIC The purpose of this exploratory notebook is to familiarize you with:
# MAGIC * Generating simulated IoT messages using a Raspberry Pi node.js app.
# MAGIC * Configuring Azure IoT Hub to capture messages from IoT devices.
# MAGIC * Configuring Databricks to subscribe to an IoT message stream using Kafka.
# MAGIC * What IoT messages look like when using the Kafka streaming connector.
# MAGIC * Special processing needed to read IoT Hub message headers with Kafka.
# MAGIC * Two common use cases for processing IoT device message data.
# MAGIC 
# MAGIC This is an interactive exploratory notebook, designed to be run cell by cell. For production IoT processing we recommend using a continuous Delta Live Tables pipeline (refer to the companion notebook iot-e2e-01-dlt-pipeline). 
# MAGIC 
# MAGIC Let's get started!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration
# MAGIC The Kafka connector is included in the Databricks Runtime distribution, so no special cluster setup is necessary. We are not persisting data so you won't need any storate credentials. 
# MAGIC 
# MAGIC You'll start by creating an IoT Hub instance and device in the Azure Portal. Then you'll configure an IoT message simulator and configure your Kafka connector. 

# COMMAND ----------

# DBTITLE 1,Step 1 - Create an IoT Hub instance and a Device
# MAGIC %md
# MAGIC TODO - show screens and the steps; possibly link to external doc.

# COMMAND ----------

# DBTITLE 1,Step 2 - Configure a Raspberry Pi simulator instance to send IoT messages
# MAGIC %md 
# MAGIC Make note of the **primary connection string** from the IoT Hub Device you created in Step 1 above (replace value below with yours)
# MAGIC * HostName=fe-shared-sa-mfg-iothub.azure-devices.net;DeviceId=my-device;SharedAccessKey=REDACTED
# MAGIC 
# MAGIC Next go to the following URL to open an instance of the Raspberry Pi simulator:
# MAGIC * https://azure-samples.github.io/raspberry-pi-web-simulator/#getstarted
# MAGIC 
# MAGIC Replace all of the code on the right side of the simulator screen with the code found in this Git Hub file. Copy/paste may bring in extra characters we don't want at the bottom. Be sure your simulator code has :
# MAGIC * https://github.com/tomatoTomahto/azure_databricks_iot/blob/master/azure_iot_simulator.js
# MAGIC 
# MAGIC Copy the Device primary connection string to line 15
# MAGIC 
# MAGIC then click Run

# COMMAND ----------

# DBTITLE 1,Step 3 - Attach a Cluster to this notebook
# MAGIC %md
# MAGIC You can use any Databricks cluster for this notebook and no special libraries or security settings are required.

# COMMAND ----------

# DBTITLE 1,Step 4 - Configure your Kafka streaming connector

# change everything below to match your values; refer back to the information in Step 1 if necessary 

iot_hub_name = "fe-shared-sa-mfg-iothub"

# can we simplify these settings since they reference each other
iot_hub_cs = "Endpoint=sb://iothub-ns-fe-shared-23360218-6d2168fc54.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=EV++zrzdhsgoOlncfnk/qwP1DQNmM1wHoloHfxvqZac=;EntityPath=fe-shared-sa-mfg-iothub"
eh_sasl = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='Endpoint=sb://iothub-ns-fe-shared-23360218-6d2168fc54.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=EV++zrzdhsgoOlncfnk/qwP1DQNmM1wHoloHfxvqZac=';"
service_bus_host = "iothub-ns-fe-shared-23360218-6d2168fc54.servicebus.windows.net"   # find this inside the eh-compatible endpoint name
bootstrap_servers = f'{service_bus_host}:9093'

# don't edit the config
kafka_config = {
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": eh_sasl,
    "kafka.bootstrap.servers": bootstrap_servers,
    "subscribe": iot_hub_name,
    #"kafka.group.id": "...",  #Don't specify this for Kafka; each Kafka stream generates a dynamic offset from the source automatically
    "includeHeaders": True
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing
# MAGIC The cells that follow are designed to be run one by one, in order. However each cell is complete and doesn't rely on anything that runs before it other than the Kafka setup code in Step 4. 

# COMMAND ----------

# DBTITLE 1,Step 5 - Run this cell to start your IoT stream
# let's first see what the stream looks like
df = (spark.readStream.format("kafka")
      .options(**kafka_config)
      .load()
     )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Decoding the message "value"
# MAGIC Notice that the data in the `value` column above is base64 encoded.
# MAGIC * You can confirm this by copying the string to a page like https://www.base64decode.org/ and checking it for yourself.
# MAGIC 
# MAGIC In the next cell we'll decode it after the `.load()` method by casting the column to a string.
# MAGIC * **Cancel the stream** in your previous cell. 
# MAGIC * Run the next cell; you may need to wait a few seconds before new messages to arrive. 

# COMMAND ----------

# DBTITLE 1,Let's cast the encoded column to a string to reverse the base64 encoding
from pyspark.sql import functions as F

df = (spark.readStream.format("kafka")
      .options(**kafka_config)
      #.option("decode","utf"). #checking into what this does as it doesn't seem to make any difference
      .load()
      .withColumn("value",F.col("value").cast("string"))
     )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading the iothub-enquedtime Header
# MAGIC The data in the `headers` column above contains an array of message headers added by IoT Hub. These may be useful to keep.
# MAGIC * Notice that the `iothub-enquedtime` header value is encoded -- e.g., "gwAAAYUJlV0U".
# MAGIC * This value is encoded by IoT Hub with the AMQP protocol, but we can decode this with a Python UDF.
# MAGIC 
# MAGIC **Cancel the stream** in your previous cell and continue.

# COMMAND ----------

# DBTITLE 1,Create a new column for the decoded iothub-enqueuedtime header value
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime
import struct

# Define a UDF to decode the IoT Hub header
def decode_enqueuedtime(unbase64_value):
    ts = struct.unpack('>Q', unbase64_value[1:9])[0]
    dt = datetime.datetime.fromtimestamp(ts//1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Register the UDF with our spark session
decode_enqueuedtime_udf = spark.udf.register("decode_enqueuedtime_sql",decode_enqueuedtime)    

# TODO - using column index notation to find the enqued time header is brittle because it may not always appear
#        in column 3 as in my case below. Check your dataframe above to see which column index its in for you.  
df = (spark.readStream.format("kafka").options(**kafka_config)
      #.option("decode","utf")
      .load()
      .withColumn("value",F.col("value").cast("string"))
      .withColumn("enqueuedtime",decode_enqueuedtime_udf(F.col("headers")[3].value))
     )

# Let's compare the decoded iothub-enqueuedtime value with the timestamp created by our Raspberry Pi simulator
#  and see what the latency is
df = df.select("value", "timestamp", "headers", "enqueuedtime")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Examining message latency
# MAGIC Compare the timestamp value inside the message value (first column) to the decoded iothub-enqueuedtime value (in the last column).
# MAGIC * They are generally within a second of each other showing there is very low latency between IoT Hub and Databricks
# MAGIC 
# MAGIC Note that the table widget above refreshes itself independently and is not reflective of when messages actually to the Kafka connector.
# MAGIC 
# MAGIC **Cancel your stream** in the previous cell and continue

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Difference Between IoT Messages and Device "Signals"
# MAGIC Up until now we've focused on configuring Kafka and the structure of messages sent by IoT Hub. Now let's consider the device data found in the `value` column.
# MAGIC 
# MAGIC In this demonstration we use a single Raspberry Pi simulator to generate messages from eleven (11) different devices: 10 wind turbines and a one weather station. We do this so you don't have to configure 11 Raspberry Pi pages and 11 IoT Hub Devices, but the result from the perspective of the Kafka stream is the same. Kafka sees a series of messages that appear to come from various IoT devices.
# MAGIC 
# MAGIC #### Message Data
# MAGIC Look at the JSON data in the `value` column, each value contains the following data sent by the "IoT device":
# MAGIC * A timestamp
# MAGIC * A deviceId - e.g., "WeatherCapture", "WindTurbine-1", "WindTurbine-2", etc.
# MAGIC * And various "signals" - rpm and angle for wind turbines, temperature, humidity, windspeed, and winddirection for the weather station.
# MAGIC 
# MAGIC Unlike Databricks Auto Loader, which can dynamically "infer" and "evolve" schema for files that it reads, with streaming sources you need to declare the schema.
# MAGIC * We'll come back to this shortly.
# MAGIC * [TODO-should we expand on why and give more coding/techniques to capture the IoT device schema?]*
# MAGIC 
# MAGIC #### Message Batching
# MAGIC In some deployments the IoT device might queue up and transmit multiple messages in a "batch" every so often. IoT Hub supports this so long as the message size does not 256 KB. 
# MAGIC 
# MAGIC * Batched messages might be used to reduce transmission costs or when radio connections are intermittent.
# MAGIC * If this is a possibility you need to "explode" the value column data to reconstitute the original device message data.
# MAGIC * This is not the case in our demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing Message Data
# MAGIC There are different ways that organizations may want to process IoT data and we'll discuss two below.
# MAGIC 
# MAGIC #### Presenting IoT Data as a Key Value Stream
# MAGIC In this case the goal is to pivot incoming device signals onto rows in key-value format. Using our simulated IoT data in this notebook the output would look like this:
# MAGIC 
# MAGIC |deviceType|deviceId|timestamp|key|value|
# MAGIC |----------|--------|---------|-----|-----|
# MAGIC | turbine_v1 |WindTurbine-3 |2022-12-20 02:54:50|rpm  | 8.307488809654679 |
# MAGIC | turbine_v1 |WindTurbine-3 |2022-12-20 02:54:50|angle| 6.269052708447845 |
# MAGIC | turbine_v1 |WindTurbine-7 |2022-12-20 02:59:21|rpm  | 22.965467930748880 |
# MAGIC | turbine_v1 |WindTurbine-7 |2022-12-20 02:59:21|angle| 270.527084269047845 |
# MAGIC | atmospheric_v1|WeatherCapture|2022-12-20 03:00:01|temperature|30.73272921062668|
# MAGIC | atmospheric_v1|WeatherCapture|2022-12-20 03:00:01|humidity|68.64895850446567|
# MAGIC | atmospheric_v1|WeatherCapture|2022-12-20 03:00:01|windspeed|6.864895850446567|
# MAGIC | atmospheric_v1|WeatherCapture|2022-12-20 03:00:01|winddirection|SW|
# MAGIC 
# MAGIC One extra consideration for this use case is that the value column must be a single data type. So if you expect text values as shown above for `winddirection`
# MAGIC you need to cast any numeric data into strings and be mindful of this for downstream applications.
# MAGIC 
# MAGIC #### Capturing and Separating Device Data into Separate Bronze_Raw Tables
# MAGIC In this case you want to send message data for different IoT device types (weather station and wind turbine) to different tables.
# MAGIC You can do this by:
# MAGIC 1. Splitting the stream with a filter (as done in the `iot-e2e-01-dlt-pipeline` companion notebook) or 
# MAGIC 2. With a UDF that matches each incoming message to a specific schema.
# MAGIC 
# MAGIC In either case the result is the same and you will create streaming Delta Live Tables for `turbine_raw` and `weather_raw`.
# MAGIC 
# MAGIC You might want to capture the incoming messages in an `iot_raw` table and have a second stream reader/writer produce 
# MAGIC the separate raw device tables.
