# iot-e2e-demo
A public repo showing how to Databricks can process IoT messages to support DS/ML and sql analytics requirements.

Initially the focus is on Azure IoT Hub as the service to consume messages from the web but later we can add other notebooks to show how Databricks can consume data from each cloud vendor's native IoT message service.

Delta Live Tables (DLT) is the preferred method for creating data pipelines and in this repo we include one DLT notebook and instructions how to schedule it. As of December 2022 DLT doesn't support the Microsoft eventhubs connector (distributed in a maven file) so we'll use Kafka instead. Also, DLT support for Unity Catalog is coming soon so we'll use the built-in hive metastore for the demo.


