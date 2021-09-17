# pyspark-stream-app
---
PySpark streaming application to read from Kafka topic and write to PostgreSQL sink

---

## Kafka Scripts
----

Create a kafka topic as mentioned in the kafka_code/

## Fake Data Generator
---
This project uses faker module to generate fake data and this data is send over to the created kafka topic.

To start the fake data generator run fake_datagen/data_gen_.py

## Spark Job Submission
---

Make sure that the required `postgresql jar` file is present in your path.

run the below command to deploy in local machine
> spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars /home/hadoop/softwares/jars/postgresql-42.2.23.jar streamin_app/driver.py
