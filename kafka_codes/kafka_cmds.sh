#!/usr/bin/env

# start the zookeper
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

# start the kafka server
kafka-server-start.sh $KAFKA_HOME/config/server.properties

# create a kafka topic, if already created it will ignore
kafka-topics.sh --create --topic agro --bootstrap-server localhost:9092

#kafka-console-producer.sh --topic agro --bootstrap-server localhost:9092

#kafka-console-consumer.sh --topic agro --from-beginning --bootstrap-server localhost:9092


spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars /home/tom/softwares/jars/postgresql-42.2.23.jar app.py
