# OS assumtion: Ubuntu 20.04 LTS
cd /opt/kafka_2.12-2.5.0/

# start zookeeper server
bin/zookeeper-server-start.sh config/zookeeper.properties

# stop zookeeper server
bin/zookeeper-server-stop.sh

# start kafka broker
bin/kafka-server-start.sh config/server.properties

# stop kafka broker
bin/kafka-server-stop.sh

# create topic source
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic kafka-source

# start default producer
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic kafka-source

# start default consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-source --from-beginning

# delete topic
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic kafka-source

# list topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
