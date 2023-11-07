docker exec -it kafka-1 bash 

./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties

#then execute anything you want
#-----------------------------------------------
#topic
./bin/kafka-topics.sh --create --bootstrap-server localhost:9091 --replication-factor 1 --partitions 3 --topic surya
./bin/kafka-topics.sh --describe --bootstrap-server localhost:9091 --topic surya
./bin/kafka-topics.sh --alter --bootstrap-server localhost:9091  --partitions 2  --topic surya
./bin/kafka-topics.sh --topic surya --delete --bootstrap-server localhost:9091
./bin/kafka-topics.sh --list --bootstrap-server localhost:9091

./bin/kafka-topics.sh --topic surya --delete --bootstrap-server localhost:9091 && \
./bin/kafka-topics.sh --create --bootstrap-server localhost:9091 --replication-factor 1 --partitions 1 --topic surya


#producer
./bin/kafka-console-producer.sh --bootstrap-server localhost:9091 --topic surya --property "parse.key=true" --property "key.separator=:"
./bin/kafka-console-producer.sh --bootstrap-server localhost:9091 --topic surya



#consumer - describing the cg1, the consumer should be down - keep another consumer with another topic in same cg1
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9091 --topic surya --group cg1
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9091 --topic surya --from-beginning --group cg1


./bin/kafka-console-consumer.sh --bootstrap-server localhost:9091 --topic surya  
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9091 --topic surya --from-beginning
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9091 --topic surya --offset 2 --partition 0

./bin/kafka-console-consumer.sh --help

#groups
./bin/kafka-consumer-groups.sh --help
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9091 --describe --group cg1
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9091 --list
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9091 --delete --group cg1


./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9091 --describe --group cg1
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9091 --group cg1 --reset-offsets --to-earliest --execute --topic surya
--to-earliest
--to-lasurya
--shift-by
--to-current
--to-offset
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9091 --group cg1 --reset-offsets --shift-by -3 --execute --topic surya




#-----------------------------------------------
