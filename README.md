## Kafka
This is a playground for exploring the features of Kafka.

### Use
Start Kafka with one broker and Zookeeper
```$ make kafka```

Create a topic
```$ docker exec -it kafka_kafka_1 kafka-topics.sh --create --bootstrap-server kafka:9092 --topic my-topic```

Create a producer
```$ docker exec -it kafka_kafka_1 kafka-console-producer.sh --bootstrap-server kafka:9092 --topic my-topic```

Create a consumer
```$ docker exec -it kafka_kafka_1 kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic my-topic --from-beginning```