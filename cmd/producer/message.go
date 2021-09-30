package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/segmentio/ksuid"
)

func kafkaMessage() *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     kafkaString(topic),
			Partition: kafka.PartitionAny,
		},
		Value: []byte(ksuid.New().String()),
	}
}

func kafkaString(s string) *string {
	return &s
}
