package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-kit/log"
)

const (
	build = "local"
	topic = "myTopic"
)

func main() {
	flag.Parse()

	l := log.NewJSONLogger(os.Stdout)
	l = log.WithPrefix(l, "build", build)
	l = log.WithPrefix(l, "date", log.DefaultTimestampUTC)

	l.Log("level", "info", "msg", "starting producer")

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":         "localhost",
		"go.delivery.reports":       true,
		"go.delivery.report.fields": "all",
		"go.events.channel.size":    1,
		"go.produce.channel.size":   1,
		"go.logs.channel.enable":    true,

		/* Enable the Idempotent Producer
		https://redventures.udemy.com/course/apache-kafka/learn/lecture/11567052#overview
		https://issues.apache.org/jira/browse/KAFKA-5494
		Using an idempotent producer provides emoves any possibility of duplicates caused
		by network errors when acks from Kafka fail to be sent to the producer while also
		maintaining ordering with retries
		Defaults:
			retries=integer.MAX_VALUE (2^31-1 = 2147483647)
			max.in.flight.requests=1 (Kafka == 0.11)
			max.in.flight.requests=5 (Kafka >= 1.0)
			acks=al
		*/
		"enable.idempotence": true,

		/* Enable compression
		To have a high throughput, low latency producer it is important to enable compression
		and set to batching. Batches will send for when either of the above conditions are true.
		NOTE: Any message that is bigger than the batch size will not be batched
		Kafka has an avg batch size metric through Kafka Producer Metrics
			"linger.ms" -- sets the max amount of time to wait before sending a batch
			"batch.size" -- sets the max amount of data to buffer before sending a batch
		*/
		"compression.type": "snappy",
		"linger.ms":        20,
		"batch.size":       16, // Defaut is 16kb
	})
	if err != nil {
		l.Log("level", "error", "msg", "could not create kafka producer", "err", err.Error())
		os.Exit(1)
	}

	// List for a shutdown or stop signal
	done := make(chan struct{}, 1)
	shutdown := make(chan os.Signal, 2)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	// Create our ticker to produce messages
	interval, err := time.ParseDuration("5s")
	if err != nil {
		l.Log("level", "error", "msg", "could no parse ticker interval", "err", err.Error())
		os.Exit(1)
	}
	go ticker(interval, producer, done)

	// Create a background process to notify results of producing messages
	go func() {
		defer close(done)
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
					return
				}

				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	// TODO: Gracefull shutdown
}

// Create a ticker to produce messages on an interval
func ticker(interval time.Duration, p *kafka.Producer, done chan struct{}) {
	t := time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-done:
				t.Stop()
				return
			case <-t.C:
				p.ProduceChannel() <- kafkaMessage()
			}
		}
	}()
}
