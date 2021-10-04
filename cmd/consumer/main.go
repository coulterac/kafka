package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	l.Log("level", "info", "msg", "starting consumer")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               "localhost",
		"group.id":                        "myGroup",
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": true,
		"auto.offset.reset":    "earliest",
	})
	if err != nil {
		l.Log("level", "error", "msg", "could not create kafka consumer", "err", err.Error())
		os.Exit(1)
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		l.Log("level", "error", "msg", "could not subscribe to topic", "topic", topic, "err", err.Error())
		os.Exit(1)
	}

	// List for a shutdown or stop signal
	done := make(chan struct{}, 1)
	shutdown := make(chan os.Signal, 2)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	// Create a background process to notify results of producing messages
	go func() {
		defer close(done)
		for e := range consumer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Consumer failed: %v\n", ev.TopicPartition.Error)
					return
				}

				fmt.Printf("Consumed message from topic %s [%d] at offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	for {
		select {
		// Handle shutdown
		case sig := <-shutdown:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			consumer.Close()
			os.Exit(0)

		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}
}
