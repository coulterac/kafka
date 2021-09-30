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
