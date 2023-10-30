package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "sync-streaming.chainbase.online:9093",
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.mechanisms":   "SCRAM-SHA-256",
		"group.id":          "{{ consumer_group }}",
		"sasl.username":     "{{ Key }}",
		"sasl.password":     "{{ password }}",
		"auto.offset.reset": "earliest",
		"socket.timeout.ms": 10000,
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"ethereum_logs"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)", err, msg)
		}
	}

	c.Close()
}
