package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
	"time"
)

func Consumer(wg *sync.WaitGroup, topicName string, partition int) {
	defer wg.Done()
	wg.Add(1)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Topic:     topicName,
		Partition: partition,
		MaxBytes:  10e6, // 10MB
		GroupID:   "consumer-group-id",
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}

func main() {
	topic := "demo-topic"
	partition := 0

	wg := &sync.WaitGroup{}

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("Error connecting to Kafka:", err.Error())
		return
	}
	go Consumer(wg, topic, partition)

	err = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		fmt.Println("Error setting write deadline:", err.Error())
		return
	}
	messages, err := conn.WriteMessages(
		kafka.Message{Value: []byte("Hello Kafka!")},
		kafka.Message{Value: []byte("Hello Kafka!")},
		kafka.Message{Value: []byte("Hello Kafka!")})
	if err != nil {
		fmt.Println("Error writing messages:", err.Error())
		return
	}
	fmt.Printf("Wrote %d messages to topic %s\n", messages, topic)
	wg.Wait()

}
