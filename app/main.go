package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
)

var Conn *kafka.Conn

func Consumer1(topicName string, partition int) {
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
		fmt.Printf("message at offset %d: %s = %s Consumer1\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}

func Consumer2(topicName string, partition int) {
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
		fmt.Printf("message at offset %d: %s = %s Consumer2\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}

func PublishMessage(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Publishing message...")

	_, err := Conn.WriteMessages(
		kafka.Message{Value: []byte("Hello Kafka!")})
	if err != nil {
		fmt.Println("Error writing messages:", err.Error())
		return
	}

	fmt.Printf("Wrote messages to topic\n")
}

func InitKafkaConnection(topic string, partition int) *kafka.Conn {

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("Error connecting to Kafka:", err.Error())
		return nil
	}
	fmt.Println("Kafka Connection initialized")
	return conn
}

func main() {
	topic := "demo-topic"
	partition := 0

	Conn = InitKafkaConnection(topic, partition)

	go Consumer1(topic, partition)
	go Consumer2(topic, partition)

	http.HandleFunc("/publish", PublishMessage)

	addr := ":3000"
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Println("Error while server starting.. ", err.Error())
		return
	}

}
