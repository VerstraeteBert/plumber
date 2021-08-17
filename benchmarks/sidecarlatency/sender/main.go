package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	brokers     string = "plumber-cluster-kafka-bootstrap.plumber-kafka:9092"
	outputTopic string = "benchmark-input"
)

type benchmarkMessage struct {
	ProduceTime string `json:"produceTime"`
}

func main() {
	log.Println("Starting a new Sarama producer")
	configProducer := sarama.NewConfig()
	configProducer.Version = sarama.V2_7_0_0
	configProducer.Producer.RequiredAcks = sarama.WaitForAll
	configProducer.Producer.Retry.Max = 5
	configProducer.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(brokers, ","), configProducer)
	if err != nil {
		log.Panicf("Error creating sync producer: %v", err)
	}

	for {
		msg := benchmarkMessage{
			ProduceTime: strconv.FormatInt(time.Now().UnixNano(), 10),
		}
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			log.Fatalln(err)
		}

		part, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: outputTopic,
			Value: sarama.ByteEncoder(jsonMsg),
		})
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Println("Sent msg to partition:", part, ", offset:", offset)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
