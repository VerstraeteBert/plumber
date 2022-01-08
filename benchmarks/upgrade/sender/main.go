package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	brokers     string = "plumber-cluster-kafka-bootstrap.plumber-kafka:9092"
	outputTopic string = "upgrade-input"
)

type benchmarkMessage struct {
	ProduceTime int64 `json:"produceTime"`
	Id          int64 `json:"id"`
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

	var messId int64 = 0
	for {
		msg := benchmarkMessage{
			ProduceTime: time.Now().UnixNano(),
			Id:          messId,
		}
		messId += 1

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
			fmt.Println("Sent msg to partition: ", part, ", offset: ", offset)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
