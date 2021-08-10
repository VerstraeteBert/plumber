package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

const (
	brokers       string = "plumber-cluster-kafka-bootstrap.plumber-kafka:9092"
	inputTopic    string = "baseline-output"
	consumerGroup string = "csv-gen-1"
)

type Consumer struct {
	ready chan bool
}

type benchmarkMessage struct {
	ProduceTime string `json:"produceTime"`
}

func main() {
	log.Println("Starting a new Sarama consumer")
	configConsumer := sarama.NewConfig()
	configConsumer.Version = sarama.V2_7_0_0
	configConsumer.Consumer.Offsets.Initial = sarama.OffsetNewest
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), consumerGroup, configConsumer)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	consumer := Consumer{
		ready: make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, []string{inputTopic}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func (c *Consumer) msgHandler(ctx context.Context, cm *sarama.ConsumerMessage) error {
	var benchMess benchmarkMessage
	_ = json.Unmarshal(cm.Value, &benchMess)
	valBegin, _ := strconv.ParseInt(benchMess.ProduceTime, 10, 64)
	valEnd := cm.Timestamp.UnixNano()
	fmt.Printf("Sender timestamp: %d; kafka timestamp: %d; diff in nano: %d, diff in ms: %f\n", valBegin, valEnd, valEnd-valBegin, float64(valEnd-valBegin)/float64(1e+6))
	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		err := consumer.msgHandler(context.TODO(), message)
		if err != nil {
			log.Printf("%v", err)
		}
		session.MarkMessage(message, "")
	}
	return nil
}
