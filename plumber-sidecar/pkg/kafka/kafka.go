package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/VerstraeteBert/plumber-sidecar/pkg/config"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Kafka struct {
	Consumer Consumer
	Producer Producer
}

// returns new Kafka instance with producer and consumer functionality
func InitKafka(cfg *config.Config) (*Kafka, error) {
	newKfk := Kafka{}
	err := newKfk.initConsumer(cfg)
	if err != nil {
		return nil, err
	}

	err = newKfk.initProducer(cfg)
	if err != nil {
		return nil, err
	}

	return &newKfk, nil
}

// creates connection with Kafka brokers for consuming/producing messages
func (k *Kafka) StartConsumeLoop(consumeCallback func(context.Context, *sarama.ConsumerMessage) (bool, error)) error {
	// set up a consumer group with specified initial offset, if there is no committed offset
	k.Consumer.callback = consumeCallback
	zap.L().Debug("Setting up consume/produce connections")

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := k.Consumer.cgClient.Consume(ctx, []string{k.Consumer.inputTopic}, &k.Consumer); err != nil {
				zap.L().Panic(fmt.Sprintf("Error from consumer: %v", err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			k.Consumer.ready = make(chan bool)
		}
	}()

	<-k.Consumer.ready // Await till the consumer has been set up
	zap.L().Info("Sarama consumer up and running")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		zap.L().Debug("terminating: context cancelled")
	case <-sigterm:
		zap.L().Debug("terminating: via signal")

	}
	cancel()
	wg.Wait()
	k.Producer.closeBestEffort()
	k.Consumer.closeBestEffort()
	return nil
}
