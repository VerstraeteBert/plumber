package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/VerstraeteBert/plumber-sidecar/pkg/config"
	"go.uber.org/zap"
)

const (
	OffsetLatest   = "Latest"
	OffsetEarliest = "Earliest"
)

type Consumer struct {
	inputTopic    string
	brokers       []string
	consumerGroup string
	callback      func(context.Context, *sarama.ConsumerMessage) (bool, error)

	saramaCfg *sarama.Config
	cgClient  sarama.ConsumerGroup
	ready     chan bool
}

func (c *Consumer) closeBestEffort() {
	if c.cgClient == nil {
		return
	}
	_ = c.cgClient.Close()
}

// initConsumer sets saramaCfg, and takes a callback that should be called on every consumer message
func (k *Kafka) initConsumer(cfg *config.Config) error {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_7_0_0
	switch cfg.InputRef.InitialOffset {
	case OffsetLatest:
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	case OffsetEarliest:
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		return fmt.Errorf("invalid initial offset supplied for consumer: %s", cfg.InputRef.InitialOffset)
	}
	k.Consumer = Consumer{
		ready:         make(chan bool),
		saramaCfg:     saramaCfg,
		inputTopic:    cfg.InputRef.Topic,
		brokers:       cfg.InputRef.Brokers,
		consumerGroup: cfg.InputRef.ConsumerGroup,
	}

	cgClient, err := sarama.NewConsumerGroup(k.Consumer.brokers, k.Consumer.consumerGroup, k.Consumer.saramaCfg)
	if err != nil {
		return err
	}
	k.Consumer.cgClient = cgClient

	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		zap.L().Debug(fmt.Sprintf("Handling claim for partition %d, offset %d", claim.Partition(), message.Offset))
		shouldMark, err := c.callback(context.Background(), message)
		if err != nil {
			zap.L().Warn(fmt.Sprintf("Failed to handle claim for partition %d, offset %d", claim.Partition(), message.Offset), zap.Error(err))

		}
		if shouldMark {
			zap.L().Info(fmt.Sprintf("Marking message of partition %d, offset %d as processed", claim.Partition(), message.Offset))
			session.MarkMessage(message, "")
		}
	}
	return nil
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
