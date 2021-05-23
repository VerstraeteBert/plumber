package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/VerstraeteBert/plumber-sidecar/pkg/config"
	"github.com/VerstraeteBert/plumber-sidecar/pkg/util"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"go.uber.org/zap"
	"sort"
	"strings"
)

type Producer struct {
	producers []brokersProducerWrapper
}

// brokersProducerWrapper is a wrapper around a single sarama.SyncProducer, with possibly multiple topics for that syncProducer
type brokersProducerWrapper struct {
	topics         []string
	brokers        []string
	saramaProducer sarama.SyncProducer
}

func (p *Producer) PublishDownstream(ctx context.Context, message binding.Message) error {
	// TODO determine if we should fail fast (dont produce to next broker/topic set if previous failed) or try best effort delivery like right now
	producingFailed := false
	for _, wrapper := range p.producers {
		if len(wrapper.topics) == 1 {
			// single topic -> sendMessage
			pMessage := sarama.ProducerMessage{
				Topic: wrapper.topics[0],
			}
			err := kafka_sarama.WriteProducerMessage(ctx, message, &pMessage)
			// this error can only happen if the message passed is invalid -> will happen on first attempt -> fail immediatly
			if err != nil {
				zap.L().Error("failed to convert cloudevent sdk binding message to sarama message", zap.Error(err))
				return err
			}
			_, _, err = wrapper.saramaProducer.SendMessage(&pMessage)
			if err != nil {
				producingFailed = true
				zap.L().Error(fmt.Sprintf("failed to produce messages downstream for topics: %s with brokers %s", strings.Join(wrapper.brokers, ","), strings.Join(wrapper.topics, ",")), zap.Error(err))
			}
		} else {
			// TODO extract some of this into a function at some point
			messages := make([]*sarama.ProducerMessage, 0)
			for _, topic := range wrapper.topics {
				pMessage := sarama.ProducerMessage{
					Topic: topic,
				}
				err := kafka_sarama.WriteProducerMessage(ctx, message, &pMessage)
				if err != nil {
					zap.L().Error("failed to convert cloudevent sdk binding message to sarama message", zap.Error(err))
					return err
				}
				messages = append(messages, &pMessage)
			}
			err := wrapper.saramaProducer.SendMessages(messages)
			if err != nil {
				producingFailed = true
				zap.L().Error(fmt.Sprintf("failed to produce messages downstream for topics: %s with brokers %s", strings.Join(wrapper.brokers, ","), strings.Join(wrapper.topics, ",")), zap.Error(err))
			}
		}
	}
	if producingFailed {
		return fmt.Errorf("failed to produce messages to some of the downstream components")
	} else {
		return nil
	}
}

func (p *Producer) closeBestEffort() {
	for _, wrapper := range p.producers {
		err := wrapper.saramaProducer.Close()
		if err != nil {
			// if any error occurs, continue, we want to try to close all of them
			zap.L().Warn("failed to close a producer client", zap.Error(err))
		}
	}
}

func (k *Kafka) initProducer(config *config.Config) error {
	// create a single SyncProducer for each distinct identified brokers list
	// ensure topics are not duplicated either for each distinct brokers list
	brokersToTopicsMap := make(map[string]*util.Set)
	for _, outRef := range config.ConfOutputRefs {
		// sort first to ensure that equality even if user specified brokers in different order
		sort.Strings(outRef.Brokers)
		brokersStr := strings.Join(outRef.Brokers, ",")

		if set, found := brokersToTopicsMap[brokersStr]; found {
			set.Add(outRef.Topic)
		} else {
			newSet := util.NewSet()
			newSet.Add(outRef.Topic)
			brokersToTopicsMap[brokersStr] = newSet
		}
	}

	k.Producer = Producer{producers: make([]brokersProducerWrapper, 0)}
	for brokersStr, topicSet := range brokersToTopicsMap {
		saramaConf := sarama.NewConfig()
		saramaConf.Producer.RequiredAcks = sarama.WaitForAll
		saramaConf.Producer.Retry.Max = 5
		saramaConf.Producer.Return.Successes = true

		producer, err := sarama.NewSyncProducer(strings.Split(brokersStr, ","), saramaConf)
		if err != nil {
			k.Producer.closeBestEffort()
			return err
		}
		k.Producer.producers = append(k.Producer.producers, brokersProducerWrapper{
			topics:         topicSet.SetToSlice(),
			saramaProducer: producer,
		})
	}

	return nil
}
