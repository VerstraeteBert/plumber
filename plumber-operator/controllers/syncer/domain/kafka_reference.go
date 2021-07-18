package domain

import "strings"

type InitialOffset string

const (
	OffsetLatest   InitialOffset = "Latest"
	OffsetEarliest               = "Earliest"
)

type KafkaReference struct {
	brokers       []string
	topic         string
	consumerGroup string
	isInternal    bool
	initialOffset InitialOffset
}

func (k *KafkaReference) GetBootstrap() []string {
	if k.isInternal {
		return strings.Split(PlumberKafkaBootstrap, ";")
	} else {
		return k.brokers
	}
}

func (k *KafkaReference) GetBrokers() []string {
	if k.isInternal {
		return strings.Split(PlumberKafkaBrokers, ";")
	} else {
		return k.brokers
	}
}

func (k *KafkaReference) GetTopicName() string {
	return k.topic
}

func (k *KafkaReference) IsInternal() bool {
	return k.isInternal
}

func (k *KafkaReference) GetConsumerGroup() string {
	return k.consumerGroup
}

func (k *KafkaReference) GetInitialOffset() InitialOffset {
	return k.initialOffset
}
