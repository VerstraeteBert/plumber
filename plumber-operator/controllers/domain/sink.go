package domain

import (
	"github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"strings"
)

type SinkType int

const (
	KafkaSink SinkType = iota
)

type Sink struct {
	Name    string
	Type    SinkType
	Brokers []string
	Topic   string
}

func getSinkTypeFromCRDVal(sinkType string) SinkType {
	switch sinkType {
	case "Kafka":
		return KafkaSink
	default:
		// shouldn't ever get here as the type is checked in the Webhook, TODO handle anyway?
		return -1
	}
}

func convertSinkFromCRD(sinkName string, sink v1alpha1.Sink) Sink {
	// TODO support other sinks than Kafka
	return Sink{
		Name:    sinkName,
		Type:    getSinkTypeFromCRDVal(sink.Type),
		Brokers: strings.Split(sink.Brokers, ";"),
		Topic:   sink.Topic,
	}
}
