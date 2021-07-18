package domain

import (
	"github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"strings"
)

type SourceType int

const (
	KafkaSource SourceType = iota
)

type Source struct {
	Name    string
	Type    SourceType
	brokers []string
	topic   string
}

func getSourceTypeFromCRDVal(sourceType string) SourceType {
	switch sourceType {
	case "Kafka":
		return KafkaSource
	default:
		// shouldn't ever get here as the type is checked in the Webhook, TODO handle anyway? (webhook down)
		return -1
	}
}

func convertSourceFromCRD(sourceName string, source v1alpha1.Source) Source {
	// currently only kafka support TODO others
	return Source{
		Name:    sourceName,
		Type:    getSourceTypeFromCRDVal(source.Type),
		brokers: strings.Split(source.Brokers, ";"),
		topic:   source.Topic,
	}
}
