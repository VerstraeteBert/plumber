package syncer

import (
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	"strings"
)

type processorKafkaRefs struct {
	inputRef   kafkaRef
	outputRefs []kafkaRef
}

type kafkaRef struct {
	bootstrapServers []string
	topic            string
	consumerGroup    string
	initialOffset    string
}

func (sh *syncerHandler) buildProcessorKafkaRefs(pName string, processor plumberv1alpha1.ComposedProcessor) processorKafkaRefs {
	var kRefs processorKafkaRefs
	// build input ref
	if inputSource, takesInputFromSource := sh.activeRevision.Spec.Sources[processor.InputFrom]; takesInputFromSource {
		kRefs.inputRef = kafkaRef{
			bootstrapServers: strings.Split(inputSource.Brokers, ","),
			topic:            inputSource.Topic,
			consumerGroup:    processor.Internal.ConsumerGroup,
			initialOffset:    processor.Internal.InitialOffset,
		}
	} else {
		// takes input from processor
		kRefs.inputRef = kafkaRef{
			// uses internal plumber kafka
			bootstrapServers: strings.Split(PlumberKafkaBootstrap, ","),
			// default topic name based on revision etc
			topic:         shared.BuildOutputTopicName(sh.activeRevision.GetNamespace(), sh.topology.GetName(), processor.InputFrom, sh.activeRevision.Spec.Revision),
			consumerGroup: processor.Internal.ConsumerGroup,
			initialOffset: processor.Internal.InitialOffset,
		}
	}
	// build output ref(s)
	if processor.HasOutputTopic() {
		kRefs.outputRefs = append(kRefs.outputRefs, kafkaRef{
			bootstrapServers: strings.Split(PlumberKafkaBootstrap, ","),
			topic:            shared.BuildOutputTopicName(sh.activeRevision.GetNamespace(), sh.topology.GetName(), pName, sh.activeRevision.Spec.Revision),
		})
	}
	// check is necessary because strings.Split("", ",") will result in: [""]
	if processor.SinkBindings != "" {
		for _, sinkBinding := range strings.Split(processor.SinkBindings, ",") {
			outputSink, _ := sh.activeRevision.Spec.Sinks[sinkBinding]
			kRefs.outputRefs = append(kRefs.outputRefs, kafkaRef{
				bootstrapServers: strings.Split(outputSink.Brokers, ","),
				topic:            outputSink.Topic,
			})
		}
	}
	return kRefs
}
