package domain

import (
	"errors"
	"github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	v1 "k8s.io/api/core/v1"
	"strings"
)

type Processor struct {
	Name                  string
	InputComponentName    string
	OutputComponentNames  []string
	InputKafkaReference   KafkaReference
	OutputKafkaReferences map[string]KafkaReference
	Image                 string
	EnvVars               []v1.EnvVar
	MaxReplicas           int
	outputTopicPartitions int
}

func GetDefaultOutputTopic(processorName string, projectName string) string {
	return "plumber-" + projectName + "-" + processorName + "-out"
}

func GetDefaultConsumerGroupName(processorName string, projectName string) string {
	return projectName + "-" + processorName + "-group"
}

func (p *Processor) HasOutputTopic() bool {
	_, found := p.OutputKafkaReferences["default"]
	return found
}

func (p *Processor) GetOutputTopicName() (string, error) {
	defaultOutputRef, found := p.OutputKafkaReferences["default"]
	if !found {
		return "", errors.New("processor does not have an output channel")
	}
	return defaultOutputRef.topic, nil
}

func (p *Processor) GetInputTopicName() string {
	return p.InputKafkaReference.GetTopicName()
}

func (p *Processor) SetOutputPartitionsIfHigher(partitions int) {
	if partitions > p.outputTopicPartitions {
		p.outputTopicPartitions = partitions
	}
}

func (p *Processor) GetOutputTopicPartitions() int {
	return p.outputTopicPartitions
}

func convertProcessorFromCRD(processorName string, processor v1alpha1.Processor, defaultMaxReplicas int) Processor {
	return Processor{
		Name:                  processorName,
		InputComponentName:    processor.InputFrom,
		OutputComponentNames:  strings.Split(processor.SinkBindings, ";"), // inter-processor connections not yet taken into account
		OutputKafkaReferences: make(map[string]KafkaReference),
		Image:                 processor.Image,
		EnvVars:               shared.ConvertEnvVars(processor.Env),
		MaxReplicas:           shared.MaxInt(defaultMaxReplicas, processor.MaxScale),
	}
}

type confInputRef struct {
	Topic         string   `json:"topic"`
	Brokers       []string `json:"brokers"`
	ConsumerGroup string   `json:"consumerGroup"`
	InitialOffset string   `json:"initialOffset"`
}

type confOutputRef struct {
	Topic   string   `json:"topic"`
	Brokers []string `json:"brokers"`
}

type confProcessorDetails struct {
	Name    string `json:"name"`
	Project string `json:"project"`
}

type SidecarConfig struct {
	InputRef         confInputRef         `json:"inputRef"`
	ConfOutputRefs   []confOutputRef      `json:"outputRefs"`
	ProcessorDetails confProcessorDetails `json:"processorDetails"`
}

func (p *Processor) GetSidecarConfig(projectName string) SidecarConfig {
	confOutputRefs := make([]confOutputRef, len(p.OutputKafkaReferences))
	i := 0
	for _, v := range p.OutputKafkaReferences {
		confOutputRefs[i] = confOutputRef{
			Topic:   v.GetTopicName(),
			Brokers: v.GetBootstrap(),
		}
		i++
	}
	return SidecarConfig{
		InputRef: confInputRef{
			Topic:         p.InputKafkaReference.GetTopicName(),
			Brokers:       p.InputKafkaReference.GetBootstrap(),
			ConsumerGroup: p.InputKafkaReference.GetConsumerGroup(),
			InitialOffset: string(p.InputKafkaReference.GetInitialOffset()),
		},
		ConfOutputRefs: confOutputRefs,
		ProcessorDetails: confProcessorDetails{
			Name:    p.Name,
			Project: projectName,
		},
	}
}
