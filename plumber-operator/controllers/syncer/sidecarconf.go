package syncer

type confInputRef struct {
	Topic         string   `json:"topic"`
	Bootstrap     []string `json:"bootstrap"`
	ConsumerGroup string   `json:"consumerGroup"`
	InitialOffset string   `json:"initialOffset"`
}

type confOutputRef struct {
	Topic     string   `json:"topic"`
	Bootstrap []string `json:"bootstrap"`
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

func (sh *syncerHandler) buildSidecarConfig(pName string, refs processorKafkaRefs) SidecarConfig {
	confOutputRefs := make([]confOutputRef, 0)
	for _, o := range refs.outputRefs {
		confOutputRefs = append(confOutputRefs, confOutputRef{
			Topic:     o.topic,
			Bootstrap: o.bootstrapServers,
		})
	}
	return SidecarConfig{
		InputRef: confInputRef{
			Topic:         refs.inputRef.topic,
			Bootstrap:     refs.inputRef.bootstrapServers,
			ConsumerGroup: refs.inputRef.consumerGroup,
			InitialOffset: refs.inputRef.initialOffset,
		},
		ConfOutputRefs: confOutputRefs,
		ProcessorDetails: confProcessorDetails{
			Name:    pName,
			Project: sh.activeRevision.GetName(),
		},
	}
}
