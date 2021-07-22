package syncer

type confInputRef struct {
	Topic string `json:"topic"`
	// TODO FIXME in sidecar & here (brokers -> bootstrap) & test
	Brokers       []string `json:"brokers"`
	ConsumerGroup string   `json:"consumerGroup"`
	InitialOffset string   `json:"initialOffset"`
}

type confOutputRef struct {
	// TODO FIXME in sidecar & here (brokers -> bootstrap) & test
	Topic   string   `json:"topic"`
	Brokers []string `json:"brokers"`
}

type confProcessorDetails struct {
	Name string `json:"name"`
	// TODO think about what needs to be included in sidecar logs
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
			Topic:   o.topic,
			Brokers: o.bootstrapServers,
		})
	}
	return SidecarConfig{
		InputRef: confInputRef{
			Topic:         refs.inputRef.topic,
			Brokers:       refs.inputRef.bootstrapServers,
			ConsumerGroup: refs.inputRef.consumerGroup,
			InitialOffset: refs.inputRef.initialOffset,
		},
		ConfOutputRefs: confOutputRefs,
		ProcessorDetails: confProcessorDetails{
			Name: pName,
			// TODO reevaluate what is used in logs in the sidecar
			Project: sh.activeRevision.GetName(),
		},
	}
}
