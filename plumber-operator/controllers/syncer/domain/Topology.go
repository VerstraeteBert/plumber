package domain

import (
	"fmt"
	"github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
)

type Topology struct {
	ProjectName         string
	Processors          map[string]Processor
	Sinks               map[string]Sink
	Sources             map[string]Source
	nameToComponentType map[string]ComponentType
	defaultMaxReplicas  int
}

func (ct *Topology) registerProcessor(processorName string, processor v1alpha1.Processor) error {
	if err := ct.validateComponentName(processorName); err != nil {
		return err
	}
	ct.Processors[processorName] = convertProcessorFromCRD(processorName, processor, ct.defaultMaxReplicas)
	ct.nameToComponentType[processorName] = ComponentProcessor
	return nil
}

func (ct *Topology) registerSource(sourceName string, source v1alpha1.Source) error {
	if err := ct.validateComponentName(sourceName); err != nil {
		return err
	}
	ct.Sources[sourceName] = convertSourceFromCRD(sourceName, source)
	ct.nameToComponentType[sourceName] = ComponentSource
	return nil
}

func (ct *Topology) registerSink(sinkName string, sink v1alpha1.Sink) error {
	if err := ct.validateComponentName(sinkName); err != nil {
		return err
	}
	ct.Sinks[sinkName] = convertSinkFromCRD(sinkName, sink)
	ct.nameToComponentType[sinkName] = ComponentSink
	return nil
}

func (ct *Topology) validateComponentName(componentName string) error {
	// a component's name must be unique within the topology
	if componentName == "" {
		return fmt.Errorf("name of a component is not set")
	}
	if _, exists := ct.nameToComponentType[componentName]; exists {
		return fmt.Errorf("a component with name %s is declared multiple times", componentName)
	}

	return nil
}

func (ct *Topology) validateSemantically() error {
	// a) the overall topology must be a DAG
	// b) the connected components must have a correct type
	//		- a processor can take input from another processor or source
	//      - a sink can take input from a processor
	//      - a source cannot take any input
	// c) there must be an end to end data flow (every flow must end in a sink)
	// We can validate these constraints in O(V + E), with V being the number of nodes (# processors + # sinks + # sources) and E being the number of edges
	// 		- Loop over the processors, adding incoming / outgoing edges for each of their SinkBindings/inputFrom entries
	//				- since each processor can only refer to a sink/processor and must have at least one outputStream, we can just check if the references are of a valid type and that there is at least one to be valid.
	topoGraph := InitTopoGraph(len(ct.nameToComponentType))
	for compName, compTy := range ct.nameToComponentType {
		topoGraph.AddNode(compName, compTy)
	}
	for _, p := range ct.Processors {
		if inputCompType, found := ct.nameToComponentType[p.InputComponentName]; found {
			if !(inputCompType == ComponentSource || inputCompType == ComponentProcessor) {
				// references invalid component type
				return fmt.Errorf("processor with name %s has an invalid connection: cannot take input from a %s with name %s", p.Name, inputCompType, p.InputComponentName)
			}
			topoGraph.AddEdge(p.InputComponentName, p.Name)
		} else {
			// not found err
			return fmt.Errorf("processor with name %s has an invalid input reference: %s does not exist", p.Name, p.InputComponentName)
		}

		for _, outputCompName := range p.OutputComponentNames {
			if outputCompName == "" {
				// no sinkbinding
				continue
			}
			if outputCompType, found := ct.nameToComponentType[outputCompName]; found {
				if !(outputCompType == ComponentSink) {
					// references invalid component type
					return fmt.Errorf("processor with name %s has an invalid connection: cannot output from processor to a %s with name %s", p.Name, outputCompType, outputCompName)
				}
				topoGraph.AddEdge(p.Name, outputCompName)
			} else {
				return fmt.Errorf("processor with name %s has an invalid output reference: %s does not exist", p.Name, outputCompName)
			}
		}
	}
	// Check if DAG (topological sorting)
	return topoGraph.TopoSort()
}

func initTopology(projectName string, defaultMaxReplicas int) Topology {
	return Topology{
		ProjectName:         projectName,
		Processors:          make(map[string]Processor),
		Sinks:               make(map[string]Sink),
		Sources:             make(map[string]Source),
		nameToComponentType: make(map[string]ComponentType),
		defaultMaxReplicas:  defaultMaxReplicas,
	}
}

func BuildTopology(topoParts *v1alpha1.TopologyPartList, projectName string, defaultMaxReplicas int) (*Topology, []error) {
	errorFound := false
	errorList := make([]error, 0)
	domainTopo := initTopology(projectName, defaultMaxReplicas)
	for _, topoPart := range topoParts.Items {
		if topoPart.Spec.Sources != nil {
			for sourceName, sourceSpec := range topoPart.Spec.Sources {
				err := domainTopo.registerSource(sourceName, sourceSpec)
				if err != nil {
					errorList = append(errorList, err)
					errorFound = true
				}
			}
		}
		if topoPart.Spec.Processors != nil {
			for procName, procSpec := range topoPart.Spec.Processors {
				err := domainTopo.registerProcessor(procName, procSpec)
				if err != nil {
					errorList = append(errorList, err)
					errorFound = true
				}
			}
		}
		if topoPart.Spec.Sinks != nil {
			for sinkName, sinkSpec := range topoPart.Spec.Sinks {
				err := domainTopo.registerSink(sinkName, sinkSpec)
				if err != nil {
					errorList = append(errorList, err)
					errorFound = true
				}
			}
		}
	}
	// if name collisions are present, do not start semantic validation
	if errorFound {
		return &Topology{}, errorList
	}
	if semError := domainTopo.validateSemantically(); semError != nil {
		errorList = append(errorList, semError)
		return &Topology{}, errorList
	}
	// Semantically & syntactically valid
	// We now have a complete view of the DESIRED Topology skeleton
	//		-> treat it as a completely new one, disregarding any current present objects in Kubernetes
	// 		-> Now we need to create the correct connections, through setting of Kafka configs per processor
	// Loop over all processors, convert a reference to a source, another processor, or sink ->  a topic & CG, and other Kafka metadata
	for k, processor := range domainTopo.Processors {
		// processor.inputComponentName can either reference a source or another processor
		inputCompType := domainTopo.nameToComponentType[processor.InputComponentName]
		if inputCompType == ComponentSource {
			srcRef := domainTopo.Sources[processor.InputComponentName]
			processor.InputKafkaReference = KafkaReference{
				isInternal:    false,
				brokers:       srcRef.brokers,
				topic:         srcRef.topic,
				consumerGroup: GetDefaultConsumerGroupName(processor.Name, projectName),
				initialOffset: OffsetLatest, // default CG to Latest if taking input from a source
			}
		} else {
			// processor -> uses Plumber's kafka broker (internal)
			// get the producing processor
			producer := domainTopo.Processors[processor.InputComponentName]
			processor.InputKafkaReference = KafkaReference{
				isInternal:    true,
				brokers:       nil,
				topic:         GetDefaultOutputTopic(producer.Name, projectName),
				consumerGroup: GetDefaultConsumerGroupName(processor.Name, projectName),
				initialOffset: OffsetEarliest, // default CG to earliest if taking input from another processor
			}
			producer.OutputKafkaReferences["default"] = KafkaReference{
				brokers:       nil,
				topic:         GetDefaultOutputTopic(producer.Name, projectName),
				consumerGroup: "",
				isInternal:    true,
			}
			producer.SetOutputPartitionsIfHigher(processor.MaxReplicas)
			domainTopo.Processors[producer.Name] = producer
		}
		// outputs -> sinkbindings
		for _, outputCompName := range processor.OutputComponentNames {
			outputComponentType := domainTopo.nameToComponentType[outputCompName]
			// nothing special needs to be done for connecting processors, since they just read from the default output topic
			if outputComponentType == ComponentSink {
				sinkRef := domainTopo.Sinks[outputCompName]
				processor.OutputKafkaReferences[outputCompName] = KafkaReference{
					brokers:       sinkRef.Brokers,
					topic:         sinkRef.Topic,
					consumerGroup: "",
					isInternal:    false,
				}
			}
		}
		domainTopo.Processors[k] = processor
	}
	return &domainTopo, nil
}
