package domain

type ComponentType string

const (
	ComponentSource    ComponentType = "source"
	ComponentSink                    = "sink"
	ComponentProcessor               = "processor"
)

const CompositionKubeName = "plumber-composed-topology"
const PlumberKafkaBootstrap = "plumber-cluster-kafka-bootstrap.plumber-kafka:9092"
const PlumberKafkaBrokers = "plumber-cluster-kafka-brokers.plumber-kafka:9092"
