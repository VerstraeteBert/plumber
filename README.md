# plumber
[![Go Report Card](https://goreportcard.com/badge/github.com/VerstraeteBert/plumber)](https://goreportcard.com/report/github.com/VerstraeteBert/plumber)  
Plumber provides a way to run a stateless serverless stream processing pipeline with little effort.
Allowing its users to only focus on the processing logic and the topology structure,
while plumber manages the lifecycle of the pipeline components, and the plumbing of the eventing mesh.

Note: this is still an early prototype

\* With possible message loss

## Concepts
TODO

## Setup

### Go 1.15.x
Kubebuilder does not support go 1.16.x yet, this operator was tested and built using go 1.15.8.
An easy way to manage go versions is [gvm](https://github.com/moovweb/gvm).

### A Kubernetes cluster
Testing and development was done using [Kind](https://kind.sigs.k8s.io/), which is the only local cluster guaranteed to work with the operator and its dependencies

### Setting up the testing dependencies
The operator is bundled with [a script](./plumber-operator/hack/cluster-setup/setup-testing-env.sh) to install all required dependencies
* A minimal, ephemeral kafka cluster using the [strimzi-kafka-operator](https://github.com/strimzi/strimzi-kafka-operator)
* Event-driven autoscaling operator [keda](https://keda.sh/)

### Installing the CRD
`make manifests` in the operator root directory, apply the resulting CRDs in the `config/crd` map. (make sure to only apply the plumber specific crds)

### Running the operator
`make run` in the operator root directory

### Running samples
Toplogy samples are available [here](./plumber-operator/config/samples)

## License
plumber-operator is licensed under the AGPLv3.0 license. See the [LICENSE](LICENSE) for more details. 
