# plumber

[![Go Report Card](https://goreportcard.com/badge/github.com/VerstraeteBert/plumber)](https://goreportcard.com/report/github.com/VerstraeteBert/plumber)  
Plumber provides a way to run a stateless serverless stream processing pipeline with little effort.
Allowing its users to only focus on the processing logic and the topology structure,
while plumber manages the lifecycle of the pipeline components, and the plumbing of the eventing mesh.

## Concepts

TODO

## Setup

### Go 1.15.x

Kubebuilder does not support go 1.16.x yet, this operator was tested and built using go 1.15.8. An easy way to manage go versions is [gvm](https://github.com/moovweb/gvm).

```shell
sudo snap install go --channel 1.15/stable --classic
```

### A Kubernetes cluster

Testing and development was done using [Kind](https://kind.sigs.k8s.io/), which is the only local cluster guaranteed to work with the operator and its dependencies.

```bash
sudo apt install docker.io
sudo groupadd docker
sudo usermod -aG docker $USER
sudo snap install kubectl --classic
GO111MODULE="on" go get sigs.k8s.io/kind@v0.11.1
```

Finally, create the k8s cluster

```bash
~/go/bin/kind create cluster
```

### Setting up the testing dependencies

The operator is bundled with [a script](./plumber-operator/hack/cluster-setup/setup-testing-env.sh) to install all required dependencies

* A minimal, ephemeral kafka cluster using the [strimzi-kafka-operator](https://github.com/strimzi/strimzi-kafka-operator)
* Event-driven autoscaling operator [keda](https://keda.sh/)

```shell
plumber-operator/hack/cluster-setup/setup-testing-env.sh
```

### Installing the CRDs

Run the following in the operator root directory.

```shell
cd plumber-operator
make manifests
```

Afterwards, `kubectl apply -f` the resulting CRDs in the directory `config/crd/bases` which have the prefix `plumber.ugent.be`.

```shell
kubectl apply -f config/crd/bases/plumber.ugent.be_topologies.yaml
kubectl apply -f config/crd/bases/plumber.ugent.be_topologyparts.yaml
kubectl apply -f config/crd/bases/plumber.ugent.be_topologyrevisions.yaml
```

### Local development prep

The controllers needs to have access to the Kubernetes cluster's DNS to function (i.e. to access the Kafka Cluster), to enable local development [Telepresence](https://www.telepresence.io/) may be used.

First, install telepresence (instructions below are for Linux).

```shell
# 1. Download the latest binary (~50 MB):
sudo curl -fL https://app.getambassador.io/download/tel2/linux/amd64/latest/telepresence -o /usr/local/bin/telepresence
# 2. Make the binary executable:
sudo chmod a+x /usr/local/bin/telepresence
```

Secondly, connect your local workstation to a (remote) Kubernetes cluster.

```shell
telepresence connect
```

### Running the operator

`make run` in the operator root directory

### Running samples

Toplogy samples are available [here](./plumber-operator/config/samples)

## Breakdown

Run the following command to remove the entire kubernetes cluster.

`kind delete cluster`

## License

plumber-operator is licensed under the AGPLv3.0 license. See the [LICENSE](LICENSE) for more details.
