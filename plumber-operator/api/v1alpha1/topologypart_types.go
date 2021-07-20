/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TopologyPartSpec defines the desired state of Topology
type TopologyPartSpec struct {
	// +optional
	Sources map[string]Source `json:"sources,omitempty"`
	// +optional
	Sinks map[string]Sink `json:"sinks,omitempty"`
	// +optional
	Processors map[string]Processor `json:"processors,omitempty"`
}

type Source struct {
	// +kubebuilder:validation:Enum=Kafka;Custom
	Type    string `json:"type"`
	Brokers string `json:"brokers"`
	Topic   string `json:"topic"`
}

type Sink struct {
	// +kubebuilder:validation:Enum=Kafka;Custom
	Type    string `json:"type"`
	Brokers string `json:"brokers"`
	Topic   string `json:"topic"`
}

type Processor struct {
	InputFrom string `json:"inputFrom"`
	Image     string `json:"image"`
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	MaxScale *int `json:"maxScale"`
	// +optional
	Env []EnvVar `json:"env,omitempty"`
	// +optional
	SinkBindings string `json:"sinkBindings,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=earliest;latest;continue
	InitialOffset string
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// TopologyPartStatus defines the observed state of TopologyPart
type TopologyPartStatus struct {
	LatestRevision int64 `json:"latestRevision"`
}

// +kubebuilder:object:root=true

// TopologyPart is the Schema for the topologies API
type TopologyPart struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TopologyPartSpec   `json:"spec,omitempty"`
	Status TopologyPartStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TopologyPartList contains a list of TopologyParts
type TopologyPartList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TopologyPart `json:"items"`
}

func (p *Processor) GetMaxScaleOrDefault() int {
	if p.MaxScale == nil {
		return 5
	}
	return *p.MaxScale
}

func init() {
	SchemeBuilder.Register(&TopologyPart{}, &TopologyPartList{})
}
