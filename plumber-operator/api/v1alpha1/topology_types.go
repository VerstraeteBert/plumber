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

type TopologyPartReference struct {
	Name     string `json:"name"`
	Revision int64  `json:"revision"`
}

// TopologySpec defines the desired state of the Topology
type TopologySpec struct {
	// Parts holds a list of references to TopologyPart revisions, such that the updater can construct full Topologies from these references
	// +optional
	Parts []TopologyPartReference `json:"parts,omitempty"`
	// DefaultScale corresponds to the default maximum amount of replicas a processor can have
	// This field is used to determine the number of partitions any of the input topics of the processors must have.
	// The value can be overwritten on each individual processor, if multiple processors consume from a topic, the highest value is picked.
	// It has a minimum value of 1, a maximum value of 100 and a default value of 5
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	DefaultScale *int `json:"defaultScale,omitempty"`
}

// TopologyStatus defines the observed state of Topology
type TopologyStatus struct {
	// ActiveRevision is the TopologyRevision ID that is currently active, i.e. actively processing messages end-to-end
	// +optional
	ActiveRevision *int64 `json:"activeRevision,omitempty"`
	// NextRevision holds the TopologyRevision ID that is queued up to become the ActiveRevision
	// +optional
	NextRevision *int64 `json:"nextRevision,omitempty"`
	// PhasingOutRevisions hold a list of TopologyRevision IDs that are currently being phased-out, i.e. processing any left-over messages
	// +optional
	PhasingOutRevisions []int64 `json:"phasingOutRevisions,omitempty"`
	// Status contains the last observed status of the overall Topology
	// +optional
	Status []metav1.Condition `json:"topoconds"`
	// SourceStatuses contains a map of the Source's spec names to their respective last observed statuses
	// +optional
	SourceStatuses map[string]*[]metav1.Condition `json:"sourceconds"`
	// ProcessorStatuses contains a map of the Processor's spec names to their respective last observed statuses
	// +optional
	ProcessorStatuses map[string]*[]metav1.Condition `json:"processorconds"`
	// SinkStatuses contains a map of the Sink's spec names to their respective last observed statuses
	// +optional
	SinkStatuses map[string]*[]metav1.Condition `json:"sinkconds"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
// Topology is the Schema for the topology API
type Topology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TopologySpec   `json:"spec,omitempty"`
	Status TopologyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true
// TopologyList contains a list of Topology
type TopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Topology `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Topology{}, &TopologyList{})
}
