package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
)

// TopologyRevisionSpec defines the desired state of Topology at a certain point, in use for TopologyRevisions
type TopologyRevisionSpec struct {
	// +optional
	Sources map[string]Source `json:"sources,omitempty"`
	// +optional
	Sinks map[string]Sink `json:"sinks,omitempty"`
	// +optional
	Processors map[string]ComposedProcessor `json:"processors,omitempty"`
	Revision   int64
}

type ComposedProcessor struct {
	InputFrom     string `json:"inputFrom"`
	Image         string `json:"image"`
	ConsumerGroup string `json:"consumerGroup"`
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	MaxScale int `json:"maxScale"`
	// +optional
	Env []EnvVar `json:"env,omitempty"`
	// +optional
	SinkBindings string `json:"sinkBindings,omitempty"`
}

// TopologyRevisionStatus defines the observed state of a TopologyRevision, currently not used anywhere
type TopologyRevisionStatus struct{}

// +kubebuilder:object:root=true
// TopologyRevision is the Schema for the TopologyRevisions API
type TopologyRevision struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TopologyRevisionSpec   `json:"spec,omitempty"`
	Status TopologyRevisionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// TopologyPartList contains a list of TopologyParts
type TopologyRevisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TopologyRevision `json:"items"`
}

// EqualRevisionSpecsSemantic checks if two revision specs are semantically deeply equal; i.e. only their consumer groups may differ
func (a *TopologyRevision) EqualRevisionSpecsSemantic(b *TopologyRevision) bool {
	revACopy := a.Spec.DeepCopy()
	revBCopy := b.Spec.DeepCopy()
	for k, p := range revACopy.Processors {
		p.ConsumerGroup = ""
		revACopy.Processors[k] = p
	}
	for k, p := range revBCopy.Processors {
		p.ConsumerGroup = ""
		revACopy.Processors[k] = p
	}
	revACopy.Revision = 0
	revBCopy.Revision = 0
	return reflect.DeepEqual(revACopy, revBCopy)
}

func init() {
	SchemeBuilder.Register(&TopologyRevision{}, &TopologyRevisionList{})
}