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
	// +optional
	DefaultScale *int  `json:"defaultScale,omitempty"`
	Revision     int64 `json:"revision"`
}

type InternalTopic struct {
	Partitions int `json:"partitions"`
}

type InternalProcDetails struct {
	// +kubebuilder:validation:Enum=Earliest;Latest
	InitialOffset string `json:"initialOffset"`
	ConsumerGroup string `json:"consumerGroup"`
	// +optional
	OutputTopic *InternalTopic `json:"outputTopic,omitempty"`
}

type ComposedProcessor struct {
	InputFrom string `json:"inputFrom"`
	Image     string `json:"image"`
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	MaxScale *int `json:"maxScale,omitempty"`
	// +optional
	Env []EnvVar `json:"env,omitempty"`
	// +optional
	SinkBindings string `json:"sinkBindings,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=Earliest;Latest;Continue
	InitialOffset string              `json:"initialOffset,omitempty"`
	Internal      InternalProcDetails `json:"internal"`
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
		p.Internal = InternalProcDetails{}
		revACopy.Processors[k] = p
	}
	for k, p := range revBCopy.Processors {
		p.Internal = InternalProcDetails{}
		revACopy.Processors[k] = p
	}
	revACopy.Revision = 0
	revBCopy.Revision = 0
	return reflect.DeepEqual(revACopy, revBCopy)
}

func (cp *ComposedProcessor) HasOutputTopic() bool {
	return cp.Internal.OutputTopic != nil
}

func (cp *ComposedProcessor) getMaxScaleOrDefault() int {
	if cp.MaxScale == nil {
		return 5
	}
	return *cp.MaxScale
}

func init() {
	SchemeBuilder.Register(&TopologyRevision{}, &TopologyRevisionList{})
}
