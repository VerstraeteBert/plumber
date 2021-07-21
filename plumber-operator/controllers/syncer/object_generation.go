package syncer

import (
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	"github.com/VerstraeteBert/plumber-operator/controllers/syncer/util"
	strimziv1beta1 "github.com/VerstraeteBert/plumber-operator/vendor-api/strimzi/v1beta1"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/autoscaling/v2beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"strings"
)

func getInt64Pointer(base int64) *int64 {
	val := base
	return &val
}

func getInt32Pointer(base int32) *int32 {
	val := base
	return &val
}

func generateOutputTopic(processor plumberv1alpha1.ComposedProcessor, pName string, activeRev plumberv1alpha1.TopologyRevision, topoName string) strimziv1beta1.KafkaTopic {
	desiredKfkTopic := strimziv1beta1.KafkaTopic{
		TypeMeta: metav1.TypeMeta{
			Kind:       "KafkaTopic",
			APIVersion: "kafka.strimzi.io/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			// TODO topicName is redundant
			Name:      shared.BuildOutputTopicName(activeRev.Namespace, topoName, pName, activeRev.Spec.Revision),
			Namespace: "plumber-kafka",
			Labels: map[string]string{
				"strimzi.io/cluster": "plumber-cluster",
			},
		},
		Spec: strimziv1beta1.KafkaTopicSpec{
			Partitions: processor.Internal.OutputTopic.Partitions,
			Replicas:   1,
			Config:     strimziv1beta1.KafkaTopicConfig{},
		},
	}
	return desiredKfkTopic
}

const (
	LabelProcessor = "plumber.ugent.be/processor-name"
)

func generateDeployment(pName string, processor plumberv1alpha1.ComposedProcessor, topoName string, topoRev plumberv1alpha1.TopologyRevision, sidecarConf SidecarConfig) appsv1.Deployment {
	jsonConfmap, _ := json.Marshal(sidecarConf)
	desiredDeployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      shared.BuildProcessorDeployName(topoName, pName, topoRev.Spec.Revision),
			Namespace: topoRev.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{LabelProcessor: shared.BuildProcessorDeployName(topoName, pName, topoRev.Spec.Revision)},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   pName,
					Labels: map[string]string{LabelProcessor: shared.BuildProcessorDeployName(topoName, pName, topoRev.Spec.Revision)},
				},
				Spec: corev1.PodSpec{
					// TODO readinessprobe / liveness probes ? both in SDK and Sidecar?
					Containers: []corev1.Container{
						{
							Name:  pName,
							Image: processor.Image,
							Env:   util.ConvertEnvVars(processor.Env),
						},
						{
							Name:  "plumber-sidecar",
							Image: "verstraetebert/plumber-sidecar:v0.0.1",
							Env: []corev1.EnvVar{
								{
									Name:  "PLUMBER_CONFIG",
									Value: string(jsonConfmap),
								},
							},
						},
					},
					TerminationGracePeriodSeconds: getInt64Pointer(0), // TODO use this for cleanup -> committing last offsets
				},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: "RollingUpdate",
			},
		},
	}
	return desiredDeployment
}

func generateScaledObject(processor plumberv1alpha1.ComposedProcessor, namespace string, pName string, topoName string, revNum int64, refs processorKafkaRefs) kedav1alpha1.ScaledObject {
	scaledObj := kedav1alpha1.ScaledObject{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kedav1alpha1.SchemeGroupVersion.String(),
			Kind:       "ScaledObject",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      shared.BuildScaledObjName(topoName, pName, revNum),
			Namespace: namespace,
		},
		Spec: kedav1alpha1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1alpha1.ScaleTarget{
				Kind: "Deployment",
				Name: shared.BuildProcessorDeployName(topoName, pName, revNum),
			},
			PollingInterval: getInt32Pointer(15),
			MinReplicaCount: getInt32Pointer(0),
			MaxReplicaCount: getInt32Pointer(int32(*processor.MaxScale)),
			// TODO reevaluate the HPA configs to not overscale!
			Advanced: &kedav1alpha1.AdvancedConfig{
				HorizontalPodAutoscalerConfig: &kedav1alpha1.HorizontalPodAutoscalerConfig{
					Behavior: &v2beta2.HorizontalPodAutoscalerBehavior{
						ScaleDown: &v2beta2.HPAScalingRules{
							StabilizationWindowSeconds: getInt32Pointer(100),
						},
					},
				},
				RestoreToOriginalReplicaCount: false, // once the scaledObject is deleted, respect the last observation
			},
			Triggers: []kedav1alpha1.ScaleTriggers{
				{
					Type: "kafka",
					Metadata: map[string]string{
						"topic":            refs.inputRef.topic,
						"bootstrapServers": strings.Join(refs.inputRef.bootstrapServers, ","),
						"consumerGroup":    refs.inputRef.consumerGroup,
						"lagThreshold":     "50",
					},
				},
			},
		},
	}

	return scaledObj
}
