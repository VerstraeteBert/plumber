package controllers

import (
	"github.com/VerstraeteBert/plumber-operator/controllers/domain"
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

func withPlumberPrefix(suffix string) string {
	return "plumber-" + suffix
}

func GetStrimziKafkaTopicName(outputTopicName string) string {
	return withPlumberPrefix(outputTopicName)
}

const (
	LabelProject   = "plumber-project"
	LabelManaged   = "plumber-managed"
	LabelProcessor = "plumber-processor"
)

func (r *TopologyReconciler) generateOutputTopic(projectName string, processor domain.Processor) strimziv1beta1.KafkaTopic {
	outputTopicName, _ := processor.GetOutputTopicName()
	desiredKfkTopic := strimziv1beta1.KafkaTopic{
		TypeMeta: metav1.TypeMeta{
			Kind:       "KafkaTopic",
			APIVersion: "kafka.strimzi.io/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      outputTopicName,
			Namespace: "plumber-kafka",
			Labels: map[string]string{
				LabelManaged:         "true",
				LabelProject:         projectName,
				"strimzi.io/cluster": "plumber-cluster",
			},
		},
		Spec: strimziv1beta1.KafkaTopicSpec{
			Partitions: processor.GetOutputTopicPartitions(),
			Replicas:   1,
			Config:     strimziv1beta1.KafkaTopicConfig{},
		},
	}

	return desiredKfkTopic
}

func GetDeploymentName(processorName string) string {
	return withPlumberPrefix(processorName)
}

func (r *TopologyReconciler) generatePlumberDeployment(processor domain.Processor, namespace string, projectName string) appsv1.Deployment {
	jsonConfmap, _ := json.Marshal(processor.GetSidecarConfig(projectName))
	desiredDeployment := appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      withPlumberPrefix(processor.Name),
			Namespace: namespace,
			Labels: map[string]string{
				LabelManaged: "true",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{LabelProcessor: processor.Name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   processor.Name,
					Labels: map[string]string{LabelProcessor: processor.Name},
				},
				Spec: corev1.PodSpec{
					// TODO readinessprobe / liveness probes ? both in SDK and Sidecar?
					Containers: []corev1.Container{
						{
							Name:  processor.Name,
							Image: processor.Image,
							Env:   processor.EnvVars,
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
				Type: "RollingUpdate", // TODO be able to set recreate ?
			},
		},
	}
	return desiredDeployment
}

func GetScalerName(processorName string) string {
	return withPlumberPrefix(processorName + "-scaler")
}

func (r *TopologyReconciler) generateScaledObject(processor domain.Processor, namespace string) kedav1alpha1.ScaledObject {
	scaledObj := kedav1alpha1.ScaledObject{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kedav1alpha1.SchemeGroupVersion.String(),
			Kind:       "ScaledObject",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetScalerName(processor.Name),
			Namespace: namespace,
			Labels: map[string]string{
				LabelManaged: "true",
			},
		},
		Spec: kedav1alpha1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1alpha1.ScaleTarget{
				Kind: "Deployment",
				Name: withPlumberPrefix(processor.Name),
			},
			PollingInterval: getInt32Pointer(15),
			MinReplicaCount: getInt32Pointer(0),
			MaxReplicaCount: getInt32Pointer(int32(processor.MaxReplicas)),
			// TODO reevaluate the HPA configs to not overscale initially!
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
						"topic":            processor.GetInputTopicName(),
						"bootstrapServers": strings.Join(processor.InputKafkaReference.GetBrokers(), ","),
						"consumerGroup":    processor.InputKafkaReference.GetConsumerGroup(),
						"lagThreshold":     "5",
					},
				},
			},
		},
	}

	return scaledObj
}
