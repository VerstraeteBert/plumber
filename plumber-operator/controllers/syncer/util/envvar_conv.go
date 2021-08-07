package util

import (
	"github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
)

func ConvertEnvVars(envVars []v1alpha1.EnvVar) []v1.EnvVar {
	converted := make([]v1.EnvVar, len(envVars))
	for i, v := range envVars {
		converted[i] = v1.EnvVar{
			Name:  v.Name,
			Value: v.Value,
		}
	}
	return converted
}
