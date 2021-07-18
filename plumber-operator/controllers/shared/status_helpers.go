package shared

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

func IsStatusConditionPresentAndFullyEqual(oldConditions []metav1.Condition, newCondition metav1.Condition) bool {
	for _, condition := range oldConditions {
		if condition.Type == newCondition.Type {
			return condition.Status == newCondition.Status && condition.Reason == newCondition.Reason && condition.Message == newCondition.Message
		}
	}
	return false
}
