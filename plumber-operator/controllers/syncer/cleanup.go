package syncer

import (
	"context"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	"github.com/VerstraeteBert/plumber-operator/controllers/syncer/domain"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func buildNecessaryObjsSet(domainTopo *domain.Topology) *shared.Set {
	set := shared.NewSet()
	for _, processor := range domainTopo.Processors {
		set.Add(GetDeploymentName(processor.Name))
		set.Add(GetScalerName(processor.Name))
		if processor.HasOutputTopic() {
			outputTopicName, _ := processor.GetOutputTopicName()
			set.Add(GetStrimziKafkaTopicName(outputTopicName))
		}
	}
	return set
}

func (r *TopologyReconciler) cleanup(namespace string, filter *shared.Set) error {
	// gets all the current running objects pertaining the the current reconciled Topology and deletes them.
	// Currently, only deployments are created, topics are kept indefinitely
	listOpts := []client.ListOption{
		client.MatchingLabels{LabelManaged: "true"},
		client.InNamespace(namespace),
	}
	// every object created by the controller has a unique name
	deployList := appsv1.DeploymentList{}
	err := r.Client.List(context.TODO(), &deployList, listOpts...)
	if err != nil {
		return err
	}
	for _, dep := range deployList.Items {
		if filter.Contains(dep.Name) {
			continue
		}
		err := r.Client.Delete(context.TODO(), &dep)
		if err != nil {
			// if not present anymore, its deleted or being deleted, just wait
			if !errors.IsNotFound(err) {
				return err
			}
		}
	}
	scaledObjlist := kedav1alpha1.ScaledObjectList{}
	err = r.Client.List(context.TODO(), &scaledObjlist, listOpts...)
	if err != nil {
		return err
	}
	for _, scaledObj := range scaledObjlist.Items {
		if filter.Contains(scaledObj.Name) {
			continue
		}
		err := r.Client.Delete(context.TODO(), &scaledObj)
		if err != nil {
			// if not present anymore, its deleted or being deleted, just wait
			if !errors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
