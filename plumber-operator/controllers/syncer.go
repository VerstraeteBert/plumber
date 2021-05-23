package controllers

import (
	"context"
	"fmt"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/domain"
	"github.com/VerstraeteBert/plumber-operator/controllers/util"
	strimziv1beta1 "github.com/VerstraeteBert/plumber-operator/vendor-api/strimzi/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *TopologyReconciler) reconcileProcessors(crdTopo *plumberv1alpha1.Topology, domainTopo *domain.Topology) error {
	// Generate and apply the desired objects
	// 1. Output topic
	// 2. Deployment with sidecar
	// 3. ScaledObject
	// Mark this controller as field owner on server side applies
	defer util.Elapsed(r.Log, "Patching topo components")()

	applyOpts := []client.PatchOption{client.FieldOwner("topology-controller"), client.ForceOwnership}
	for _, processor := range domainTopo.Processors {
		// generate output topic
		var err error
		if processor.HasOutputTopic() {
			desiredKfkTopic := r.generateOutputTopic(domainTopo.ProjectName, processor)
			currentKfkTopic := strimziv1beta1.KafkaTopic{}

			var shouldPatch bool
			err = r.Client.Get(context.TODO(), types.NamespacedName{
				Namespace: desiredKfkTopic.Namespace,
				Name:      desiredKfkTopic.Name,
			}, &currentKfkTopic)

			if err != nil {
				if !errors.IsNotFound(err) {
					return err
				}
				// not found
				shouldPatch = true
			} else {
				// topic already exists
				// ! number partitions is the only variable spec value currently.
				// check if # of desired partitions is lower than current num of partitions.
				// 		if desired < current -> dont change it
				// https://stackoverflow.com/questions/45497878/how-to-decrease-number-partitions-kafka-topic#:~:text=Apache%20Kafka%20doesn't%20support,of%20them%20means%20data%20loss.
				// a method for lowering partitions is under proposal as of now though: https://cwiki.apache.org/confluence/display/KAFKA/KIP-694%3A+Support+Reducing+Partitions+for+Topics
				// probably still a no-go to support it, for partition ordering, keys sake (if we ever get to implementing keyed messaging)
				shouldPatch = desiredKfkTopic.Spec.Partitions >= currentKfkTopic.Spec.Partitions
			}

			if shouldPatch {
				err = r.Patch(context.TODO(), &desiredKfkTopic, client.Apply, applyOpts...)
				if err != nil {
					return fmt.Errorf("failed to patch output topic for processor %s because: %s", processor.Name, err.Error())
				}
			}
		}

		// generate deployment (sidecar + user code)
		deployment := r.generatePlumberDeployment(processor, crdTopo.Namespace, domainTopo.ProjectName)
		// safe to ignore the error
		err = ctrl.SetControllerReference(crdTopo, &deployment, r.Scheme)
		if err != nil {
			r.Log.Error(err, err.Error())
			return fmt.Errorf("failed to set ownerreference for deployment %s", deployment.Name)
		}
		err = r.Patch(context.TODO(), &deployment, client.Apply, applyOpts...)
		if err != nil {
			r.Log.Error(err, err.Error())
			return fmt.Errorf("failed to patch deployment for processor %s", processor.Name)
		}

		// reconcile scaled object (keda)
		scaledObject := r.generateScaledObject(processor, crdTopo.Namespace)
		err = ctrl.SetControllerReference(crdTopo, &scaledObject, r.Scheme)
		if err != nil {
			r.Log.Error(err, err.Error())
			return fmt.Errorf("failed to set ownerreference for scaledObject %s", scaledObject.Name)
		}
		err = r.Patch(context.TODO(), &scaledObject, client.Apply, applyOpts...)
		if err != nil {
			r.Log.Error(err, err.Error())
			return fmt.Errorf("failed to update scaled object for processor %s", processor.Name)
		}
	}
	return nil
}
