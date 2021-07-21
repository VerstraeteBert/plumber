package syncer

import (
	"context"
	"fmt"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	strimziv1beta1 "github.com/VerstraeteBert/plumber-operator/vendor-api/strimzi/v1beta1"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *TopologyReconciler) reconcileProcessors(topo plumberv1alpha1.Topology, activeRev plumberv1alpha1.TopologyRevision) error {
	// Generate and apply the desired for a given active Revision
	// Important to note that if a nextRevision is set, the immediate dominators processors of a source are not reconciled (except for theeoutput topic)
	defer shared.Elapsed(r.Log, "Patching topo components")()
	// Mark this controller as field owner on server side applies
	// 1. first, create all output topics
	applyOpts := []client.PatchOption{client.FieldOwner("plumber-syncer"), client.ForceOwnership}
	for pName, proc := range activeRev.Spec.Processors {
		if proc.HasOutputTopic() {
			desiredKfkTopic := generateOutputTopic(proc, pName, activeRev, topo.Name)
			var currentKfkTopic strimziv1beta1.KafkaTopic
			{
			}
			err := r.Client.Get(context.TODO(), types.NamespacedName{
				Namespace: desiredKfkTopic.Namespace,
				Name:      desiredKfkTopic.Name,
			}, &currentKfkTopic)

			var shouldPatch bool
			if err != nil {
				if !kerrors.IsNotFound(err) {
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
					return errors.Wrap(err, fmt.Sprintf("failed to patch output topic for processor %s", pName))
				}
			}
		}
	}
	for pName, proc := range activeRev.Spec.Processors {
		// generate scaledobject / deployment per processor
		// important: if a NextRevision is set, any deployment/scaledobject of a processor connected to a source must be deleted to prepare for phase-out
		if topo.Status.NextRevision == nil {
			r.Log.Info("Next revision is nil")
		}
		if _, takesInputFromSource := activeRev.Spec.Sources[proc.InputFrom]; takesInputFromSource && topo.Status.NextRevision != nil {
			r.Log.Info(fmt.Sprintf("Next revision is %d", *topo.Status.NextRevision))
			var deployToDelete appsv1.Deployment
			err := r.Client.Get(context.TODO(), client.ObjectKey{
				Namespace: topo.GetNamespace(),
				Name:      shared.BuildProcessorDeployName(topo.GetName(), pName, activeRev.Spec.Revision),
			}, &deployToDelete)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to fetch deployment of source connected processor")
				}
			} else {
				r.Log.Info(fmt.Sprintf("Attempting to delete %s", shared.BuildProcessorDeployName(topo.GetName(), pName, activeRev.Spec.Revision)))
				err := r.Client.Delete(context.TODO(), &deployToDelete)
				if err != nil && !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to delete deployment of source connected processor")
				}
			}
			var scaledObjToDelete kedav1alpha1.ScaledObject
			err = r.Client.Get(context.TODO(), client.ObjectKey{
				Namespace: topo.GetNamespace(),
				Name:      shared.BuildScaledObjName(topo.GetName(), pName, activeRev.Spec.Revision),
			}, &scaledObjToDelete)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to fetch scaledobject of source connected processor")
				}
			} else {
				r.Log.Info(fmt.Sprintf("Attempting to delete %s", shared.BuildScaledObjName(topo.GetName(), pName, activeRev.Spec.Revision)))
				err := r.Client.Delete(context.TODO(), &scaledObjToDelete)
				if err != nil && !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to delete scaledobject of source connected processor")
				}
			}
		} else {
			procKRefs := buildProcessorKafkaRefs(pName, proc, activeRev, topo.Name)
			sidecarConfig := buildSidecarConfig(pName, procKRefs, activeRev)
			deployment := generateDeployment(pName, proc, topo.Name, activeRev, sidecarConfig)
			// safe to ignore the error
			err := ctrl.SetControllerReference(&activeRev, &deployment, r.Scheme)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to set ownerreference for deployment %s", deployment.Name))
			}
			err = r.Patch(context.TODO(), &deployment, client.Apply, applyOpts...)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to patch deployment for processor %s", pName))
			}
			// reconcile scaled object (keda)
			scaledObject := generateScaledObject(proc, topo.Namespace, pName, topo.Name, activeRev.Spec.Revision, procKRefs)
			err = ctrl.SetControllerReference(&activeRev, &scaledObject, r.Scheme)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to set ownerreference for scaledObject %s", scaledObject.Name))
			}
			err = r.Patch(context.TODO(), &scaledObject, client.Apply, applyOpts...)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to update scaled object for processor %s", pName))
			}
		}
	}
	return nil
}
