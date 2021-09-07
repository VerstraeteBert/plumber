package syncer

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	strimziv1beta1 "github.com/VerstraeteBert/plumber-operator/vendor-api/strimzi/v1beta1"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type syncerHandler struct {
	cClient        client.Client
	activeRevision plumberv1alpha1.TopologyRevision
	topology       plumberv1alpha1.Topology
	Log            logr.Logger
	Scheme         *runtime.Scheme
}

const (
	hashLabel string = "plumber.ugent.be/hash"
)

func hashObject(object interface{}) string {
	hf := fnv.New32()
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}
	_, _ = printer.Fprintf(hf, "%#v", object)
	return fmt.Sprint(hf.Sum32())
}

func (sh *syncerHandler) reconcileProcessors() error {
	// Generate and apply the desired for a given active Revision
	// Important to note that if a nextRevision is set, the immediate dominators processors of a source are not reconciled (except for theeoutput topic)
	defer shared.Elapsed(sh.Log, "Patching topo components")()
	// Mark this controller as field owner on server side applies
	// 1. first, create all output topics
	hadToPatch := false
	hadToDelete := false
	applyOpts := []client.PatchOption{client.FieldOwner("plumber-syncer"), client.ForceOwnership}
	for pName, proc := range sh.activeRevision.Spec.Processors {
		if proc.HasOutputTopic() {
			desiredKfkTopic := sh.generateOutputTopic(pName, proc)
			var currentKfkTopic strimziv1beta1.KafkaTopic
			{
			}
			err := sh.cClient.Get(context.TODO(), types.NamespacedName{
				Namespace: desiredKfkTopic.Namespace,
				Name:      desiredKfkTopic.Name,
			}, &currentKfkTopic)

			shouldPatch := false
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
				shouldPatch = desiredKfkTopic.Spec.Partitions > currentKfkTopic.Spec.Partitions
			}
			if shouldPatch {
				err = sh.cClient.Patch(context.TODO(), &desiredKfkTopic, client.Apply, applyOpts...)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("failed to patch output topic for processor %s", pName))
				}
			}
		}
	}
	for pName, proc := range sh.activeRevision.Spec.Processors {
		// generate scaledobject / deployment per processor
		// important: if a NextRevision is set, any deployment/scaledobject of a processor connected to a source must be deleted to prepare for phase-out
		if _, takesInputFromSource := sh.activeRevision.Spec.Sources[proc.InputFrom]; takesInputFromSource && sh.topology.Status.NextRevision != nil {
			var deployToDelete appsv1.Deployment
			err := sh.cClient.Get(context.TODO(), client.ObjectKey{
				Namespace: sh.topology.GetNamespace(),
				Name:      shared.BuildProcessorDeployName(sh.topology.GetName(), pName, sh.activeRevision.Spec.Revision),
			}, &deployToDelete)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to fetch deployment of source connected processor")
				}
			} else {
				err := sh.cClient.Delete(context.TODO(), &deployToDelete)
				if err != nil && !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to delete deployment of source connected processor")
				}
			}
			var scaledObjToDelete kedav1alpha1.ScaledObject
			err = sh.cClient.Get(context.TODO(), client.ObjectKey{
				Namespace: sh.topology.GetNamespace(),
				Name:      shared.BuildScaledObjName(sh.topology.GetName(), pName, sh.activeRevision.Spec.Revision),
			}, &scaledObjToDelete)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to fetch scaledobject of source connected processor")
				}
			} else {
				err := sh.cClient.Delete(context.TODO(), &scaledObjToDelete)
				if err != nil && !kerrors.IsNotFound(err) {
					return errors.Wrap(err, "failed to delete scaledobject of source connected processor")
				}
			}
		} else {
			procKRefs := sh.buildProcessorKafkaRefs(pName, proc)
			sidecarConfig := sh.buildSidecarConfig(pName, procKRefs)
			desiredDeployment := sh.generateDeployment(pName, proc, sidecarConfig)
			// safe to ignore the error
			_ = ctrl.SetControllerReference(&sh.activeRevision, &desiredDeployment, sh.Scheme)
			// TODO pull some of the determination code if a patch is needed into seperate functions
			//		+ shared package for hashing specific methods: i.e. setHashLabel, getHashLabel, GenerateHashFromObject
			depHash := hashObject(desiredDeployment)
			desiredDeployment.Labels[hashLabel] = depHash
			shouldPatch := false
			var currDeploy appsv1.Deployment
			err := sh.cClient.Get(context.TODO(), client.ObjectKeyFromObject(&desiredDeployment), &currDeploy)
			if err != nil {
				// not found & any other error
				shouldPatch = true
			} else {
				if currHash, exists := currDeploy.GetLabels()[hashLabel]; exists {
					if currHash != depHash {
						shouldPatch = true
					}
				} else {
					shouldPatch = true
				}
			}
			if shouldPatch {
				err = sh.cClient.Patch(context.TODO(), &desiredDeployment, client.Apply, applyOpts...)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("failed to patch deployment for processor %s", pName))
				}
				hadToPatch = true
			}

			// reconcile scaled object (keda)
			shouldPatch = false
			desiredScaledObject := sh.generateScaledObject(pName, proc, procKRefs)
			_ = ctrl.SetControllerReference(&sh.activeRevision, &desiredScaledObject, sh.Scheme)
			scaledObjHash := hashObject(desiredScaledObject)
			desiredScaledObject.Labels[hashLabel] = scaledObjHash
			var currScaledObject kedav1alpha1.ScaledObject
			err = sh.cClient.Get(context.TODO(), client.ObjectKeyFromObject(&desiredScaledObject), &currScaledObject)
			if err != nil {
				shouldPatch = true
			} else {
				if currHash, exists := currScaledObject.GetLabels()[hashLabel]; exists {
					if currHash != scaledObjHash {
						shouldPatch = true
					}
				} else {
					shouldPatch = true
				}
			}
			if shouldPatch {
				err = sh.cClient.Patch(context.TODO(), &desiredScaledObject, client.Apply, applyOpts...)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("failed to update scaled object for processor %s", pName))
				}
			}
		}
		if hadToPatch {
			sh.Log.Info(fmt.Sprintf("####%s,%s,%s,%s", sh.topology.GetName(), "activeObjectsCreated", strconv.FormatInt(*sh.topology.Status.ActiveRevision, 10), strconv.FormatInt(time.Now().UnixNano(), 10)))
		}
		if hadToDelete {
			sh.Log.Info(fmt.Sprintf("####%s,%s,%s,%s", sh.topology.GetName(), "sourceConnectedDeleted", strconv.FormatInt(*sh.topology.Status.ActiveRevision, 10), strconv.FormatInt(time.Now().UnixNano(), 10)))
		}
	}
	return nil
}
