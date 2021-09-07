package updater

import (
	"context"
	"fmt"
	"strconv"
	"time"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Updater struct {
	cClient  client.Client
	Log      logr.Logger
	topology *plumberv1alpha1.Topology
	scheme   *runtime.Scheme
	uClient  client.Client
}

func (u *Updater) handleTopoDeleted() (reconcile.Result, error) {
	revs, err := u.listTopologyRevisions()
	if err != nil {
		return reconcile.Result{}, err
	}
	err = u.pruneRevisions(revs, false)
	if err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (u *Updater) handleTopoExists() (reconcile.Result, error) {
	defer shared.Elapsed(u.Log, "updater loop")()
	revisionHistory, err := u.listTopologyRevisions()
	if err != nil {
		u.Log.Error(err, "failed to list topology revisions")
		return reconcile.Result{}, err
	}
	err = u.pruneRevisions(revisionHistory, true)
	if err != nil {
		return reconcile.Result{}, err
	}
	nextRevNum := getNextRevisionNumber(revisionHistory)
	u.Log.Info(fmt.Sprintf("new revision number: %d", nextRevNum))
	newRevision, err := u.revisionFromTopologyWithDefaults(nextRevNum)
	if err != nil {
		return reconcile.Result{}, err
	}
	// ActiveRevision & NextRevision are pointers
	isActiveRevisionSet := u.topology.Status.ActiveRevision != nil
	isNextRevisionSet := u.topology.Status.NextRevision != nil
	var activeRevision plumberv1alpha1.TopologyRevision
	var nextRevision plumberv1alpha1.TopologyRevision
	if isActiveRevisionSet {
		u.Log.Info("active revision set")
		activeRevision, isActiveRevisionSet = revisionHistory[*u.topology.Status.ActiveRevision]
	}
	if isNextRevisionSet {
		u.Log.Info("next revision set")
		nextRevision, isNextRevisionSet = revisionHistory[*u.topology.Status.NextRevision]
	}
	if isNextRevisionSet {
		// next revision set, active unknown
		if isActiveRevisionSet {
			// active and next set
			// a. if new == next
			//	1. check postdom deletion
			//		if not deleted
			//			requeue after 10
			//		else
			//		 phasingout += active, active = next, next = nil
			//		 done
			// b. else
			//   1. calculate CGs keeping into account the active revision
			// 	 2. persist new revision
			//   3. set new revision to be next
			//   4. requeue immediately
			if newRevision.SemanticallyEqual(nextRevision) {
				readyForPhaseOut, err := u.checkActiveRevisionReadyForPhaseOut(activeRevision)
				if err != nil {
					return reconcile.Result{}, errors.Wrap(err, "failed to check if active revision was ready for phase out")
				}
				if !readyForPhaseOut {
					u.Log.Info("syncer has not yet completed phasing out preparation, requeuing")
					return reconcile.Result{
						RequeueAfter: time.Second * 5,
					}, nil
				}
				currentTopo := u.topology.DeepCopy()
				currentTopo.Status.PhasingOutRevisions = append(currentTopo.Status.PhasingOutRevisions, *currentTopo.Status.ActiveRevision)
				currentTopo.Status.ActiveRevision = currentTopo.Status.NextRevision
				currentTopo.Status.NextRevision = nil
				err = u.cClient.Status().Patch(context.TODO(), currentTopo, client.MergeFrom(u.topology), &client.PatchOptions{FieldManager: FieldManager})
				if err != nil {
					if kerrors.IsNotFound(err) {
						// topology object deleted while reconciling, ignore
						return reconcile.Result{}, nil
					} else {
						return reconcile.Result{}, errors.Wrap(err, "failed to patch topology status with new activeRevision when next set and active not set")
					}
				}
				u.Log.Info(fmt.Sprintf("####%s,%s,%s,%s", currentTopo.GetName(), "markedActive", strconv.FormatInt(*currentTopo.Status.ActiveRevision, 10), strconv.FormatInt(time.Now().UnixNano(), 10)))
				return reconcile.Result{}, nil
			}
			propagateCGs(&newRevision, activeRevision)
			// TODO check if last persisted revision == new, skip persisting otherwise
			err := u.persistTopologyRevision(newRevision)
			if err != nil {
				return reconcile.Result{}, err
			}
			currentTopo := u.topology.DeepCopy()
			currentTopo.Status.NextRevision = &newRevision.Spec.Revision
			err = u.cClient.Status().Patch(context.TODO(), currentTopo, client.MergeFrom(u.topology), &client.PatchOptions{FieldManager: FieldManager})
			if err != nil {
				if kerrors.IsNotFound(err) {
					// topology object deleted while reconciling, ignore
					return reconcile.Result{}, nil
				} else {
					return reconcile.Result{}, errors.Wrap(err, "failed to patch topology status with new activeRevision when next set and active not set")
				}
			}
			return reconcile.Result{}, nil
		} else {
			// next set, active not set
			// if next == new:
			//	transition next -> active
			// else:
			//	 calculate CGs (default)
			//   persist new revision
			//   unset next, set new to active
			//   done
			if newRevision.SemanticallyEqual(nextRevision) {
				currentTopo := u.topology.DeepCopy()
				currentTopo.Status.ActiveRevision = currentTopo.Status.NextRevision
				currentTopo.Status.NextRevision = nil
				err = u.cClient.Status().Patch(context.TODO(), currentTopo, client.MergeFrom(u.topology), &client.PatchOptions{FieldManager: FieldManager})
				if err != nil {
					if kerrors.IsNotFound(err) {
						// topology object deleted while reconciling, ignore
						return reconcile.Result{}, nil
					} else {
						return reconcile.Result{}, errors.Wrap(err, "failed to patch topology status with new activeRevision when next set and active not set")
					}
				}
				return reconcile.Result{}, nil
			}
			// TODO check if last persisted revision == new, skip persisting otherwise
			err := u.persistTopologyRevision(newRevision)
			if err != nil {
				return reconcile.Result{}, err
			}
			currentTopo := u.topology.DeepCopy()
			currentTopo.Status.ActiveRevision = &newRevision.Spec.Revision
			currentTopo.Status.NextRevision = nil
			err = u.cClient.Status().Patch(context.TODO(), currentTopo, client.MergeFrom(u.topology), &client.PatchOptions{FieldManager: FieldManager})
			if err != nil {
				if kerrors.IsNotFound(err) {
					// topology object deleted while reconciling, ignore
					return reconcile.Result{}, nil
				} else {
					return reconcile.Result{}, errors.Wrap(err, "failed to patch topology status with new activeRevision when next set and active not set")
				}
			}
			return reconcile.Result{}, nil
		}
	} else {
		// next revision not set, active unknown
		if isActiveRevisionSet {
			// active revision set, next not set
			//  1.if new == active
			//        done
			//    else
			//	2. calculate CGS taking into account active revision
			//  2. persist new revision
			//  3. set new revision to be next
			//  4. requeue after 10 -> branch where active and next are set will be taken
			if newRevision.SemanticallyEqual(activeRevision) {
				u.Log.Info("revisions are equal!")
				return reconcile.Result{}, nil
			}
			propagateCGs(&newRevision, activeRevision)
			// TODO check if last persisted revision == new, skip persisting otherwise
			err := u.persistTopologyRevision(newRevision)
			if err != nil {
				return reconcile.Result{}, err
			}
			currentTopo := u.topology.DeepCopy()
			currentTopo.Status.NextRevision = &newRevision.Spec.Revision
			err = u.cClient.Status().Patch(context.TODO(), currentTopo, client.MergeFrom(u.topology), &client.PatchOptions{FieldManager: FieldManager})
			if err != nil {
				if kerrors.IsNotFound(err) {
					// topology object deleted while reconciling, ignore
					return reconcile.Result{}, nil
				} else {
					return reconcile.Result{}, errors.Wrap(err, "failed to patch topology status with new activeRevision when active set, next not set")
				}
			}
			u.Log.Info(fmt.Sprintf("####%s,%s,%s,%s", currentTopo.GetName(), "markedNext", strconv.FormatInt(*currentTopo.Status.NextRevision, 10), strconv.FormatInt(time.Now().UnixNano(), 10)))
			return reconcile.Result{
				RequeueAfter: time.Second * 5,
			}, nil
		} else {
			// active and next unset
			// 1. determine default CGs for each processor
			// 2. persist new revision
			// 3. set new revision as active
			// TODO check if last persisted revision == new, skip persisting otherwise
			err := u.persistTopologyRevision(newRevision)
			if err != nil {
				return reconcile.Result{}, err
			}
			currentTopo := u.topology.DeepCopy()
			currentTopo.Status.ActiveRevision = &newRevision.Spec.Revision
			err = u.cClient.Status().Patch(context.TODO(), currentTopo, client.MergeFrom(u.topology), &client.PatchOptions{FieldManager: FieldManager})
			if err != nil {
				if kerrors.IsNotFound(err) {
					// topology object deleted while reconciling, ignore
					return reconcile.Result{}, nil
				} else {
					return reconcile.Result{}, errors.Wrap(err, "failed to patch topology status with new activeRevision when active and next were unset")
				}
			}
			return reconcile.Result{}, nil
		}
	}
}

// checkActiveRevisionReadyForPhaseOut checks if a revision is ready for phasing out
// this condition is met if all deployments & scaledobjects of these processors are deleted
func (u *Updater) checkActiveRevisionReadyForPhaseOut(activeRevision plumberv1alpha1.TopologyRevision) (bool, error) {
	for pName := range activeRevision.Spec.Processors {
		deployDeleted := false
		var pDeploy appsv1.Deployment
		err := u.uClient.Get(
			context.TODO(),
			client.ObjectKey{
				Namespace: activeRevision.Namespace,
				Name:      shared.BuildProcessorDeployName(u.topology.Name, pName, activeRevision.Spec.Revision),
			},
			&pDeploy,
		)
		if err != nil {
			if kerrors.IsNotFound(err) {
				deployDeleted = true
			} else {
				return false, errors.Wrap(err, "failed to get deployment when checking phaseout readiness")
			}
		}
		if !deployDeleted {
			return false, nil
		}
		var pScaledObj kedav1alpha1.ScaledObject
		err = u.uClient.Get(
			context.TODO(),
			client.ObjectKey{
				Namespace: activeRevision.Namespace,
				Name:      shared.BuildScaledObjName(u.topology.Name, pName, activeRevision.Spec.Revision),
			},
			&pScaledObj,
		)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return true, nil
			} else {
				return false, errors.Wrap(err, "failed to get scaledobject when checking phaseout readiness")
			}
		} else {
			// scaledobj found; possibly finalizing -> requeue
			return false, nil
		}
	}
	return true, nil
}

func propagateCGs(newRevision *plumberv1alpha1.TopologyRevision, activeRevision plumberv1alpha1.TopologyRevision) {
	for nProcessorName, nProcessor := range newRevision.Spec.Processors {
		// if supplied initialOffset != continue -> already set correctly (earliest/latest during building of revision or defaulted)
		if nProcessor.InitialOffset != shared.OffsetContinue && nProcessor.InitialOffset != "" {
			continue
		}
		// if both in active and new revision, the same processor (name) refers to the same source (name) -> take over CGs
		if _, nIsSourceRef := newRevision.Spec.Sources[nProcessor.InputFrom]; nIsSourceRef {
			if aProcessor, aProcessorExists := activeRevision.Spec.Processors[nProcessorName]; aProcessorExists {
				if _, aIsSourceRef := activeRevision.Spec.Sources[aProcessor.InputFrom]; aIsSourceRef {
					// take over CG
					nProcessor.Internal.ConsumerGroup = aProcessor.Internal.ConsumerGroup
					newRevision.Spec.Processors[nProcessorName] = nProcessor
				}
			}
		}
	}
}

// revisionFromTopology creates a new Revision object with a given revisionNumber; it applies default consumer groups
func (u *Updater) revisionFromTopologyWithDefaults(revisionNumber int64) (plumberv1alpha1.TopologyRevision, error) {
	revSpec := plumberv1alpha1.TopologyRevisionSpec{
		Sources:    make(map[string]plumberv1alpha1.Source),
		Sinks:      make(map[string]plumberv1alpha1.Sink),
		Processors: make(map[string]plumberv1alpha1.ComposedProcessor),
		Revision:   revisionNumber,
	}

	// combine all parts into a single revision (source/sinks as is, processors need some extra generated details)
	allProcessors := make(map[string]plumberv1alpha1.Processor)
	for _, topoPartRef := range u.topology.Spec.Parts {
		var topoPartControllerRev appsv1.ControllerRevision
		err := u.cClient.Get(context.TODO(),
			client.ObjectKey{
				Namespace: u.topology.Namespace,
				Name:      shared.BuildTopoPartRevisionName(topoPartRef.Name, topoPartRef.Revision),
			},
			&topoPartControllerRev,
		)
		if err != nil {
			// TODO isnotfound handling
			return plumberv1alpha1.TopologyRevision{}, errors.Wrap(err, "failed to get topologyparts objects while creating revision from topology")
		}
		var topoPart plumberv1alpha1.TopologyPart
		_, _, _ = unstructured.UnstructuredJSONScheme.Decode(topoPartControllerRev.Data.Raw, &schema.GroupVersionKind{
			Group:   "plumber.ugent.be",
			Version: "v1alpha1",
			Kind:    "TopologyPart",
		}, &topoPart)

		for name, source := range topoPart.Spec.Sources {
			revSpec.Sources[name] = source
		}
		for name, sink := range topoPart.Spec.Sinks {
			revSpec.Sinks[name] = sink
		}
		for name, processor := range topoPart.Spec.Processors {
			allProcessors[name] = processor
		}
	}

	// InternalProcDetails
	// initialOffset
	//		default: if connected to processor -> earliest
	//				 if connected to source -> respect supplied initialOffset value if it is not set or "continue" -> latest
	// consumerGroup
	//		set default here
	//		later pass which takes into account the active revision may overwrite this if initialOffset == "continue"
	// outputTopic:
	//		first detect if it is needed: if any processor takes input from it
	//				if so: identify largest MaxScale of an immediate successor processor as the number of partitions (using connectedProcessorScales as a helper datastructure)
	// helper structure to determine partitions needed for outputTopics
	connectedProcessorsMaxScale := make(map[string]int)
	for name, proc := range allProcessors {
		initialOffset := proc.InitialOffset
		if _, takesInputFromSource := revSpec.Sources[proc.InputFrom]; takesInputFromSource {
			if proc.InitialOffset == "" || proc.InitialOffset == shared.OffsetContinue {
				initialOffset = shared.OffsetLatest
			}
		} else {
			// takes input from processor
			initialOffset = shared.OffsetEarliest
			// partition calcs
			procMaxScale := proc.GetMaxScaleOrDefault()

			if currMaxMaxScale, found := connectedProcessorsMaxScale[proc.InputFrom]; found {
				connectedProcessorsMaxScale[proc.InputFrom] = shared.MaxInt(currMaxMaxScale, procMaxScale)
			} else {
				connectedProcessorsMaxScale[proc.InputFrom] = procMaxScale
			}
		}
		revSpec.Processors[name] = plumberv1alpha1.ComposedProcessor{
			InputFrom:     proc.InputFrom,
			Image:         proc.Image,
			MaxScale:      proc.MaxScale,
			Env:           proc.Env,
			SinkBindings:  proc.SinkBindings,
			InitialOffset: proc.InitialOffset,
			Internal: plumberv1alpha1.InternalProcDetails{
				ConsumerGroup: u.topology.Namespace + "-" + u.topology.Name + "-" + name + "-" + strconv.FormatInt(revisionNumber, 10),
				InitialOffset: initialOffset,
			},
		}
	}
	for pName, reqOutPartitions := range connectedProcessorsMaxScale {
		procObj := revSpec.Processors[pName]
		procObj.Internal.OutputTopic = &plumberv1alpha1.InternalTopic{
			Partitions: reqOutPartitions,
		}
		revSpec.Processors[pName] = procObj
	}
	return plumberv1alpha1.TopologyRevision{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TopologyRevision",
			APIVersion: plumberv1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      shared.BuildTopoRevisionName(u.topology.Name, revisionNumber),
			Namespace: u.topology.Namespace,
			Labels: map[string]string{
				shared.ManagedByLabel:      u.topology.GetName(),
				shared.RevisionNumberLabel: strconv.FormatInt(revisionNumber, 10),
			},
		},
		Spec:   revSpec,
		Status: plumberv1alpha1.TopologyRevisionStatus{},
	}, nil
}
