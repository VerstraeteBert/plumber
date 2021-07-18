package updater

import (
	"context"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"time"
)

type Updater struct {
	cClient  client.Client
	Log      logr.Logger
	topology *plumberv1alpha1.Topology
	scheme   *runtime.Scheme
}

const (
	ControllerRevisionManagedByLabel string = "plumber.ugent.be/managed-by"
	ContollerRevisionNumber                 = "plumber.ugent.be/revision-number"
)

func (u *Updater) handle() (reconcile.Result, error) {
	revisionHistory, err := u.listTopologyControllerRevisions()
	if err != nil {
		u.Log.Error(err, "failed to list topology revisions")
		return reconcile.Result{}, err
	}
	_ = u.pruneRevisions()
	nextRevNum := getNextRevisionNumber(revisionHistory)
	revMap := decodeRevs(revisionHistory)
	newRevision, err := u.revisionFromTopologyWithDefaults(nextRevNum)
	if err != nil {
		return reconcile.Result{}, err
	}
	// ActiveRevision & NextRevision are pointers
	isActiveRevisionSet := u.topology.Status.ActiveRevision != nil
	isNextRevisionSet := u.topology.Status.NextRevision != nil
	var activeRevision *plumberv1alpha1.TopologyRevision
	var nextRevision *plumberv1alpha1.TopologyRevision
	if isActiveRevisionSet {
		activeRevision, isActiveRevisionSet = revMap[*u.topology.Status.ActiveRevision]
	}
	if isNextRevisionSet {
		nextRevision, isNextRevisionSet = revMap[*u.topology.Status.NextRevision]
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
			//   4. requeue
			if newRevision.EqualRevisionSpecsSemantic(nextRevision) {
				//goland:noinspection GoNilness
				readyForPhaseOut, err := u.checkActiveRevisionReadyForPhaseOut(*activeRevision)
				if err != nil {
					return reconcile.Result{}, errors.Wrap(err, "failed to check if active revision was ready for phase out")
				}
				if !readyForPhaseOut {
					u.Log.Info("syncer has not yet completed phasing out preparation, requeuing")
					return reconcile.Result{
						RequeueAfter: time.Second * 10,
					}, nil
				}
				currentTopo := u.topology.DeepCopy()
				currentTopo.Status.PhasingOutRevisions = append(currentTopo.Status.PhasingOutRevisions, currentTopo.Status.ActiveRevision)
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
			//goland:noinspection GoNilness
			propagateCGs(&newRevision, *activeRevision)
			// TODO check if last persisted revision == new, skip persisting otherwise
			err := u.persistTopologyRevision(&newRevision)
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
			if newRevision.EqualRevisionSpecsSemantic(nextRevision) {
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
			err := u.persistTopologyRevision(&newRevision)
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
			if newRevision.EqualRevisionSpecsSemantic(activeRevision) {
				return reconcile.Result{}, nil
			}
			// activeRev can't be nil
			//goland:noinspection GoNilness
			propagateCGs(&newRevision, *activeRevision)
			// TODO check if last persisted revision == new, skip persisting otherwise
			err := u.persistTopologyRevision(&newRevision)
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
			return reconcile.Result{
				RequeueAfter: time.Second * 10,
			}, nil
		} else {
			// active and next unset
			// 1. determine default CGs for each processor
			// 2. persist new revision
			// 3. set new revision as active
			// TODO check if last persisted revision == new, skip persisting otherwise
			err := u.persistTopologyRevision(&newRevision)
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
	for pName, _ := range activeRevision.Spec.Processors {
		var pDeploy appsv1.Deployment
		err := u.cClient.Get(
			context.TODO(),
			client.ObjectKey{
				Namespace: activeRevision.Namespace,
				Name:      u.topology.Name + "-" + pName + "-" + strconv.FormatInt(activeRevision.Spec.Revision, 10) + "-deploy",
			},
			&pDeploy,
		)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return false, err
			}
		} else {
			// deploy found; possibly finalizing -> requeue
			return false, nil
		}
		var pScaledObj kedav1alpha1.ScaledObject
		err = u.cClient.Get(
			context.TODO(),
			client.ObjectKey{
				Namespace: activeRevision.Namespace,
				Name:      u.topology.Name + "-" + pName + "-" + strconv.FormatInt(activeRevision.Spec.Revision, 10) + "-scaler",
			},
			&pScaledObj,
		)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return false, err
			}
		} else {
			// deploy found; possibly finalizing -> requeue
			return false, nil
		}
	}
	return true, nil
}

func propagateCGs(newRevision *plumberv1alpha1.TopologyRevision, activeRevision plumberv1alpha1.TopologyRevision) {
	for nProcessorName, nProcessor := range newRevision.Spec.Processors {
		// if both in active and new revision, the same processor (name) refers to the same source (name) -> take over CGs
		if _, nIsSourceRef := newRevision.Spec.Sources[nProcessor.InputFrom]; nIsSourceRef {
			if aProcessor, aProcessorExists := activeRevision.Spec.Processors[nProcessorName]; aProcessorExists {
				if _, aIsSourceRef := activeRevision.Spec.Sources[aProcessor.InputFrom]; aIsSourceRef {
					// take over CG
					nProcessor.ConsumerGroup = aProcessor.ConsumerGroup
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
	for _, topoPartRef := range u.topology.Spec.Parts {
		var topoPart plumberv1alpha1.TopologyPart
		err := u.cClient.Get(context.TODO(),
			client.ObjectKey{
				Namespace: u.topology.Namespace,
				Name:      "topologypart-" + topoPartRef.Name + "-revision-" + strconv.FormatInt(topoPartRef.Revision, 10),
			},
			&topoPart,
		)
		if err != nil {
			// TODO isnotfound handling
			return plumberv1alpha1.TopologyRevision{}, errors.Wrap(err, "failed to get topologyparts objects while creating revision from topology")
		}
		for name, source := range topoPart.Spec.Sources {
			revSpec.Sources[name] = source
		}
		for name, sink := range topoPart.Spec.Sinks {
			revSpec.Sinks[name] = sink
		}
		for name, proc := range topoPart.Spec.Processors {
			revSpec.Processors[name] = plumberv1alpha1.ComposedProcessor{
				InputFrom:     proc.InputFrom,
				Image:         proc.Image,
				MaxScale:      proc.MaxScale,
				Env:           proc.Env,
				SinkBindings:  proc.SinkBindings,
				ConsumerGroup: u.topology.Namespace + "-" + u.topology.Name + "-" + name + "-" + strconv.FormatInt(revisionNumber, 10),
			}
		}
	}
	return plumberv1alpha1.TopologyRevision{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TopologyRevision",
			APIVersion: plumberv1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      u.topology.Name,
			Namespace: u.topology.Namespace,
		},
		Spec:   revSpec,
		Status: plumberv1alpha1.TopologyRevisionStatus{},
	}, nil
}