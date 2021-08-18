package updater

import (
	"context"
	"strconv"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	strimziv1beta1 "github.com/VerstraeteBert/plumber-operator/vendor-api/strimzi/v1beta1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	finalizer string = "plumber.ugent.be/finalizer"
)

func (u *Updater) listTopologyRevisions() (map[int64]plumberv1alpha1.TopologyRevision, error) {
	// List all TopologyRevisions in the namespace that match the selector
	var revisionList = new(plumberv1alpha1.TopologyRevisionList)
	selector := labels.SelectorFromSet(labels.Set(map[string]string{shared.ManagedByLabel: u.topology.GetName()}))
	err := u.cClient.List(context.TODO(), revisionList, client.InNamespace(u.topology.GetNamespace()), client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, err
	}
	var history = revisionList.Items
	owned := make(map[int64]plumberv1alpha1.TopologyRevision)
	for i := range history {
		owned[history[i].Spec.Revision] = history[i]
	}
	return owned, err
}

func (u *Updater) persistTopologyRevision(revision plumberv1alpha1.TopologyRevision) error {
	// no ownership issues possible, new object.
	_ = ctrl.SetControllerReference(u.topology, &revision, u.scheme)
	controllerutil.AddFinalizer(&revision, finalizer)
	patchOpts := []client.PatchOption{client.FieldOwner(FieldManager), client.ForceOwnership}
	err := u.cClient.Patch(context.TODO(), &revision, client.Apply, patchOpts...)
	if err != nil {
		u.Log.Error(err, "failed to patch new revision")
		return err
	}
	return nil
}

func getNextRevisionNumber(revs map[int64]plumberv1alpha1.TopologyRevision) int64 {
	nextNum := int64(0)
	for _, r := range revs {
		if r.Spec.Revision > nextNum {
			nextNum = r.Spec.Revision
		}
	}
	return nextNum + 1
}

func arrayContains(needle int64, haystack []int64) (int, bool) {
	for idx, item := range haystack {
		if item == needle {
			return idx, true
		}
	}
	return -1, false
}

func (u *Updater) deleteAllProcessorOutputTopics(topoRev plumberv1alpha1.TopologyRevision) error {
	err := u.cClient.DeleteAllOf(
		context.TODO(),
		&strimziv1beta1.KafkaTopic{},
		client.InNamespace(shared.KafkaNamespace),
		client.MatchingLabels{
			shared.ManagedByLabel:      u.topology.GetName(),
			shared.RevisionNumberLabel: strconv.FormatInt(topoRev.Spec.Revision, 10),
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to delete all processor output topics for a revision")
	}
	return nil
}

func (u *Updater) deleteAllProcessorDeployments(topoRev plumberv1alpha1.TopologyRevision) error {
	err := u.cClient.DeleteAllOf(
		context.TODO(),
		&appsv1.Deployment{},
		client.InNamespace(topoRev.GetNamespace()),
		client.MatchingLabels{
			shared.ManagedByLabel:      u.topology.GetName(),
			shared.RevisionNumberLabel: strconv.FormatInt(topoRev.Spec.Revision, 10),
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to delete all processor deployments for a revision")
	}
	return nil
}

func (u *Updater) pruneRevisionFromTopologyMarkers(revNum int64) error {
	shouldPatch := false
	prunedTopo := u.topology.DeepCopy()
	if prunedTopo.Status.ActiveRevision != nil && revNum == *prunedTopo.Status.ActiveRevision {
		prunedTopo.Status.ActiveRevision = nil
		shouldPatch = true
	} else if prunedTopo.Status.NextRevision != nil && revNum == *prunedTopo.Status.NextRevision {
		prunedTopo.Status.NextRevision = nil
		shouldPatch = true
	} else if idx, contains := arrayContains(revNum, prunedTopo.Status.PhasingOutRevisions); contains {
		prunedTopo.Status.PhasingOutRevisions = append(prunedTopo.Status.PhasingOutRevisions[:idx], prunedTopo.Status.PhasingOutRevisions[idx+1:]...)
		shouldPatch = true
	}

	if shouldPatch {
		err := u.cClient.Status().Patch(context.TODO(), prunedTopo, client.MergeFrom(u.topology))
		if err != nil {
			u.Log.Error(err, "failed to delete finalizing revision status marker", "revision", revNum)
		}
		u.topology = prunedTopo
	}
	return nil
}

// pruneRevisions is responsible for:
// a) checking if a revision is finalizing
//		if so delete all data plane components of processors: first the deployments then the topics
// b) ensuring that active, next, phasing-out revision references actually still exist (otherwise the references are removed)
// TODO could potentially also keep at max 10 superfluous (not in use) revisions
func (u *Updater) pruneRevisions(revs map[int64]plumberv1alpha1.TopologyRevision, deleteFromMarkers bool) error {
	for revNum, topoRev := range revs {
		if topoRev.GetDeletionTimestamp() != nil && controllerutil.ContainsFinalizer(&topoRev, finalizer) {
			// delete all deployments
			err := u.deleteAllProcessorDeployments(topoRev)
			if err != nil {
				u.Log.Error(err, "revision", revNum)
				return err
			}
			// delete all topics
			err = u.deleteAllProcessorOutputTopics(topoRev)
			if err != nil {
				u.Log.Error(err, "revision", revNum)
				return err
			}

			if deleteFromMarkers {
				// ensure the revision number is not in the status markers anymore
				err = u.pruneRevisionFromTopologyMarkers(revNum)
				if err != nil {
					return err
				}
			}

			// finally, remove the finalizer of the topologyrevision
			updatedTopoRev := topoRev.DeepCopy()
			controllerutil.RemoveFinalizer(updatedTopoRev, finalizer)
			err = u.cClient.Patch(context.TODO(), updatedTopoRev, client.MergeFrom(&topoRev))
			if err != nil {
				return errors.Wrap(err, "failed to unset finalizer for a topologyrevision")
			}
		}
	}

	return nil
}
