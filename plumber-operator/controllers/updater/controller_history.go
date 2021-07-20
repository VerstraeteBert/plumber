package updater

import (
	"context"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (u *Updater) listTopologyRevisions() (map[int64]*plumberv1alpha1.TopologyRevision, error) {
	// List all TopologyRevisions in the namespace that match the selector
	var revisionList = new(plumberv1alpha1.TopologyRevisionList)
	selector := labels.SelectorFromSet(labels.Set(map[string]string{RevisionManagedByLabel: u.topology.GetName()}))
	err := u.cClient.List(context.TODO(), revisionList, client.InNamespace(u.topology.GetNamespace()), client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, err
	}
	var history = revisionList.Items
	owned := make(map[int64]*plumberv1alpha1.TopologyRevision)
	for i := range history {
		ref := metav1.GetControllerOf(&history[i])
		if ref == nil || ref.UID == u.topology.GetUID() {
			owned[history[i].Spec.Revision] = &history[i]
		}
	}
	return owned, err
}

func (u *Updater) persistTopologyRevision(revision *plumberv1alpha1.TopologyRevision) error {
	// no ownership issues possible, new object.
	_ = ctrl.SetControllerReference(u.topology, revision, u.scheme)
	applyOpts := []client.PatchOption{client.FieldOwner(FieldManager), client.ForceOwnership}
	err := u.cClient.Patch(context.TODO(), revision, client.Apply, applyOpts...)
	if err != nil {
		u.Log.Error(err, "failed to patch new revision")
		return err
	}
	return nil
}

func getNextRevisionNumber(revs map[int64]*plumberv1alpha1.TopologyRevision) int64 {
	nextNum := int64(0)
	for _, r := range revs {
		if r.Spec.Revision > nextNum {
			nextNum = r.Spec.Revision
		}
	}
	return nextNum + 1
}

// pruneRevisions is responsible for
// 1. ensuring that at max 10 superfluous revisions are kept (with superfluous meaning: not in use)
// 2. ensuring that active, next, phasing-out revision references actually still exist (otherwise the references are removed)
func (u *Updater) pruneRevisions() error {
	// TODO implement me
	return nil
}
