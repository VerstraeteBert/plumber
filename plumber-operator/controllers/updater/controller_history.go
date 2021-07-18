package updater

import (
	"bytes"
	"context"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
)

func (u *Updater) listTopologyControllerRevisions() ([]*appsv1.ControllerRevision, error) {
	// List all TopologyRevisions in the namespace that match the selector
	var revisionList = new(appsv1.ControllerRevisionList)
	selector := labels.SelectorFromSet(labels.Set(map[string]string{ControllerRevisionManagedByLabel: u.topology.GetName()}))
	err := u.cClient.List(context.TODO(), revisionList, client.InNamespace(u.topology.GetNamespace()), client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, err
	}
	var history = revisionList.Items
	var owned []*appsv1.ControllerRevision
	for i := range history {
		ref := metav1.GetControllerOf(&history[i])
		if ref == nil || ref.UID == u.topology.GetUID() {
			owned = append(owned, &history[i])
		}
	}
	return owned, err
}

func (u *Updater) persistTopologyRevision(revision *plumberv1alpha1.TopologyRevision) error {
	str := &bytes.Buffer{}
	_ = unstructured.UnstructuredJSONScheme.Encode(revision, str)
	revToWrite := appsv1.ControllerRevision{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ControllerRevision",
			APIVersion: appsv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "topology-" + revision.Name + "-revision-" + strconv.FormatInt(revision.Spec.Revision, 10),
			Namespace: revision.Namespace,
			Labels: map[string]string{
				ControllerRevisionManagedByLabel: revision.Name,
				ContollerRevisionNumber:          strconv.FormatInt(revision.Spec.Revision, 10),
			},
		},
		Data:     runtime.RawExtension{Raw: str.Bytes()},
		Revision: revision.Spec.Revision,
	}
	// no ownership issues possible, new object.
	_ = ctrl.SetControllerReference(u.topology, revision, u.scheme)
	applyOpts := []client.PatchOption{client.FieldOwner(FieldManager), client.ForceOwnership}
	err := u.cClient.Patch(context.TODO(), &revToWrite, client.Apply, applyOpts...)
	if err != nil {
		u.Log.Error(err, "failed to patch new revision")
		return err
	}
	return nil
}

func decodeRevs(revs []*appsv1.ControllerRevision) map[int64]*plumberv1alpha1.TopologyRevision {
	decoded := make(map[int64]*plumberv1alpha1.TopologyRevision, len(revs))
	for _, rev := range revs {
		var topologyRevision plumberv1alpha1.TopologyRevision
		// note: not catching errors here: could only go wrong if API version changes, and older revision can not be decoded anymore
		_, _, _ = unstructured.UnstructuredJSONScheme.Decode(rev.Data.Raw, &schema.GroupVersionKind{
			Group:   "plumber.ugent.be",
			Version: "v1alpha1",
			Kind:    "TopologyRevision",
		}, &topologyRevision)
		decoded[rev.Revision] = &topologyRevision
	}
	return decoded
}

func getNextRevisionNumber(revs []*appsv1.ControllerRevision) int64 {
	nextNum := int64(0)
	for _, r := range revs {
		if r.Revision > nextNum {
			nextNum = r.Revision
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
