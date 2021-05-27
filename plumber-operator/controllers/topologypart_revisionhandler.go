package controllers

import (
	"bytes"
	"context"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sort"
	"strconv"
)

type RevisionHandler struct {
	cClient client.Client
	Log    logr.Logger
	topologyPart *plumberv1alpha1.TopologyPart
	scheme 	*runtime.Scheme
}

const (
	ControllerRevisionManagedByLabel string = "plumber.ugent.be/managed-by"
	ContollerRevisionNumber = "plumber.ugent.be/revision-number"
)

func (rh *RevisionHandler) handle() (reconcile.Result, error) {
	// listRevisions
	revisionHistory, err := rh.listControllerRevisions()
	if err != nil {
		rh.Log.Error(err, "failed to list topologypart revisions")
		return reconcile.Result{}, err
	}
	// sort by revision creation time
	sort.Stable(byRevision(revisionHistory))

	// create new in-mem revision based on the given topologypart
	newRevision, err := createNewRevision(rh.topologyPart, getNextRevisionNumber(revisionHistory))
	if err != nil {
		rh.Log.Error(err,"failed to create new revision in-memory")
		return reconcile.Result{}, err
	}

	// get the last one
	//if len(revisionHistory) == 0 {
	//	// first revision
	//	// push it immediately
	//} else {
	//	// possibly new version
	//	lastRevision := revisionHistory[len(revisionHistory) - 1]
	//	isTopologyPartNew := compareRevisions()
	//}
	_ = ctrl.SetControllerReference(rh.topologyPart, newRevision, rh.scheme)
	applyOpts := []client.PatchOption{client.FieldOwner("topologypart-controller"), client.ForceOwnership}
	err = rh.cClient.Patch(context.TODO(), newRevision, client.Apply, applyOpts...)
	if err != nil {
		rh.Log.Error(err, "failed to patch new revision")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func getNextRevisionNumber(revisions []*appsv1.ControllerRevision) int64 {
	count := len(revisions)
	if count <= 0 {
		return 1
	}
	return revisions[count-1].Revision + 1
}

func createNewRevision(topologyPart *plumberv1alpha1.TopologyPart, revisionNum int64) (*appsv1.ControllerRevision, error) {
	str := &bytes.Buffer{}
	err := unstructured.UnstructuredJSONScheme.Encode(topologyPart, str)
	if err != nil {
		return nil, err
	}
	return &appsv1.ControllerRevision{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ControllerRevision",
			APIVersion: appsv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: topologyPart.Name + "-revision-" + strconv.FormatInt(revisionNum, 10),
			Namespace: topologyPart.Namespace,
			Labels: map[string]string{
				ControllerRevisionManagedByLabel: topologyPart.Name,
				ContollerRevisionNumber: strconv.FormatInt(revisionNum, 10),
			},
		},
		Data: runtime.RawExtension{Raw: str.Bytes()},
		Revision: revisionNum,
	}, nil
}

// byRevision implements sort.Interface to allow ControllerRevisions to be sorted by Revision.
type byRevision []*appsv1.ControllerRevision

func (br byRevision) Len() int {
	return len(br)
}

// Less breaks ties first by creation timestamp, then by name
func (br byRevision) Less(i, j int) bool {
	if br[i].Revision == br[j].Revision {
		if br[j].CreationTimestamp.Equal(&br[i].CreationTimestamp) {
			return br[i].Name < br[j].Name
		}
		return br[j].CreationTimestamp.After(br[i].CreationTimestamp.Time)
	}
	return br[i].Revision < br[j].Revision
}

func (br byRevision) Swap(i, j int) {
	br[i], br[j] = br[j], br[i]
}

func (rh *RevisionHandler) listControllerRevisions() ([]*appsv1.ControllerRevision, error) {
	// List all revisions in the namespace that match the selector
	var revisionList = new(appsv1.ControllerRevisionList)
	selector := labels.SelectorFromSet(labels.Set(map[string]string{ControllerRevisionManagedByLabel: rh.topologyPart.GetName()}))
	err := rh.cClient.List(context.TODO(), revisionList, client.InNamespace(rh.topologyPart.GetNamespace()), client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, err
	}
	var history = revisionList.Items
	var owned []*appsv1.ControllerRevision
	for i := range history {
		ref := metav1.GetControllerOf(&history[i])
		if ref == nil || ref.UID == rh.topologyPart.GetUID() {
			owned = append(owned, &history[i])
		}
	}
	return owned, err
}
