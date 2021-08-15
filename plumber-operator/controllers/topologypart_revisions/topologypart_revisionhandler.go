package topologypart_revisions

import (
	"bytes"
	"context"
	"sort"
	"strconv"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type RevisionHandler struct {
	cClient      client.Client
	Log          logr.Logger
	topologyPart *plumberv1alpha1.TopologyPart
	scheme       *runtime.Scheme
}

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
	updatedRevisionNumber := getNextRevisionNumber(revisionHistory)
	newRevision, err := createNewRevision(rh.topologyPart, updatedRevisionNumber)
	if err != nil {
		rh.Log.Error(err, "failed to create new revision in-memory")
		return reconcile.Result{}, err
	}

	if len(revisionHistory) == 0 {
		err := rh.persistNewRevision(newRevision)
		if err != nil {
			return reconcile.Result{}, err
		}
	} else {
		// an older revision exists, check for equality of desired spec with last created revision
		prevRevision := revisionHistory[len(revisionHistory)-1]
		var prevTopologyPart plumberv1alpha1.TopologyPart
		_, _, err := unstructured.UnstructuredJSONScheme.Decode(prevRevision.Data.Raw, &schema.GroupVersionKind{
			Group:   "plumber.ugent.be",
			Version: "v1alpha1",
			Kind:    "TopologyPart",
		}, &prevTopologyPart)
		if err != nil {
			rh.Log.Error(err, "failed to decode last revision")
			return reconcile.Result{}, err
		}

		if equality.Semantic.DeepEqual(rh.topologyPart.Spec, prevTopologyPart.Spec) {
			updatedRevisionNumber = prevRevision.Revision
		} else {
			err = rh.persistNewRevision(newRevision)
			if err != nil {
				return reconcile.Result{}, err
			}
		}
	}

	// update status with newest revision number, if it needs to be updated
	//if updatedRevisionNumber != rh.topologyPart.Status.LatestRevision {
	// FIXME not found error on topology, unsure why
	//rh.topologyPart.Status.LatestRevision = updatedRevisionNumber
	//err := rh.cClient.Status().Update(context.TODO(), rh.topologyPart)
	//if err != nil {
	//	rh.Log.Error(err, "failed to update status")
	//	return reconcile.Result{}, err
	//}
	//}

	return reconcile.Result{}, nil
}

func (rh *RevisionHandler) persistNewRevision(rev *appsv1.ControllerRevision) error {
	_ = ctrl.SetControllerReference(rh.topologyPart, rev, rh.scheme)
	applyOpts := []client.PatchOption{client.FieldOwner("topologypart-controller"), client.ForceOwnership}
	err := rh.cClient.Patch(context.TODO(), rev, client.Apply, applyOpts...)
	if err != nil {
		rh.Log.Error(err, "failed to patch new revision")
		return err
	}
	return nil
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
			Name:      shared.BuildTopoPartRevisionName(topologyPart.GetName(), revisionNum),
			Namespace: topologyPart.Namespace,
			Labels: map[string]string{
				shared.ManagedByLabel:      topologyPart.Name,
				shared.RevisionNumberLabel: strconv.FormatInt(revisionNum, 10),
			},
		},
		Data:     runtime.RawExtension{Raw: str.Bytes()},
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
	selector := labels.SelectorFromSet(labels.Set(map[string]string{shared.ManagedByLabel: rh.topologyPart.GetName()}))
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
