package controllers

import (
	"context"
	"fmt"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/domain"
	"github.com/VerstraeteBert/plumber-operator/controllers/util"
	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strconv"
)

// TopologyReconciler reconciles a Topology object
type TopologyReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopologyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	topoMapFn := func(obj client.Object) []reconcile.Request {
		ns := obj.GetNamespace()
		return []reconcile.Request{
			// FIXME, shouldn't lock the name of the composition object
			{NamespacedName: types.NamespacedName{
				Namespace: ns,
				Name:      domain.CompositionKubeName,
			}},
		}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&plumberv1alpha1.Topology{}).
		// Ensures all changes to topologyparts trigger reconciliation of a topology in the same namespace
		Watches(
			&source.Kind{Type: &plumberv1alpha1.TopologyPart{}},
			handler.EnqueueRequestsFromMapFunc(topoMapFn),
			// TODO evaluate if event filtering is needed
			// TODO filter status changes (generation change)
		).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		Owns(&appsv1.Deployment{}).
		Owns(&kedav1alpha1.ScaledObject{}).
		Complete(r)
}

// TODO when changing this to only reconcile based on revisions, reevaluate the status / patching sections
//		- update/creates of objects should go hand in hand with updating statuses
//		- model every distinct step as a substruct of the reconciler, with all necessary object embedded in the struct
//				-> this would allow to keep the functions within them minimal in terms of signature (cfr. flinkonk8s)
//				-> keep all of the logic outside of the Reconcile function, it calls a handler that returns a reconcile.Result wrapper
//				-> the handler then invokes all necessary steps
// TODO generate RBACs for all objects generated and watched
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology/finalizers,verbs=update
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologypart,verbs=get;list;watch
func (r *TopologyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	defer util.Elapsed(r.Log, "Reconciliation")()
	// 1. get composition object: check if deleted
	var crdComp plumberv1alpha1.Topology
	if err := r.Get(ctx, req.NamespacedName, &crdComp); err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Topology object not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		// requeue on any other error
		r.Log.Error(err, "Failed to get Topology Object %s", req.String())
		return ctrl.Result{}, err
	}
	// 2. Fetch all TopologyParts referenced in the spec
	topoParts := plumberv1alpha1.TopologyPartList{}
	for _, partRef := range crdComp.Spec.Parts {
		topoRevision := appsv1.ControllerRevision{}
		err := r.Client.Get(ctx,
			types.NamespacedName{
				Name:      partRef.Name + "-revision-" + strconv.FormatInt(partRef.Revision, 10),
				Namespace: crdComp.Namespace,
			},
			&topoRevision,
		)
		if err != nil {
			if errors.IsNotFound(err) {
				// not found -> invalid ref: don't retry
				return ctrl.Result{}, nil
			} else {
				// API server error -> retry
				return ctrl.Result{}, err
			}
		}
		var topoPart plumberv1alpha1.TopologyPart
		_, _, err = unstructured.UnstructuredJSONScheme.Decode(topoRevision.Data.Raw, &schema.GroupVersionKind{
			Group:   "plumber.ugent.be",
			Version: "v1alpha1",
			Kind:    "TopologyPart",
		}, &topoPart)
		topoParts.Items = append(topoParts.Items, topoPart)
	}
	//err := r.Client.List(ctx, &topoParts, client.InNamespace(req.Namespace))
	//if err != nil {
	//	// requeue on any list error
	//	r.Log.Error(err, fmt.Sprintf("Failed to list topologies in namespace %s", req.Namespace))
	//	return ctrl.Result{}, err
	//}
	// 3. build topology domain object, combining all present topologyparts.
	// The build function also detects semantic errors, if any semantic errors were present shouldCont is set to false
	domainTopo, shouldCont, err := r.buildDomainTopo(&crdComp, &topoParts)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !shouldCont {
		return ctrl.Result{}, nil
	}

	// 4. patch processors
	err = r.reconcileProcessors(&crdComp, domainTopo)
	if err != nil {
		r.Log.Error(err, err.Error())
		return ctrl.Result{}, err
	}
	// 4. delete unnecessary objects
	errCleanup := r.cleanup(crdComp.Namespace, buildNecessaryObjsSet(domainTopo))
	if errCleanup != nil {
		r.Log.Error(err, "Failed to perform cleanup")
	}
	// 5. update state (global & for each component)
	errUpdateState := r.updateStateSuccess(&crdComp, domainTopo)
	if errUpdateState != nil {
		r.Log.Error(err, "Failed to update status")
	}
	if errCleanup != nil || errUpdateState != nil {
		return ctrl.Result{}, fmt.Errorf("error in cleanup or during state updates")
	}
	return ctrl.Result{}, nil
}
