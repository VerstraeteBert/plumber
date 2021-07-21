package syncer

import (
	"context"
	"fmt"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// TopologyReconciler reconciles a Topology object
type TopologyReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

func syncerUpdaterFilters() predicate.Predicate {
	return predicate.Funcs{
		// the updater watches the topology object,
		//and is only interested in updates if a change occurred in its status.NextRevision or status.ActiveRevision
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld.GetObjectKind().GroupVersionKind().Kind == "Topology" {
				oldTopo := e.ObjectOld.(*plumberv1alpha1.Topology)
				newTopo := e.ObjectNew.(*plumberv1alpha1.Topology)
				return oldTopo.Status.NextRevision != newTopo.Status.NextRevision || oldTopo.Status.ActiveRevision != newTopo.Status.ActiveRevision
			}
			return true
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopologyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&plumberv1alpha1.Topology{}).
		Owns(&appsv1.Deployment{}).
		Owns(&kedav1alpha1.ScaledObject{}).
		WithEventFilter(syncerUpdaterFilters()).
		Complete(r)
}

//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology/finalizers,verbs=update
func (r *TopologyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	defer shared.Elapsed(r.Log, "Syncing")()
	r.Log.Info("starting syncer")
	// 1. get composition object:
	var topo plumberv1alpha1.Topology
	if err := r.Get(ctx, req.NamespacedName, &topo); err != nil {
		if kerrors.IsNotFound(err) {
			r.Log.Info("Topology object not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		// requeue on any other error
		return ctrl.Result{}, errors.Wrap(err, fmt.Sprintf("Failed to get Topology Object %s", req.String()))
	}
	// 2. Fetch active TopologyRevision
	// if no activeRevision is set -> stop

	if topo.Status.ActiveRevision == nil {
		r.Log.Info("Active revision is nil")
		return ctrl.Result{}, nil
	}
	r.Log.Info("Active revision is not nil")
	var topoRev plumberv1alpha1.TopologyRevision
	err := r.Client.Get(ctx,
		types.NamespacedName{
			Name:      shared.BuildTopoRevisionName(topo.Name, *topo.Status.ActiveRevision),
			Namespace: topo.Namespace,
		},
		&topoRev,
	)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// not found: must be deleted.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 3. patch processors
	err = r.reconcileProcessors(topo, topoRev)
	if err != nil {
		return ctrl.Result{}, err
	}

	//// 5. update state (global & for each component)
	//errUpdateState := r.updateState(&crdComp, domainTopo)
	//if errUpdateState != nil {
	//	r.Log.Error(err, "Failed to update status")
	//}
	//if  errUpdateState != nil {
	//	return ctrl.Result{}, fmt.Errorf("error in cleanup or during state updates")
	//}
	return ctrl.Result{}, nil
}
