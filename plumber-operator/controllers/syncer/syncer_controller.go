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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Syncer reconciles a Topology object
type Syncer struct {
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
func (s *Syncer) SetupWithManager(mgr ctrl.Manager) error {
	topoToactiveRevisionMapper := func(obj client.Object) []reconcile.Request {
		topo := obj.(*plumberv1alpha1.Topology)
		if topo.Status.ActiveRevision == nil {
			return []reconcile.Request{}
		}
		return []reconcile.Request{
			{NamespacedName: types.NamespacedName{
				Namespace: topo.GetNamespace(),
				Name:      shared.BuildTopoRevisionName(topo.GetName(), *topo.Status.ActiveRevision),
			}},
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&plumberv1alpha1.TopologyRevision{}).
		Owns(&appsv1.Deployment{}).
		Owns(&kedav1alpha1.ScaledObject{}).
		Watches(
			&source.Kind{
				Type: &plumberv1alpha1.Topology{},
				// cache?
			},
			handler.EnqueueRequestsFromMapFunc(topoToactiveRevisionMapper),
		).
		WithEventFilter(syncerUpdaterFilters()).
		Complete(s)
}

//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topology/finalizers,verbs=update
func (s *Syncer) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	defer shared.Elapsed(s.Log, "Syncing")()
	s.Log.Info("starting syncer")
	// 1. get composition object:
	var topoRev plumberv1alpha1.TopologyRevision
	if err := s.Get(ctx, req.NamespacedName, &topoRev); err != nil {
		if kerrors.IsNotFound(err) {
			s.Log.Info("TopologyRevision object not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		// requeue on any other error
		return ctrl.Result{}, errors.Wrap(err, fmt.Sprintf("Failed to get Topology Object %s", req.String()))
	}
	// 2. Fetch topology that owns the topologyRevision
	managingTopoName, found := topoRev.GetLabels()[shared.ManagedByLabel]
	if !found {
		// TODO log me
		return ctrl.Result{}, nil
	}
	var topo plumberv1alpha1.Topology
	err := s.Client.Get(ctx,
		types.NamespacedName{
			Name:      managingTopoName,
			Namespace: topoRev.GetNamespace(),
		},
		&topo,
	)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// not found: must be deleted.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if topo.Status.ActiveRevision == nil {
		return ctrl.Result{}, nil
	}
	if *topo.Status.ActiveRevision != topoRev.Spec.Revision {
		return ctrl.Result{}, nil
	}
	sHandler := syncerHandler{
		cClient:        s.Client,
		activeRevision: topoRev,
		topology:       topo,
		Log:            s.Log,
	}
	// 3. patch processors
	err = sHandler.reconcileProcessors()
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. update state based on active revision components (global & for each component)
	err = sHandler.updateActiveStatus()
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to update status based on active revision")
	}
	return ctrl.Result{}, nil
}
