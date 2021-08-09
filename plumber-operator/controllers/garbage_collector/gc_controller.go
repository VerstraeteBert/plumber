package garbage_collector

import (
	"context"
	"fmt"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type GarbageCollector struct {
	UClient    client.Client
	CClient    client.Client
	Log        logr.Logger
	KfkClients KafkaClients
}

//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologies/finalizers,verbs=update
func (gc *GarbageCollector) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	gc.Log = gc.Log.WithValues("Topology", req.NamespacedName)

	var crdTopo plumberv1alpha1.Topology
	if err := gc.CClient.Get(ctx, req.NamespacedName, &crdTopo); err != nil {
		if kerrors.IsNotFound(err) {
			gc.Log.Info("Topology object not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		// requeue on any other error
		return ctrl.Result{}, errors.Wrap(err, fmt.Sprintf("Failed to get Topology %s", req.String()))
	}

	rvh := GarbageCollectorHandler{
		cClient:    gc.CClient,
		topology:   &crdTopo,
		Log:        gc.Log,
		uClient:    gc.UClient,
		kfkClients: gc.KfkClients,
	}
	return rvh.handle()
}

func gcFilters() predicate.Predicate {
	// the gc is not interested in creates/deletes of a topology
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		// the updater watches the topology object,
		//and is only interested in updates if a change occurred in its status.phasingOut
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld.GetObjectKind().GroupVersionKind().Kind == "Topology" {
				oldTopo := e.ObjectOld.(*plumberv1alpha1.Topology)
				newTopo := e.ObjectNew.(*plumberv1alpha1.Topology)
				if newTopo.Status.PhasingOutRevisions == nil {
					return false
				}
				if oldTopo.Status.PhasingOutRevisions == nil {
					return true
				}
				// check if a revision was added to the phasing out list
				return len(oldTopo.Status.PhasingOutRevisions) < len(newTopo.Status.PhasingOutRevisions)
			}
			return true
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (gc *GarbageCollector) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&plumberv1alpha1.Topology{}).
		WithEventFilter(gcFilters()).
		Complete(gc)
}
