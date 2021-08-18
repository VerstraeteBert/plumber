package updater

import (
	"context"
	"fmt"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Updater reconciles a Topology object
type UpdaterReconciler struct {
	client.Client
	Log     logr.Logger
	Scheme  *runtime.Scheme
	UClient client.Client
}

//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologies/finalizers,verbs=update
func (u *UpdaterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := u.Log.WithValues("Topology", req.NamespacedName)

	var crdTopo plumberv1alpha1.Topology
	if err := u.UClient.Get(ctx, req.NamespacedName, &crdTopo); err != nil {
		if kerrors.IsNotFound(err) {
			logger.Info("topology object not found, must be deleted. Ensuring all owned TopologyRevisions are finalized")
			mockTopo := plumberv1alpha1.Topology{}
			mockTopo.SetNamespace(req.NamespacedName.Namespace)
			mockTopo.SetName(req.NamespacedName.Name)
			rvh := Updater{
				cClient:  u.Client,
				topology: &mockTopo,
				scheme:   u.Scheme,
				Log:      logger,
				uClient:  u.UClient,
			}

			return rvh.handleTopoDeleted()
		}
		// requeue on any other error
		return ctrl.Result{}, errors.Wrap(err, fmt.Sprintf("failed to get Topology %s", req.String()))
	}

	rvh := Updater{
		cClient:  u.Client,
		topology: &crdTopo,
		scheme:   u.Scheme,
		Log:      logger,
		uClient:  u.UClient,
	}
	return rvh.handleTopoExists()
}

// SetupWithManager sets up the controller with the Manager.
func (u *UpdaterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&plumberv1alpha1.Topology{}).
		Owns(&plumberv1alpha1.TopologyRevision{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(u)
}
