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
	if err := u.Get(ctx, req.NamespacedName, &crdTopo); err != nil {
		if kerrors.IsNotFound(err) {
			logger.Info("Topology object not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		// requeue on any other error
		return ctrl.Result{}, errors.Wrap(err, fmt.Sprintf("Failed to get Topology %s", req.String()))
	}

	rvh := Updater{
		cClient:  u.Client,
		topology: &crdTopo,
		scheme:   u.Scheme,
		Log:      logger,
		uClient:  u.UClient,
	}
	return rvh.handle()
}

// SetupWithManager sets up the controller with the Manager.
func (u *UpdaterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&plumberv1alpha1.Topology{}).
		Owns(&plumberv1alpha1.TopologyRevision{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(u)
}
