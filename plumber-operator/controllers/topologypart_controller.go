package controllers

import (
	"context"
	"github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TopologyPartReconciler reconciles a TopologyPart object
type TopologyPartReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologyparts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologyparts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=plumber.ugent.be,resources=topologyparts/finalizers,verbs=update
func (r *TopologyPartReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("topologypart", req.NamespacedName)

	var crdPart plumberv1alpha1.TopologyPart
	if err := r.Get(ctx, req.NamespacedName, &crdPart); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("TopologyPart object not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		// requeue on any other error
		logger.Error(err, "Failed to get TopologyPart Object %s", req.String())
		return ctrl.Result{}, err
	}

	rvh := RevisionHandler{
		cClient: r.Client,
		// deepcopy because the status is changed later on
		topologyPart: crdPart.DeepCopy(),
		scheme:       r.Scheme,
		Log:          logger,
	}
	return rvh.handle()
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopologyPartReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.TopologyPart{}).
		Complete(r)
}
