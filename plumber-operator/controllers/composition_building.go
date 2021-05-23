package controllers

import (
	"context"
	"fmt"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/domain"
	"github.com/VerstraeteBert/plumber-operator/controllers/util"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

const (
	DefaultMaxReplicas = 10
)

func (r *TopologyReconciler) buildDomainTopo(crdComp *plumberv1alpha1.Topology, topoParts *plumberv1alpha1.TopologyPartList) (*domain.Topology, bool, error) {
	defer util.Elapsed(r.Log, "Building topo")()
	projName := strings.Split(crdComp.Namespace, "plumber-")[1]
	defaultMaxScalePtr := crdComp.Spec.DefaultScale
	var defaultMaxScale int
	if defaultMaxScalePtr == nil {
		defaultMaxScale = DefaultMaxReplicas
	} else {
		defaultMaxScale = *defaultMaxScalePtr
	}

	domainTopo, errs := domain.BuildTopology(topoParts, projName, defaultMaxScale)
	if errs != nil {
		err := r.handleSemanticValidationErrors(crdComp, errs)
		return domainTopo, false, err
	}
	return domainTopo, true, nil
}

// on semantic validation errors, everything thats running is currently nukes (only deletes objects owned by the controller)
func (r *TopologyReconciler) handleSemanticValidationErrors(topo *plumberv1alpha1.Topology, errorList []error) error {
	// log all encountered errors
	for _, semErr := range errorList {
		r.Log.Error(semErr, semErr.Error())
	}
	// set status to invalid, with list of errors
	// delete all statuses of previously active components & set global status with list of encountered semantic errors
	changed := false
	newStatus := topo.Status.DeepCopy()
	newGlobalStat := metav1.Condition{
		Type:    StatusReady,
		Status:  metav1.ConditionFalse,
		Reason:  "SemanticValidationError",
		Message: util.ErrorsToString(errorList),
	}
	if !util.IsStatusConditionPresentAndFullyEqual(topo.Status.Status, newGlobalStat) {
		changed = true
		meta.SetStatusCondition(&newStatus.Status, newGlobalStat)
		// clearing all other statuses, if global status is already semantic error, all of them will already be empty maps
		// TODO keep individual component statuses, and set specific validation errors on each of the components
		newStatus.ProcessorStatuses = make(map[string]*[]metav1.Condition)
		newStatus.SinkStatuses = make(map[string]*[]metav1.Condition)
		newStatus.SourceStatuses = make(map[string]*[]metav1.Condition)
		topo.Status = *newStatus
	}

	if changed {
		err := r.Status().Update(context.TODO(), topo)
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("failed to update status: %s", err.Error()))
			return err
		}
	}

	err := r.cleanup(topo.Namespace, util.NewSet())
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Failed to tear running components down"))
		return err
	}
	return nil
}
