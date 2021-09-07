package syncer

import (
	"context"
	"fmt"

	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	StatusDeploymentReady string = "DeploymentReady"
	StatusReady           string = "Ready"
	FieldManager          string = "plumber-syncer"
)

func pruneMappedConditions(reqNames map[string]interface{}, currentNames *map[string]*[]metav1.Condition) bool {
	pruned := false
	for currName := range *currentNames {
		if _, found := reqNames[currName]; !found {
			delete(*currentNames, currName)
			pruned = true
		}
	}
	return pruned
}

func (sh *syncerHandler) determineSourceStates(newStatus *plumberv1alpha1.TopologyStatus) {
	if newStatus.SourceStatuses == nil {
		newStatus.SourceStatuses = make(map[string]*[]metav1.Condition)
	} else {
		nameMap := make(map[string]interface{}, len(newStatus.SourceStatuses))
		for k := range sh.activeRevision.Spec.Sources {
			nameMap[k] = nil
		}
		pruneMappedConditions(nameMap, &newStatus.SourceStatuses)
	}
	for sourceName := range sh.activeRevision.Spec.Sources {
		_, found := newStatus.SourceStatuses[sourceName]
		if !found {
			newConds := make([]metav1.Condition, 0)
			meta.SetStatusCondition(&newConds, metav1.Condition{
				Type:    StatusReady,
				Status:  "True",
				Reason:  "ok",
				Message: "",
			})
			newStatus.SourceStatuses[sourceName] = &newConds
		}
	}
}

func (sh *syncerHandler) determineSinkStatuses(newStatus *plumberv1alpha1.TopologyStatus) {
	if newStatus.SinkStatuses == nil {
		newStatus.SinkStatuses = make(map[string]*[]metav1.Condition)
	} else {
		nameMap := make(map[string]interface{})
		for k := range sh.activeRevision.Spec.Sinks {
			nameMap[k] = nil
		}
		pruneMappedConditions(nameMap, &newStatus.SinkStatuses)
	}
	for sinkName := range sh.activeRevision.Spec.Sinks {
		_, found := newStatus.SinkStatuses[sinkName]
		if !found {
			newConds := make([]metav1.Condition, 0)
			meta.SetStatusCondition(&newConds, metav1.Condition{
				Type:    StatusReady,
				Status:  "True",
				Reason:  "ok",
				Message: "",
			})
			newStatus.SinkStatuses[sinkName] = &newConds
		}
	}
}

func toCompatCondStatus(status v1.ConditionStatus) metav1.ConditionStatus {
	switch status {
	case v1.ConditionFalse:
		return metav1.ConditionFalse
	case v1.ConditionTrue:
		return metav1.ConditionTrue
	default:
		return metav1.ConditionUnknown
	}
}

// TODO this logic should definitely be reconsidered heavily (more meaningful info than just propagating the deployment ready status)
func (sh *syncerHandler) deriveDeploymentReadyStatus(pName string) (*metav1.Condition, error) {
	var deployment appsv1.Deployment
	err := sh.cClient.Get(context.TODO(), types.NamespacedName{
		Namespace: sh.activeRevision.GetNamespace(),
		Name:      shared.BuildProcessorDeployName(sh.topology.GetName(), pName, sh.activeRevision.Spec.Revision),
	}, &deployment)
	if err != nil {
		// this shouldn't ever occur, since we just created the object successfully
		// requeue if any error (notfound, anything else)
		return nil, err
	}
	// deployment found, check condition available field
	// this should normally always be set, requeue if not
	var condAvailable *appsv1.DeploymentCondition
	for _, c := range deployment.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			condAvailable = &c
		}
	}
	if condAvailable == nil {
		return &metav1.Condition{
			Type:    StatusDeploymentReady,
			Status:  "Unknown",
			Reason:  "NotReported",
			Message: "",
		}, nil
	}
	// propagate the available status for now
	return &metav1.Condition{
		Type:    StatusDeploymentReady,
		Status:  toCompatCondStatus(condAvailable.Status),
		Reason:  condAvailable.Reason,
		Message: condAvailable.Message,
	}, nil
}

func (sh *syncerHandler) determineProcessorStatus(newStatus *plumberv1alpha1.TopologyStatus, pName string) (bool, bool) {
	shouldRequeue := false
	updated := false
	depConds, found := newStatus.ProcessorStatuses[pName]
	// create new processor substatus if it is not present
	if !found {
		newConds := make([]metav1.Condition, 0)
		depConds = &newConds
		updated = true
	}
	deployStat, err := sh.deriveDeploymentReadyStatus(pName)
	if err != nil {
		sh.Log.Error(err, "failed to derive deployment status")
		shouldRequeue = true
	}
	if deployStat != nil {
		if !meta.IsStatusConditionPresentAndEqual(*depConds, deployStat.Type, deployStat.Status) {
			meta.SetStatusCondition(depConds, *deployStat)
			newStatus.ProcessorStatuses[pName] = depConds
			updated = true
		}
	}

	return updated, shouldRequeue
}

func (sh *syncerHandler) determineProcessorStatuses(newStatus *plumberv1alpha1.TopologyStatus) (bool, bool) {
	// create new status if not present
	updated := false
	if newStatus.ProcessorStatuses == nil {
		newStatus.ProcessorStatuses = make(map[string]*[]metav1.Condition)
	} else {
		nameMap := make(map[string]interface{})
		for k := range sh.activeRevision.Spec.Processors {
			nameMap[k] = nil
		}
		updated = pruneMappedConditions(nameMap, &newStatus.ProcessorStatuses)
	}
	shouldRequeue := false
	for pName := range sh.activeRevision.Spec.Processors {
		pStatUpdated, pShouldReq := sh.determineProcessorStatus(newStatus, pName)
		updated = updated || pStatUpdated
		shouldRequeue = shouldRequeue || pShouldReq
	}
	return updated, shouldRequeue
}

func boolToCondStr(b bool) metav1.ConditionStatus {
	if b {
		return metav1.ConditionTrue
	} else {
		return metav1.ConditionFalse
	}
}

func (sh *syncerHandler) determineGlobalStatus(newStatus *plumberv1alpha1.TopologyStatus) bool {
	if newStatus.Status == nil {
		newStatus.Status = make([]metav1.Condition, 0)
	}
	// loop over all components and determine if their states are ready
	// first, determine the number of components that should be marked as ready in the status, then count the number of actual ready components
	numRequired := 0
	//for range domainTopo.Processors {
	//	// processor either requires 2 or 3 components to be ready
	//	// 	-> 2 in case the processor has no interested processors (and thus needs no output topic)
	//	//if p.HasOutputTopic() {
	//	//	numRequired += 3
	//	//} else {
	//	//	numRequired += 2
	//	//
	//}
	numRequired += len(sh.activeRevision.Spec.Processors) + len(sh.activeRevision.Spec.Sinks) + len(sh.activeRevision.Spec.Sources)

	// count actual number of statuses present and ready
	condTypesToCheck := []string{StatusDeploymentReady, StatusReady}
	numActual := 0
	for _, ps := range newStatus.ProcessorStatuses {
		if ps == nil {
			continue
		}
		for _, cond := range *ps {
			if shared.Contains(condTypesToCheck, cond.Type) && cond.Status == metav1.ConditionTrue {
				numActual++
			}
		}
	}

	for _, sis := range newStatus.SinkStatuses {
		if sis == nil {
			continue
		}
		for _, cond := range *sis {
			if shared.Contains(condTypesToCheck, cond.Type) && cond.Status == metav1.ConditionTrue {
				numActual++
			}
		}
	}

	for _, sos := range newStatus.SourceStatuses {
		if sos == nil {
			continue
		}
		for _, cond := range *sos {
			if shared.Contains(condTypesToCheck, cond.Type) && cond.Status == metav1.ConditionTrue {
				numActual++
			}
		}
	}

	globalReady := numActual == numRequired
	reason := ""
	message := ""
	if !globalReady {
		reason = "NotAllComponentsReady"
		message = fmt.Sprintf("Only %d out of %d required components are ready", numActual, numRequired)
	} else {
		reason = "AllComponentsReady"
	}

	newGlobalReadyStat := metav1.Condition{
		Type:    StatusReady,
		Status:  boolToCondStr(globalReady),
		Reason:  reason,
		Message: message,
	}

	changed := false
	if !meta.IsStatusConditionPresentAndEqual(newStatus.Status, StatusReady, newGlobalReadyStat.Status) {
		meta.SetStatusCondition(&newStatus.Status, newGlobalReadyStat)
		changed = true
	}

	return changed
}

// updateState observes the current state of all objects generated by plumber within its namespace + topics in the plumber-kafka namespace..
// The function generates a new Status subresource based on these observations.
// To monitor the state of the full Topology & its subcomponents the metav1.Condition guidelines are used
// Currently, for the global & subcomponents only a "Ready" condition is implemented
// 		-> this logic should possibly live in the actual object generation loop, where for each object, based on observations, actions are taken on the status & object itself
// https://github.com/kubernetes/apimachinery/blob/master/pkg/api/meta/conditions.go
func (sh *syncerHandler) updateActiveStatus() error {
	//defer shared.Elapsed(sh.Log, "Updating state")()

	var newStat plumberv1alpha1.TopologyStatus
	sh.topology.Status.DeepCopyInto(&newStat)

	updated := false
	shouldRequeue := false

	sh.determineSourceStates(&newStat)
	updatedProcessorStat, pShouldRequeue := sh.determineProcessorStatuses(&newStat)
	updated = updated || updatedProcessorStat
	shouldRequeue = shouldRequeue || pShouldRequeue
	sh.determineSinkStatuses(&newStat)

	updatedGlobalStat := sh.determineGlobalStatus(&newStat)
	updated = updated || updatedGlobalStat

	if updated {
		var newTopo plumberv1alpha1.Topology
		sh.topology.DeepCopyInto(&newTopo)
		newTopo.Status = newStat
		err := sh.cClient.Status().Patch(context.TODO(), &newTopo, client.MergeFrom(&sh.topology), &client.PatchOptions{FieldManager: FieldManager})
		if err != nil {
			return errors.Wrap(err, "failed to patch topology status")
		}
	}

	if shouldRequeue {
		return fmt.Errorf("something went wrong while updating state")
	}
	return nil
}
