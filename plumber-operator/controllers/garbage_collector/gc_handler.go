package garbage_collector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/VerstraeteBert/plumber-operator/controllers/shared"
	strimziv1beta1 "github.com/VerstraeteBert/plumber-operator/vendor-api/strimzi/v1beta1"
	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/api/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type GarbageCollectorHandler struct {
	cClient    client.Client
	Log        logr.Logger
	topology   *plumberv1alpha1.Topology
	uClient    client.Client
	kfkClients KafkaClients
}

func (gch *GarbageCollectorHandler) listPhasingOutRevisions() ([]plumberv1alpha1.TopologyRevision, error) {
	strRevNums := make([]string, len(gch.topology.Status.PhasingOutRevisions))
	for i, revNum := range gch.topology.Status.PhasingOutRevisions {
		strRevNums[i] = strconv.FormatInt(revNum, 10)
	}
	phasingOutReq, _ := labels.NewRequirement(shared.RevisionNumberLabel, selection.In, strRevNums)
	topoMatchReq, _ := labels.NewRequirement(shared.ManagedByLabel, selection.In, []string{gch.topology.Name})
	listSelector := labels.NewSelector().Add(*phasingOutReq).Add(*topoMatchReq)
	var revList plumberv1alpha1.TopologyRevisionList
	err := gch.uClient.List(context.TODO(), &revList, client.MatchingLabelsSelector{Selector: listSelector}, client.InNamespace((*gch.topology).GetNamespace()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to list all phasing out topology revisions")
	}
	return revList.Items, nil
}

// an active processor is a processor which's deployment still exists
func (gch *GarbageCollectorHandler) listActiveProcessorsForRevision(rev plumberv1alpha1.TopologyRevision) (map[string]interface{}, error) {
	phasingOutReq, _ := labels.NewRequirement(shared.RevisionNumberLabel, selection.In, []string{strconv.FormatInt(rev.Spec.Revision, 10)})
	topoMatchReq, _ := labels.NewRequirement(shared.ManagedByLabel, selection.In, []string{gch.topology.Name})
	listSelector := labels.NewSelector().Add(*phasingOutReq).Add(*topoMatchReq)
	var deployList appsv1.DeploymentList
	err := gch.cClient.List(context.TODO(), &deployList, client.MatchingLabelsSelector{Selector: listSelector}, client.InNamespace((*gch.topology).GetNamespace()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to list all active processors for a given revision")
	}
	activeProcs := make(map[string]interface{})
	for _, deploy := range deployList.Items {
		procOwner, found := deploy.Labels[shared.ProcessorNameLabel]
		if !found {
			continue
		}
		_, procExists := rev.Spec.Processors[procOwner]
		if procExists {
			activeProcs[procOwner] = true
		}
	}
	return activeProcs, nil
}

// TODO would be better to clean up all processor objects in at once per reconcile loop; deleteAllOf would only incur one API server call vs N calls with N being the amount processors that were just determined to be finished in the reconcile
func (gch *GarbageCollectorHandler) cleanUpProcessor(processorName string, topoRev plumberv1alpha1.TopologyRevision) error {
	alreadyDeleted := false
	var procScaledObj kedav1alpha1.ScaledObject
	err := gch.cClient.Get(context.TODO(), client.ObjectKey{
		Namespace: topoRev.GetNamespace(),
		Name:      shared.BuildScaledObjName(gch.topology.GetName(), processorName, topoRev.Spec.Revision),
	}, &procScaledObj)
	if err != nil {
		if kerrors.IsNotFound(err) {
			alreadyDeleted = true
		} else {
			return errors.Wrap(err, "failed to get scaled object")
		}
	}
	if !alreadyDeleted {
		err = gch.cClient.Delete(context.TODO(), &procScaledObj)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return errors.Wrap(err, "failed to delete scaled object")
			}
		}
	}

	alreadyDeleted = false
	var procDep appsv1.Deployment
	err = gch.cClient.Get(context.TODO(), client.ObjectKey{
		Namespace: topoRev.GetNamespace(),
		Name:      shared.BuildProcessorDeployName(gch.topology.GetName(), processorName, topoRev.Spec.Revision),
	}, &procDep)
	if err != nil {
		if kerrors.IsNotFound(err) {
			alreadyDeleted = true
		} else {
			return errors.Wrap(err, "failed to get deployment")
		}
	}
	if !alreadyDeleted {
		err = gch.cClient.Delete(context.TODO(), &procDep)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return errors.Wrap(err, "failed to delete deployment")
			}
		}
	}

	return nil
}

func getSourceConnectedProcessorsSuccessors(rev plumberv1alpha1.TopologyRevision) []string {
	sourceConn := make([]string, 0)
	for procName, procObj := range rev.Spec.Processors {
		if _, connectedToSource := rev.Spec.Sources[procObj.InputFrom]; connectedToSource {
			sourceConn = append(sourceConn, procName)
		}
	}
	succ := make([]string, 0)
	for _, procName := range sourceConn {
		succ = append(succ, getSuccessors(procName, rev.Spec.Processors)...)
	}
	return succ
}

func getSuccessors(procName string, processors map[string]plumberv1alpha1.ComposedProcessor) []string {
	successors := make([]string, 0)
	for candProcName, candProc := range processors {
		if candProc.InputFrom == procName {
			successors = append(successors, candProcName)
		}
	}
	return successors
}

func (gch *GarbageCollectorHandler) getPartitions(topic string) ([]int32, error) {
	topicsMetadata, err := gch.kfkClients.admin.DescribeTopics([]string{topic})
	if err != nil {
		return nil, errors.Wrap(err, "error describing topics")

	}

	partitionMetadata := topicsMetadata[0].Partitions
	partitions := make([]int32, len(partitionMetadata))
	for i, p := range partitionMetadata {
		partitions[i] = p.ID
	}

	return partitions, nil
}

func (gch *GarbageCollectorHandler) getConsumerOffsets(partitions []int32, topic string, consumergroup string) (map[int32]int64, error) {
	offsets, err := gch.kfkClients.admin.ListConsumerGroupOffsets(consumergroup, map[string][]int32{
		topic: partitions,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error listing consumer group offsets")
	}
	consumerOffsets := make(map[int32]int64)
	for _, part := range partitions {
		block := offsets.GetBlock(topic, part)
		if block == nil {
			return nil, fmt.Errorf("failed to find consumer offset for partition %s in topic %s", strconv.FormatInt(int64(part), 10), topic)
		}
		consumerOffsets[part] = block.Offset
	}
	return consumerOffsets, nil
}

func (gch *GarbageCollectorHandler) getProducerOffsets(partitions []int32, topic string) (map[int32]int64, error) {
	requests := make(map[*sarama.Broker]*sarama.OffsetRequest)

	for _, partitionID := range partitions {
		// can potentially get this from the initial partition queries instead
		broker, err := gch.kfkClients.client.Leader(topic, partitionID)
		if err != nil {
			return nil, err
		}

		request, ok := requests[broker]
		if !ok {
			request = &sarama.OffsetRequest{Version: 1}
			requests[broker] = request
		}

		request.AddBlock(topic, partitionID, sarama.OffsetNewest, 1)
	}

	offsets := make(map[int32]int64)

	for broker, request := range requests {
		response, err := broker.GetAvailableOffsets(request)

		if err != nil {
			return nil, err
		}

		for _, blocks := range response.Blocks {
			for partitionID, block := range blocks {
				if block.Err != sarama.ErrNoError {
					return nil, block.Err
				}
				offsets[partitionID] = block.Offset
			}
		}
	}
	return offsets, nil
}

func (gch *GarbageCollectorHandler) identifyProcessorInputTopic(processorName string, rev plumberv1alpha1.TopologyRevision) (string, error) {
	procObj, found := rev.Spec.Processors[processorName]
	if !found {
		return "", fmt.Errorf("processor %s not found on topologyrevision", processorName)
	}
	if _, isProcInput := rev.Spec.Processors[procObj.InputFrom]; isProcInput {
		return shared.BuildOutputTopicName(rev.GetNamespace(), gch.topology.GetName(), procObj.InputFrom, rev.Spec.Revision), nil
	}
	if inputSource, isSourceInput := rev.Spec.Sources[procObj.InputFrom]; isSourceInput {
		return inputSource.Topic, nil
	}
	return "", fmt.Errorf("failed to identify input topic: no matching source object")
}

func (gch *GarbageCollectorHandler) deleteAllProcessorOutputTopics(topoRev plumberv1alpha1.TopologyRevision) error {
	err := gch.cClient.DeleteAllOf(
		context.TODO(),
		&strimziv1beta1.KafkaTopic{},
		client.InNamespace(shared.KafkaNamespace),
		client.MatchingLabels{
			shared.ManagedByLabel:      gch.topology.GetName(),
			shared.RevisionNumberLabel: strconv.FormatInt(topoRev.Spec.Revision, 10),
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to delete all processor output topics")
	}
	return nil
}

func (gch *GarbageCollectorHandler) persistNewPhasingOutList(phasingOutList []int64) error {
	newTopo := gch.topology.DeepCopy()
	newTopo.Status.PhasingOutRevisions = phasingOutList
	err := gch.cClient.Status().Patch(context.TODO(), newTopo, client.MergeFrom(gch.topology), client.FieldOwner("plumber-gc"))
	if err != nil {
		return errors.Wrap(err, "failed to patch phasingoutlist for topology")
	}
	return nil
}

func (gch *GarbageCollectorHandler) handle() (reconcile.Result, error) {
	if gch.topology.Status.PhasingOutRevisions == nil || len(gch.topology.Status.PhasingOutRevisions) == 0 {
		// if phasing out revisions is nil or empty list, there's no work to do
		// this is an important check, as the list labelSelector will throw an error if the list is empty or nil
		return reconcile.Result{}, nil
	}
	// list all topologyRevisions in phasing out list
	phasingOutList, err := gch.listPhasingOutRevisions()
	if err != nil {
		return reconcile.Result{}, err
	}
	newPhasingOutList := make([]int64, len(phasingOutList))
	for i, rev := range phasingOutList {
		newPhasingOutList[i] = rev.Spec.Revision
	}
	shouldRequeue := false
	phasingOutChanged := false
	for _, topoRev := range phasingOutList {
		activeProcSet, err := gch.listActiveProcessorsForRevision(topoRev)
		if err != nil {
			gch.Log.Error(err, "failed to list active processors for revision", "revision", topoRev.Spec.Revision)
			shouldRequeue = true
			continue
		}
		if len(activeProcSet) == 0 {
			// no more active processors -> done!
			// 1. delete all intermediary topics!
			err := gch.deleteAllProcessorOutputTopics(topoRev)
			if err != nil {
				shouldRequeue = true
				continue
			}
			// 2. delete from phasing out list
			for i, revNum := range newPhasingOutList {
				if revNum == topoRev.Spec.Revision {
					newPhasingOutList = append(newPhasingOutList[:i], newPhasingOutList[i+1:]...)
					break
				}
			}
			phasingOutChanged = true
			continue
		}
		workQueue := getSourceConnectedProcessorsSuccessors(topoRev)
		for len(workQueue) > 0 {
			// peek
			currProcName := workQueue[0]
			// pop
			workQueue = workQueue[1:]
			if _, isActive := activeProcSet[currProcName]; !isActive {
				// if it is not active, this one has already been cleaned up. We can continue with the successor processors
				// enqueue
				workQueue = append(workQueue, getSuccessors(currProcName, topoRev.Spec.Processors)...)
			}
			currProc := topoRev.Spec.Processors[currProcName]
			inputTopic, _ := gch.identifyProcessorInputTopic(currProcName, topoRev)
			partIds, err := gch.getPartitions(inputTopic)
			if err != nil {
				// TODO should depend on the actual error being thrown; brokers unavailable we can retry, a topic not existing will just put this in a constant retry loop
				shouldRequeue = true
				continue
			}
			consumerOffsets, err := gch.getConsumerOffsets(partIds, inputTopic, currProc.Internal.ConsumerGroup)
			if err != nil {
				gch.Log.Error(err, "failed to get consumer offsets", "topic", inputTopic)
				shouldRequeue = true
				continue
			}
			producerOffsets, err := gch.getProducerOffsets(partIds, inputTopic)
			if err != nil {
				gch.Log.Error(err, "failed to get producer offsets", "topic", inputTopic)
				shouldRequeue = true
				continue
			}
			foundNonCatchUp := false
			for part, consOffset := range consumerOffsets {
				if foundNonCatchUp {
					break
				}
				prodOffset := producerOffsets[part]
				if consOffset < prodOffset {
					foundNonCatchUp = true
				}
			}
			if foundNonCatchUp {
				shouldRequeue = true
				continue
			}
			//  all caught up
			//	1. delete processor objects (first scaled object, then deployment. Topics are deleted when full topology is done)
			err = gch.cleanUpProcessor(currProcName, topoRev)
			if err != nil {
				gch.Log.Error(err, "failed to delete processor objects", "rev", topoRev.Spec.Revision, "processor", currProc)
				shouldRequeue = true
				continue
			}
			// 	2. enqueue sucessors of processor
			workQueue = append(workQueue, getSuccessors(currProcName, topoRev.Spec.Processors)...)
		}
	}
	if phasingOutChanged {
		// persist new phasing out list on status
		err := gch.persistNewPhasingOutList(newPhasingOutList)
		if err != nil {
			gch.Log.Error(err, "failed to persist new phasing out list")
			shouldRequeue = true
		}
	}
	shouldRequeue = shouldRequeue || len(newPhasingOutList) > 0
	if shouldRequeue {
		return reconcile.Result{RequeueAfter: time.Second * 30}, nil
	}
	return reconcile.Result{}, nil
}
