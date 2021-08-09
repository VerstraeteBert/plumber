package garbage_collector

import (
	"context"
	"strconv"

	"github.com/Shopify/sarama"
	plumberv1alpha1 "github.com/VerstraeteBert/plumber-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
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

const (
	RevisionManagedByLabel string = "plumber.ugent.be/managed-by"
	RevisionNumberLabel    string = "plumber.ugent.be/revision-number"
	ProcessorNameLabel     string = "plumber.ugent.be/processor-name"
)

func (gch *GarbageCollectorHandler) listPhasingOutRevisions() ([]plumberv1alpha1.TopologyRevision, error) {
	strRevNums := make([]string, len(gch.topology.Status.PhasingOutRevisions))
	for i, revNum := range gch.topology.Status.PhasingOutRevisions {
		strRevNums[i] = strconv.FormatInt(revNum, 10)
	}
	phasingOutReq, _ := labels.NewRequirement(RevisionNumberLabel, selection.In, strRevNums)
	topoMatchReq, _ := labels.NewRequirement(RevisionManagedByLabel, selection.In, []string{gch.topology.Name})
	listSelector := labels.NewSelector().Add(*phasingOutReq).Add(*topoMatchReq)
	var revList plumberv1alpha1.TopologyRevisionList
	err := gch.cClient.List(context.TODO(), &revList, client.MatchingLabelsSelector{listSelector}, client.InNamespace((*gch.topology).GetNamespace()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to list all phasing out topology revisions")
	}
	return revList.Items, nil
}

// an active processor is a processor which's deployment still exists
func (gch *GarbageCollectorHandler) listActiveProcessorsForRevision(rev plumberv1alpha1.TopologyRevision) (map[string]interface{}, error) {
	phasingOutReq, _ := labels.NewRequirement(RevisionNumberLabel, selection.In, []string{strconv.FormatInt(rev.Spec.Revision, 10)})
	topoMatchReq, _ := labels.NewRequirement(RevisionManagedByLabel, selection.In, []string{gch.topology.Name})
	listSelector := labels.NewSelector().Add(*phasingOutReq).Add(*topoMatchReq)
	var deployList appsv1.DeploymentList
	err := gch.cClient.List(context.TODO(), &deployList, client.MatchingLabelsSelector{listSelector}, client.InNamespace((*gch.topology).GetNamespace()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to list all active processors for a given revision")
	}
	activeProcs := make(map[string]interface{})
	for _, deploy := range deployList.Items {
		procOwner, found := deploy.Labels[ProcessorNameLabel]
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

func getSourceConnectedProcessors(rev plumberv1alpha1.TopologyRevision) []string {
	res := make([]string, 0)
	for procName, procObj := range rev.Spec.Processors {
		if _, connectedToSource := rev.Spec.Sources[procObj.InputFrom]; connectedToSource {
			res = append(res, procName)
		}
	}
	return res
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

func (gch *GarbageCollectorHandler) getConsumerOffsets(partitions []int32, topic string, consumergroup string) (*sarama.OffsetFetchResponse, error) {
	offsets, err := gch.kfkClients.admin.ListConsumerGroupOffsets(consumergroup, map[string][]int32{
		topic: partitions,
	})

	if err != nil {
		return nil, errors.Wrap(err, "error listing consumer group offsets")
	}

	return offsets, nil
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
	shouldRequeue := false
	phasingOutChanged := false
	for _, topoRev := range phasingOutList {
		activeProcSet, err := gch.listActiveProcessorsForRevision(topoRev)
		if err != nil {
			// okay to wait for the full 30s period to try again
			gch.Log.Error(err, "failed to list active processors for revision", "revision", topoRev.Spec.Revision)
			shouldRequeue = true
			continue
		}
		if len(activeProcSet) == 0 {
			// no more active processors -> done!
		}
		workQueue := getSourceConnectedProcessors(topoRev)
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
			currProc, _ := topoRev.Spec.Processors[currProcName]
			// TODO if processor is source connected it may not point to an internal kafka topic, must construct a client for that specific bootstraplist
			inputTopic := getInputTopic(currProcName, topoRev)
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

		}

	}
	return reconcile.Result{}, nil
}
