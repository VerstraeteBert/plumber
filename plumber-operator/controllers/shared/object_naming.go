package shared

import "strconv"

func BuildTopoRevisionName(topoName string, revisionNum int64) string {
	return "topology-" + topoName + "-revision-" + strconv.FormatInt(revisionNum, 10)
}

func BuildProcessorDeployName(topoName string, processorName string, revisionNum int64) string {
	return withPlumberPrefix(buildProcessorObjBaseName(topoName, processorName, revisionNum) + "-deploy")
}

func BuildScaledObjName(topoName string, processorName string, revisionNum int64) string {
	return withPlumberPrefix(buildProcessorObjBaseName(topoName, processorName, revisionNum) + "-scaler")
}

func buildProcessorObjBaseName(topoName string, processorName string, revisionNum int64) string {
	return topoName + "-" + processorName + "-" + strconv.FormatInt(revisionNum, 10)
}

func BuildOutputTopicName(namespace string, topoName string, processorName string, revisionNum int64) string {
	return namespace + "-" + topoName + "-" + processorName + "-" + strconv.FormatInt(revisionNum, 10)
}

func BuildTopoPartRevisionName(topoPartName string, revisionNumber int64) string {
	return "topologypart-" + topoPartName + "-revision-" + strconv.FormatInt(revisionNumber, 10)
}

func withPlumberPrefix(suffix string) string {
	return "plumber-" + suffix
}
