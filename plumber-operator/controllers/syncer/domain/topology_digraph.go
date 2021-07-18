package domain

import (
	"fmt"
)

type TopoGraph struct {
	nodes        []TopoNode
	adjList      [][]int
	nameIndexMap map[string]int
	size         int
}

func InitTopoGraph(size int) TopoGraph {
	return TopoGraph{
		nodes:        make([]TopoNode, size),
		adjList:      make([][]int, size),
		nameIndexMap: make(map[string]int),
		size:         0,
	}
}

func (tg *TopoGraph) AddNode(name string, compTy ComponentType) {
	tg.nodes[tg.size] = TopoNode{
		name:     name,
		nodeType: compTy,
	}
	tg.nameIndexMap[name] = tg.size
	tg.size++
}

func (tg *TopoGraph) AddEdge(fromCompName string, toCompName string) {
	fromIdx := tg.nameIndexMap[fromCompName]
	toIdx := tg.nameIndexMap[toCompName]
	tg.adjList[fromIdx] = append(tg.adjList[fromIdx], toIdx)
}

type topoMark int

const (
	MarkNone topoMark = iota
	MarkTemp
	MarkPerm
)

func (tg *TopoGraph) TopoSort() error {
	marks := make([]topoMark, tg.size)
	for i := range marks {
		marks[i] = MarkNone
	}
	for currNode := range tg.nodes {
		if err := tg.TopoSortHelper(currNode, marks); err != nil {
			return err
		}
	}

	return nil
}

func (tg *TopoGraph) TopoSortHelper(currNode int, marks []topoMark) error {
	if marks[currNode] == MarkPerm {
		return nil
	}
	if marks[currNode] == MarkTemp {
		return fmt.Errorf("the topology contains a cycle")
	}
	marks[currNode] = MarkTemp
	for _, adjNode := range tg.adjList[currNode] {
		if err := tg.TopoSortHelper(adjNode, marks); err != nil {
			return err
		}
	}
	marks[currNode] = MarkPerm
	return nil
}

type TopoNode struct {
	name     string
	nodeType ComponentType
}
