// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type treeAuxNode struct {
	ss storepb.SeriesSet
	// Either one of these needs to be set.
	previousAuxIndex, previousNodeIndex int
}

// ProxyTournamentTree is a tournament tree
// for storage.SeriesSet nodes. It performs
// k-way merge between multiple storage.SeriesSet.
type ProxyTournamentTree struct {
	nodes          []storepb.SeriesSet
	auxiliaryNodes []*treeAuxNode

	lastChangedNodeIndex int
}

var infinity storepb.SeriesSet

func NewProxyTournamentTree(nodes []storepb.SeriesSet) *ProxyTournamentTree {
	if len(nodes)%2 != 0 {
		nodes = append(nodes, infinity)
	}

	tt := &ProxyTournamentTree{
		nodes:                nodes,
		lastChangedNodeIndex: -1,
	}

	var auxNodes int

	n := len(nodes)
	for n > 1 {
		if n%2 == 0 {
			auxNodes += n / 2
			n = n / 2
		} else {
			auxNodes += 1 + (n / 2)
			n = 1 + (n / 2)
		}
	}

	tt.auxiliaryNodes = make([]*treeAuxNode, int(auxNodes))

	tt.initialFix()

	return tt
}

func nextLevelNodeCount(n int) int {
	if n%2 == 0 {
		n = n / 2
	} else {
		if n == 1 {
			n = 0
		} else {
			n = 1 + (n / 2)
		}
	}

	return n
}

func (t *ProxyTournamentTree) initialFix() {
	lastLoserIndex := -1

	for left := 0; left < len(t.nodes); left += 2 {
		right := left + 1
		loserIndex := left / 2

		if t.nodes[right] == infinity && t.nodes[left] != infinity {
			t.auxiliaryNodes[loserIndex] = &treeAuxNode{
				ss:                t.nodes[left],
				previousNodeIndex: left,
				previousAuxIndex:  -1,
			}
		} else if t.nodes[left] == infinity && t.nodes[right] != infinity {
			t.auxiliaryNodes[loserIndex] = &treeAuxNode{
				ss:                t.nodes[right],
				previousNodeIndex: left,
				previousAuxIndex:  -1,
			}
		} else if t.nodes[left] == infinity && t.nodes[right] == infinity {
			t.auxiliaryNodes[loserIndex] = &treeAuxNode{
				ss: infinity,
			}
		} else {
			leftLbls, _ := t.nodes[left].At()
			rightLbls, _ := t.nodes[right].At()

			if labels.Compare(leftLbls, rightLbls) < 0 {
				t.auxiliaryNodes[loserIndex] = &treeAuxNode{
					ss:                t.nodes[left],
					previousNodeIndex: left,
					previousAuxIndex:  -1,
				}
			} else {
				t.auxiliaryNodes[loserIndex] = &treeAuxNode{
					ss:                t.nodes[right],
					previousNodeIndex: right,
					previousAuxIndex:  -1,
				}
			}
		}

		lastLoserIndex = loserIndex
	}

	// Build out other layers.
	if lastLoserIndex < len(t.auxiliaryNodes) {

		nodesInLevel := len(t.nodes)
		{
			// 2nd level (from 0).
			nodesInLevel = nextLevelNodeCount(nodesInLevel)
			nodesInLevel = nextLevelNodeCount(nodesInLevel)
		}

		var from, until int

		for nodesInLevel >= 1 {

			previousLevelIdx := from
			from, until = lastLoserIndex+1, lastLoserIndex+nodesInLevel

			for loserIdx := from; loserIdx <= until; loserIdx++ {

				var leftIdx, rightIdx int
				if previousLevelIdx%2 == 0 {
					leftIdx = previousLevelIdx
					rightIdx = previousLevelIdx + 1
				} else {
					leftIdx = previousLevelIdx - 1
					rightIdx = previousLevelIdx
				}

				nilAuxNode := func(i int) bool {
					return t.auxiliaryNodes[i] == nil || t.auxiliaryNodes[i].ss == infinity
				}
				if rightIdx >= from || nilAuxNode(rightIdx) {
					t.auxiliaryNodes[loserIdx] = &treeAuxNode{
						ss:                t.auxiliaryNodes[leftIdx].ss,
						previousAuxIndex:  leftIdx,
						previousNodeIndex: -1,
					}
				} else if nilAuxNode(leftIdx) && !nilAuxNode(rightIdx) {
					t.auxiliaryNodes[loserIdx] = &treeAuxNode{
						ss:                t.auxiliaryNodes[rightIdx].ss,
						previousAuxIndex:  rightIdx,
						previousNodeIndex: -1,
					}
				} else {
					leftLbls, _ := t.auxiliaryNodes[leftIdx].ss.At()
					rightLbls, _ := t.auxiliaryNodes[rightIdx].ss.At()

					if labels.Compare(leftLbls, rightLbls) < 0 {
						t.auxiliaryNodes[loserIdx] = &treeAuxNode{
							ss:                t.auxiliaryNodes[leftIdx].ss,
							previousAuxIndex:  leftIdx,
							previousNodeIndex: -1,
						}
					} else {
						t.auxiliaryNodes[loserIdx] = &treeAuxNode{
							ss:                t.auxiliaryNodes[rightIdx].ss,
							previousAuxIndex:  rightIdx,
							previousNodeIndex: -1,
						}
					}
				}

				previousLevelIdx += 2
			}

			lastLoserIndex = until
			if nodesInLevel%2 == 0 {
				nodesInLevel = nodesInLevel / 2
			} else {
				if nodesInLevel == 1 {
					nodesInLevel = 0
				} else {
					nodesInLevel = 1 + (nodesInLevel / 2)
				}
			}
		}
	}
}

// Fix fixes the tournament tree order after popping.
func (t *ProxyTournamentTree) Fix() {
	if t.lastChangedNodeIndex == -1 {
		panic("please call Fix() only after popping")
	}

	// Rebuild auxiliary nodes.

	// Advance the original node & delete it if nothing left.
	nextSeries := t.nodes[t.lastChangedNodeIndex].Next()
	if !nextSeries {
		t.nodes[t.lastChangedNodeIndex] = infinity
	}

	nodesInLevel := len(t.nodes)
	if nodesInLevel%2 == 0 {
		nodesInLevel = nodesInLevel / 2
	} else {
		nodesInLevel = 1 + (nodesInLevel / 2)
	}

	from, until := 0, nodesInLevel

	auxNodeOffset := nodesInLevel / 2

	var leftIdx, rightIdx int

	if t.lastChangedNodeIndex%2 == 0 {
		leftIdx = t.lastChangedNodeIndex
		rightIdx = t.lastChangedNodeIndex + 1
	} else {
		leftIdx = t.lastChangedNodeIndex - 1
		rightIdx = t.lastChangedNodeIndex
	}

	lookInNodes := true

	for until <= len(t.auxiliaryNodes) {
		candidateIdx := from + auxNodeOffset

		var left, right storepb.SeriesSet
		if lookInNodes {
			left = t.nodes[leftIdx]
			right = t.nodes[rightIdx]

		} else {
			left = t.auxiliaryNodes[leftIdx].ss
			right = t.auxiliaryNodes[rightIdx].ss
		}

		// Deduce the winner.
		if right == infinity && left != infinity {
			if lookInNodes {
				t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
					ss:                left,
					previousAuxIndex:  -1,
					previousNodeIndex: leftIdx,
				}
			} else {
				t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
					ss:                left,
					previousAuxIndex:  leftIdx,
					previousNodeIndex: -1,
				}
			}
		} else if right != infinity && left == infinity {
			if lookInNodes {
				t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
					ss:                right,
					previousAuxIndex:  -1,
					previousNodeIndex: rightIdx,
				}
			} else {
				t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
					ss:                right,
					previousAuxIndex:  rightIdx,
					previousNodeIndex: -1,
				}
			}
		} else if right == infinity && left == infinity {
			t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
				ss: infinity,
			}
		} else {
			lsetLeft, _ := left.At()
			lsetRight, _ := right.At()

			if labels.Compare(lsetLeft, lsetRight) < 0 {
				if lookInNodes {
					t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
						ss:                left,
						previousAuxIndex:  -1,
						previousNodeIndex: leftIdx,
					}
				} else {
					t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
						ss:                left,
						previousAuxIndex:  leftIdx,
						previousNodeIndex: -1,
					}
				}
			} else {
				if lookInNodes {
					t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
						ss:                right,
						previousAuxIndex:  -1,
						previousNodeIndex: rightIdx,
					}
				} else {
					t.auxiliaryNodes[candidateIdx] = &treeAuxNode{
						ss:                right,
						previousAuxIndex:  rightIdx,
						previousNodeIndex: -1,
					}
				}
			}
		}

		if nodesInLevel%2 == 0 {
			nodesInLevel = nodesInLevel / 2
		} else {
			nodesInLevel = 1 + (nodesInLevel / 2)
		}

		if lookInNodes {
			lookInNodes = false
		}

		from, until = until, until+nodesInLevel
	}
}

func (t *ProxyTournamentTree) Pop() storepb.SeriesSet {
	loserNode := t.auxiliaryNodes[len(t.auxiliaryNodes)-1]

	if loserNode != nil && loserNode.ss != infinity {
		curNodeIdx := len(t.auxiliaryNodes) - 1
		curNode := t.auxiliaryNodes[curNodeIdx]

		for {
			if curNode.previousAuxIndex != -1 {
				oldNodeIdx := curNodeIdx
				curNodeIdx = curNode.previousAuxIndex

				curNode = t.auxiliaryNodes[curNode.previousAuxIndex]
				t.auxiliaryNodes[oldNodeIdx] = nil
				continue
			}
			if curNode.previousNodeIndex != -1 {
				t.auxiliaryNodes[curNodeIdx] = nil
				t.lastChangedNodeIndex = curNode.previousNodeIndex
				break
			}
		}
		return loserNode.ss
	}
	return nil
}

type respSeriesSet struct {
	responses []*storepb.SeriesResponse
	i         int
}

var _ = (storepb.SeriesSet)(&respSeriesSet{})

func (ss *respSeriesSet) Next() bool {
	ss.i++
	return ss.i < len(ss.responses)
}

func (ss *respSeriesSet) Err() error {
	return nil
}

func (ss *respSeriesSet) Warnings() storage.Warnings {
	return nil
}

func (ss *respSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	return ss.responses[ss.i].GetSeries().PromLabels(), ss.responses[ss.i].GetSeries().Chunks

}
