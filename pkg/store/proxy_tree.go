// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type treeAuxNode struct {
	ss storage.SeriesSet
	// Either one of these needs to be set.
	previousAuxIndex, previousNodeIndex int
}

// ProxyTournamentTree is a tournament tree
// for storage.SeriesSet nodes. It performs
// k-way merge between multiple storage.SeriesSet.
type ProxyTournamentTree struct {
	nodes          []storage.SeriesSet
	auxiliaryNodes []*treeAuxNode

	lastChangedNodeIndex int
}

func NewProxyTournamentTree(nodes []storage.SeriesSet) *ProxyTournamentTree {
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

	tt.Fix()

	return tt
}

// Fix fixes the tournament tree order.
func (t *ProxyTournamentTree) Fix() {
	// Initial build.
	if t.lastChangedNodeIndex == -1 {
		lastLoserIndex := -1

		for left := 0; left < len(t.nodes); left += 2 {
			right := left + 1
			loserIndex := left / 2

			if right == len(t.nodes) {
				// Start wins automatically.
				t.auxiliaryNodes[loserIndex] = &treeAuxNode{
					ss:                t.nodes[left],
					previousNodeIndex: left,
					previousAuxIndex:  -1,
				}
			} else {

				startLbls := t.nodes[left].At().Labels()
				endLbls := t.nodes[right].At().Labels()

				if labels.Compare(startLbls, endLbls) < 0 {
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

		// Build out other stuff!
		if lastLoserIndex < len(t.auxiliaryNodes) {
			firstIter := true
			var oldLastLoserIndex int

			for lastLoserIndex != len(t.auxiliaryNodes)-1 {

				var from, until int

				if firstIter {
					firstIter = false
					from, until = 0, lastLoserIndex+1
				} else {
					from, until = oldLastLoserIndex+1, lastLoserIndex+1
				}

				oldLastLoserIndex = lastLoserIndex
				for left := from; left < until; left += 2 {
					right := left + 1
					loserIndex := lastLoserIndex + 1

					if right == until || t.auxiliaryNodes[right] == nil {
						// Start wins automatically.
						t.auxiliaryNodes[loserIndex] = &treeAuxNode{
							ss:                t.auxiliaryNodes[left].ss,
							previousAuxIndex:  left,
							previousNodeIndex: -1,
						}
					} else {

						startLbls := t.auxiliaryNodes[left].ss.At().Labels()
						endLbls := t.auxiliaryNodes[right].ss.At().Labels()

						if labels.Compare(startLbls, endLbls) < 0 {
							t.auxiliaryNodes[loserIndex] = &treeAuxNode{
								ss:                t.auxiliaryNodes[left].ss,
								previousAuxIndex:  left,
								previousNodeIndex: -1,
							}
						} else {
							t.auxiliaryNodes[loserIndex] = &treeAuxNode{
								ss:                t.auxiliaryNodes[right].ss,
								previousAuxIndex:  right,
								previousNodeIndex: -1,
							}
						}
					}

					lastLoserIndex++
				}
			}
		}
	} else {
		// Rebuild auxiliary nodes.

		// Advance the original node & delete it if nothing left.
		nextSeries := t.nodes[t.lastChangedNodeIndex].Next()
		if !nextSeries {
			t.nodes[t.lastChangedNodeIndex] = nil
		}

		// Initial rebuild.
		auxNodeIndex := t.lastChangedNodeIndex / 2

		var leftIdx, rightIdx int
		if t.lastChangedNodeIndex%2 == 0 {
			leftIdx = t.lastChangedNodeIndex
			rightIdx = t.lastChangedNodeIndex + 1
		} else {
			leftIdx = t.lastChangedNodeIndex - 1
			rightIdx = t.lastChangedNodeIndex
		}

		// Deduce the winner (loser).
		if (leftIdx >= 0 && rightIdx < len(t.nodes) && t.nodes[rightIdx] == nil && t.nodes[leftIdx] == nil) ||
			(leftIdx >= 0 && rightIdx >= len(t.nodes) && t.nodes[leftIdx] == nil) {
			t.auxiliaryNodes[auxNodeIndex] = nil
		} else if rightIdx >= len(t.nodes) || t.nodes[rightIdx] == nil {
			t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
				ss:                t.nodes[leftIdx],
				previousAuxIndex:  -1,
				previousNodeIndex: leftIdx,
			}
		} else if leftIdx < 0 || t.nodes[leftIdx] == nil {
			t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
				ss:                t.nodes[rightIdx],
				previousAuxIndex:  -1,
				previousNodeIndex: rightIdx,
			}
		} else {
			leftSS := t.nodes[leftIdx].At().Labels()
			rightSS := t.nodes[rightIdx].At().Labels()

			if labels.Compare(leftSS, rightSS) < 0 {
				t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
					ss:                t.nodes[leftIdx],
					previousAuxIndex:  -1,
					previousNodeIndex: leftIdx,
				}
			} else {
				t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
					ss:                t.nodes[rightIdx],
					previousAuxIndex:  -1,
					previousNodeIndex: rightIdx,
				}
			}
		}

		// Rebuild auxiliary nodes.
		nodesInLevel := len(t.nodes)
		if nodesInLevel%2 == 0 {
			nodesInLevel = nodesInLevel / 2
		} else {
			nodesInLevel = 1 + (nodesInLevel / 2)
		}

		// Already done after the first iter.
		if nodesInLevel == 1 {
			return
		}

		for {
			// Do actions with that level.

			if auxNodeIndex%2 == 0 {
				leftIdx = auxNodeIndex
				rightIdx = auxNodeIndex + 1
			} else {
				leftIdx = auxNodeIndex - 1
				rightIdx = auxNodeIndex
			}
			auxNodeIndex = nodesInLevel + (auxNodeIndex / 2)

			if (leftIdx >= 0 && rightIdx < len(t.auxiliaryNodes) && t.auxiliaryNodes[rightIdx] == nil && t.auxiliaryNodes[leftIdx] == nil) ||
				(leftIdx >= 0 && rightIdx >= len(t.auxiliaryNodes) && t.auxiliaryNodes[leftIdx] == nil) {
			} else if rightIdx >= len(t.auxiliaryNodes) || t.auxiliaryNodes[rightIdx] == nil {
				t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
					ss:                t.auxiliaryNodes[leftIdx].ss,
					previousAuxIndex:  leftIdx,
					previousNodeIndex: -1,
				}
			} else if leftIdx < 0 || t.auxiliaryNodes[leftIdx] == nil {
				t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
					ss:                t.auxiliaryNodes[rightIdx].ss,
					previousAuxIndex:  rightIdx,
					previousNodeIndex: -1,
				}
			} else {
				leftSS := t.auxiliaryNodes[leftIdx].ss.At().Labels()
				rightSS := t.auxiliaryNodes[rightIdx].ss.At().Labels()

				if labels.Compare(leftSS, rightSS) < 0 {
					t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
						ss:                t.auxiliaryNodes[leftIdx].ss,
						previousAuxIndex:  leftIdx,
						previousNodeIndex: -1,
					}
				} else {
					t.auxiliaryNodes[auxNodeIndex] = &treeAuxNode{
						ss:                t.auxiliaryNodes[rightIdx].ss,
						previousAuxIndex:  rightIdx,
						previousNodeIndex: -1,
					}
				}
			}

			// Start other.
			if nodesInLevel%2 == 0 {
				nodesInLevel = nodesInLevel / 2
			} else {
				nodesInLevel = 1 + (nodesInLevel / 2)
			}
			if nodesInLevel == 1 {
				break
			}
		}
	}
}

func (t *ProxyTournamentTree) Pop() storage.SeriesSet {
	loserNode := t.auxiliaryNodes[len(t.auxiliaryNodes)-1]

	if loserNode != nil {
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
