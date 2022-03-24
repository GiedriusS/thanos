// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// ProxyTournamentTree is a tournament tree
// for storage.SeriesSet nodes. It performs
// k-way merge between multiple storage.SeriesSet.
type ProxyTournamentTree struct {
	nodes          []storage.SeriesSet
	auxiliaryNodes []storage.SeriesSet

	lastChangedIndex int
}

func NewProxyTournamentTree(nodes []storage.SeriesSet) *ProxyTournamentTree {
	tt := &ProxyTournamentTree{
		nodes:            nodes,
		lastChangedIndex: -1,
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

	tt.auxiliaryNodes = make([]storage.SeriesSet, int(auxNodes))

	tt.Fix()

	return tt
}

// Fix fixes the tournament tree order.
func (t *ProxyTournamentTree) Fix() {
	// Initial build.
	if t.lastChangedIndex == -1 {
		lastLoserIndex := -1

		for left := 0; left < len(t.nodes); left += 2 {
			right := left + 1
			loserIndex := left / 2

			if right == len(t.nodes) {
				// Start wins automatically.
				t.auxiliaryNodes[loserIndex] = t.nodes[left]
			} else {

				startLbls := t.nodes[left].At().Labels()
				endLbls := t.nodes[right].At().Labels()

				if labels.Compare(startLbls, endLbls) < 0 {
					t.auxiliaryNodes[loserIndex] = t.nodes[left]
				} else {
					t.auxiliaryNodes[loserIndex] = t.nodes[right]
				}
			}

			lastLoserIndex = loserIndex
		}

		// Build out other stuff!
		if lastLoserIndex < len(t.auxiliaryNodes) {
			firstIter := true
			var oldLastLoserIndex int

			/*
				       a
				    b     b
				 y     y     y
				x x   x x   x
			*/
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
						t.auxiliaryNodes[loserIndex] = t.auxiliaryNodes[left]
					} else {

						startLbls := t.auxiliaryNodes[left].At().Labels()
						endLbls := t.auxiliaryNodes[right].At().Labels()

						if labels.Compare(startLbls, endLbls) < 0 {
							t.auxiliaryNodes[loserIndex] = t.auxiliaryNodes[left]
						} else {
							t.auxiliaryNodes[loserIndex] = t.auxiliaryNodes[right]
						}
					}

					lastLoserIndex++
				}
			}
		}
	}
}
