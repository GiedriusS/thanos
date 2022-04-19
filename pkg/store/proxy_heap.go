// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"container/heap"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// ProxyResponseHeap is a heap for storepb.SeriesSets.
// It performs k-way merge between all of those sets.
// TODO(GiedriusS): can be improved with a tournament tree.
// This is O(n*logk) but can be Theta(n*logk).
type ProxyResponseHeap []ProxyResponseHeapNode

func (h *ProxyResponseHeap) Less(i, j int) bool {
	iLbls, _ := (*h)[i].ss.At()
	jLbls, _ := (*h)[j].ss.At()

	return labels.Compare(iLbls, jLbls) < 0
}

func (h *ProxyResponseHeap) Len() int {
	return len(*h)
}

func (h *ProxyResponseHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *ProxyResponseHeap) Push(x interface{}) {
	*h = append(*h, x.(ProxyResponseHeapNode))
}

func (h *ProxyResponseHeap) Pop() (v interface{}) {
	*h, v = (*h)[:h.Len()-1], (*h)[h.Len()-1]
	return
}

func (h *ProxyResponseHeap) Empty() bool {
	return h.Len() == 0
}

func (h *ProxyResponseHeap) Min() *ProxyResponseHeapNode {
	return &(*h)[0]
}

type ProxyResponseHeapNode struct {
	ss storepb.SeriesSet
}

func NewProxyResponseHeap(seriesSets ...storepb.SeriesSet) storepb.SeriesSet {
	ret := make(ProxyResponseHeap, 0, len(seriesSets))

	for _, ss := range seriesSets {
		ss := ss
		ret.Push(ProxyResponseHeapNode{ss: ss})
	}

	heap.Init(&ret)

	return &ret
}

func (h *ProxyResponseHeap) Next() bool {
	return !h.Empty()
}

func (h *ProxyResponseHeap) At() (labels.Labels, []storepb.AggrChunk) {
	min := h.Min().ss

	atLbls, atChks := min.At()

	if min.Next() {
		heap.Fix(h, 0)
	} else {
		heap.Remove(h, 0)
	}

	return atLbls, atChks
}

func (h *ProxyResponseHeap) Err() error {
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
