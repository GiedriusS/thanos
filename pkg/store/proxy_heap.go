// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"container/heap"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// ProxyResponseHeap is a heap for storepb.SeriesSets.
// It performs k-way merge between all of those sets.
// TODO(GiedriusS): can be improved with a tournament tree.
// This is O(n*logk) but can be Theta(n*logk).
type ProxyResponseHeap []ProxyResponseHeapNode

func (h *ProxyResponseHeap) Less(i, j int) bool {
	iResp := (*h)[i].rs.At()
	jResp := (*h)[j].rs.At()

	if iResp.GetSeries() != nil && jResp.GetSeries() != nil {
		iLbls := labelpb.ZLabelsToPromLabels(iResp.GetSeries().Labels)
		jLbls := labelpb.ZLabelsToPromLabels(jResp.GetSeries().Labels)
		return labels.Compare(iLbls, jLbls) < 0
	} else if iResp.GetSeries() == nil && jResp.GetSeries() != nil {
		return true
	} else if iResp.GetSeries() != nil && jResp.GetSeries() == nil {
		return false
	}
	// If it is not a series then the order does not matter.
	return false
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
	rs *respSet
}

func NewProxyResponseHeap(seriesSets ...*respSet) *ProxyResponseHeap {
	ret := make(ProxyResponseHeap, 0, len(seriesSets))

	for _, ss := range seriesSets {
		ss := ss
		ret.Push(ProxyResponseHeapNode{rs: ss})
	}

	heap.Init(&ret)

	return &ret
}

func (h *ProxyResponseHeap) Next() bool {
	return !h.Empty()
}

func (h *ProxyResponseHeap) At() *storepb.SeriesResponse {
	min := h.Min().rs

	atResp := min.At()

	if min.Next() {
		heap.Fix(h, 0)
	} else {
		heap.Remove(h, 0)
	}

	return atResp
}

func (h *ProxyResponseHeap) Err() error {
	return nil
}

type respSet struct {
	responses []*storepb.SeriesResponse
	i         int
}

func (ss *respSet) Next() bool {
	ss.i++
	return ss.i < len(ss.responses)
}

func (ss *respSet) Err() error {
	return nil
}

func (ss *respSet) Warnings() storage.Warnings {
	return nil
}

func (ss *respSet) At() *storepb.SeriesResponse {
	return ss.responses[ss.i]
}
