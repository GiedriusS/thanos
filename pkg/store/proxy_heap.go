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

// dedupResponseHeap is a wrapper around ProxyResponseHeap
// that deduplicates identical chunks identified by the same labelset.
// It uses a hashing function to do that.
type dedupResponseHeap struct {
	h *ProxyResponseHeap

	responses []*storepb.SeriesResponse

	previousResponse *storepb.SeriesResponse
	previousNext     bool
}

func NewDedupResponseHeap(h *ProxyResponseHeap) *dedupResponseHeap {
	return &dedupResponseHeap{
		h:            h,
		previousNext: h.Next(),
	}
}

func (d *dedupResponseHeap) At() *storepb.SeriesResponse {
	defer func() {
		d.responses = d.responses[:0]
	}()
	if len(d.responses) == 0 {
		return nil
	} else if len(d.responses) == 1 {
		return d.responses[0]
	}
	chunkDedupMap := map[string]*storepb.AggrChunk{}

	for _, resp := range d.responses {
		for _, chk := range resp.GetSeries().Chunks {
			h := chk.Hash()

			if _, ok := chunkDedupMap[h]; !ok {
				chk := chk
				chunkDedupMap[h] = &chk
			}
		}
	}

	finalChunks := make([]storepb.AggrChunk, len(chunkDedupMap))

	for _, chk := range chunkDedupMap {
		finalChunks = append(finalChunks, *chk)
	}

	return storepb.NewSeriesResponse(&storepb.Series{
		Labels: d.responses[0].GetSeries().Labels,
		Chunks: finalChunks,
	})
}

func (d *dedupResponseHeap) Next() bool {
	if !d.previousNext {
		return len(d.responses) > 0
	}

	var resp *storepb.SeriesResponse
	if d.previousResponse != nil {
		resp = d.previousResponse
		d.previousResponse = nil
	} else {
		resp = d.h.At()
	}

	var nextHeap bool
	defer func(next *bool) {
		d.previousNext = *next
	}(&nextHeap)

	d.responses = d.responses[:0]
	d.responses = append(d.responses, resp)

	if resp.GetSeries() == nil {
		d.previousResponse = resp
		return true
	}

	for {
		nextHeap = d.h.Next()
		if !nextHeap {
			break
		}
		resp = d.h.At()
		if resp.GetSeries() == nil {
			d.previousResponse = resp
			break
		}

		lbls := resp.GetSeries().Labels
		lastLbls := d.responses[len(d.responses)-1].GetSeries().Labels

		if labels.Compare(labelpb.ZLabelsToPromLabels(lbls), labelpb.ZLabelsToPromLabels(lastLbls)) == 0 {
			d.responses = append(d.responses, resp)
		} else {
			// This one is different. It will be taken care of via the next Next() call.
			d.previousResponse = resp
			break
		}
	}

	return true
}

// ProxyResponseHeap is a heap for storepb.SeriesSets.
// It performs k-way merge between all of those sets.
// TODO(GiedriusS): can be improved with a tournament tree.
// This is O(n*logk) but can be Theta(n*logk). However,
// tournament trees need n-1 auxiliary nodes so there
// might not be much of a difference.
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
