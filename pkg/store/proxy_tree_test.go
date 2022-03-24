// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type mockSeriesSet struct {
	series []labels.Labels
	i      int
}

func (m *mockSeriesSet) Next() bool {
	ret := m.i < len(m.series)
	m.i++
	return ret
}

func (m *mockSeriesSet) Err() error {
	return nil
}

func (m *mockSeriesSet) Warnings() storage.Warnings {
	return []error{}
}

func (m *mockSeriesSet) At() storage.Series {
	return &mockSeries{lbls: m.series[m.i]}
}

type mockSeries struct {
	lbls labels.Labels
}

func (s *mockSeries) Iterator() chunkenc.Iterator {
	return nil
}

func (s *mockSeries) Labels() labels.Labels {
	return s.lbls
}

func TestTournamentTreeBuild(t *testing.T) {
	// Tree of size 5.
	{
		tt := NewProxyTournamentTree([]storage.SeriesSet{
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "aaa",
							Value: "aaa",
						},
					},
				},
			},
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "aaa",
							Value: "bbb",
						},
					},
				},
			},
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "ddd",
							Value: "eee",
						},
					},
				},
			},
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "ddd",
							Value: "fff",
						},
					},
				},
			},
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "ddd",
							Value: "ggg",
						},
					},
				},
			},
		})
		testutil.Equals(t, 6, len(tt.auxiliaryNodes))
		loser := tt.auxiliaryNodes[5].At().Labels()
		testutil.Equals(t, labels.Labels{
			labels.Label{
				Name:  "aaa",
				Value: "aaa",
			},
		}, loser)
	}

	// Tree of size 3.
	{
		tt := NewProxyTournamentTree([]storage.SeriesSet{
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "test",
							Value: "foo",
						},
					},
				},
			},
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "test",
							Value: "bar",
						},
					},
				},
			},
			&mockSeriesSet{
				series: []labels.Labels{
					{
						labels.Label{
							Name:  "test",
							Value: "baz",
						},
					},
				},
			},
		})

		testutil.Equals(t, 3, len(tt.auxiliaryNodes))
		loser := tt.auxiliaryNodes[2].At().Labels()
		testutil.Equals(t, labels.Labels{
			labels.Label{
				Name:  "test",
				Value: "bar",
			},
		}, loser)
	}
}
