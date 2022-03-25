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
	m.i++
	return m.i < len(m.series)
}

func (m *mockSeriesSet) Err() error {
	return nil
}

func (m *mockSeriesSet) Warnings() storage.Warnings {
	return []error{}
}

func (m *mockSeriesSet) At() storage.Series {
	if m.i >= len(m.series) {
		return nil
	}
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

func TestTournamentTreePop(t *testing.T) {
	// Tree of size 2.
	{
		tt := NewProxyTournamentTree(
			[]storage.SeriesSet{
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "baa",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "bab",
							},
						},
					},
				},
			},
		)

		ssLbls := tt.Pop().At().Labels()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "baa"}}, ssLbls)

		tt.Fix()
		ssLbls = tt.Pop().At().Labels()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "bab"}}, ssLbls)

		tt.Fix()
		ss := tt.Pop()
		testutil.Equals(t, nil, ss)
	}

	// Tree of size 3.
	{
		tt := NewProxyTournamentTree(
			[]storage.SeriesSet{
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "baa",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "bab",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "caa",
							},
						},
					},
				},
			},
		)

		ssLbls := tt.Pop().At().Labels()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "baa"}}, ssLbls)

		tt.Fix()
		ssLbls = tt.Pop().At().Labels()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "bab"}}, ssLbls)

		tt.Fix()
		ssLbls = tt.Pop().At().Labels()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "caa"}}, ssLbls)

		tt.Fix()
		ss := tt.Pop()
		testutil.Equals(t, nil, ss)
	}
}

func TestTournamentTreeBuild(t *testing.T) {
	for _, tcase := range []struct {
		series      []storage.SeriesSet
		lenAuxNodes int
		loser       labels.Labels
	}{
		{
			series: []storage.SeriesSet{
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "baa",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "bab",
							},
						},
					},
				},
			},
			lenAuxNodes: 1,
			loser: labels.Labels{
				labels.Label{
					Name:  "test",
					Value: "baa",
				},
			},
		},
		{
			series: []storage.SeriesSet{
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
			},
			lenAuxNodes: 6,
			loser: labels.Labels{
				labels.Label{
					Name:  "aaa",
					Value: "aaa",
				},
			},
		},
		{
			series: []storage.SeriesSet{
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
			},
			lenAuxNodes: 3,
			loser: labels.Labels{
				labels.Label{
					Name:  "test",
					Value: "bar",
				},
			},
		},
	} {
		tt := NewProxyTournamentTree(tcase.series)
		testutil.Equals(t, tcase.lenAuxNodes, len(tt.auxiliaryNodes))
		loserLabels := tt.auxiliaryNodes[len(tt.auxiliaryNodes)-1].ss.At().Labels()
		testutil.Equals(t, tcase.loser, loserLabels)
	}
}
