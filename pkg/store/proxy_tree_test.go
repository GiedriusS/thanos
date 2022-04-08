// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
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

func (m *mockSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	if m.i >= len(m.series) {
		return nil, nil
	}
	return m.series[m.i], nil
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

func init() {
	rand.Seed(500)
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestTournamentTreeCharacteristics(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(300)
	parameters.MinSuccessfulTests = 500
	properties := gopter.NewProperties(parameters)

	properties.Property("we can always pop at least the number of nodes", prop.ForAllNoShrink(
		func(numberOfNodes, eachNodeLen int64) (bool, error) {
			ss := []storepb.SeriesSet{}

			for i := 0; i < int(numberOfNodes); i++ {
				m := &mockSeriesSet{
					series: []labels.Labels{},
				}

				for j := 0; j < int(eachNodeLen); j++ {
					m.series = append(m.series, labels.FromStrings(RandStringRunes(10), RandStringRunes(10)))
				}

				sort.Slice(m.series, func(i, j int) bool {
					return labels.Compare(m.series[i], m.series[j]) < 0
				})

				ss = append(ss, m)
			}

			tt := NewProxyTournamentTree(ss)

			var prvsLbls labels.Labels

			total := numberOfNodes * eachNodeLen

			for total > 0 {
				n := tt.Pop()
				if n == nil {
					return false, fmt.Errorf("%d iterations done out of %d (got nil)", -(total - (numberOfNodes * eachNodeLen)), (numberOfNodes * eachNodeLen))
				}

				lbls, _ := n.At()
				if prvsLbls != nil {
					if labels.Compare(lbls, prvsLbls) > 0 {
						return false, fmt.Errorf("got unsorted labels (%v and then %v)", prvsLbls, lbls)
					}
				}
				prvsLbls = lbls

				total--
				tt.Fix()
			}

			return true, nil
		}, gen.Int64Range(1, 50), gen.Int64Range(1, 150),
	))

	properties.TestingRun(t)
}

func TestTournamentTreePop(t *testing.T) {
	// Tree of size 2.
	{
		tt := NewProxyTournamentTree(
			[]storepb.SeriesSet{
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

		ssLbls, _ := tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "baa"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "bab"}}, ssLbls)

		tt.Fix()
		ss := tt.Pop()
		testutil.Equals(t, nil, ss)
	}

	// Tree of size 3.
	{
		tt := NewProxyTournamentTree(
			[]storepb.SeriesSet{
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

		ssLbls, _ := tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "baa"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "bab"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "caa"}}, ssLbls)

		tt.Fix()
		ss := tt.Pop()
		testutil.Equals(t, nil, ss)
	}

	// Tree of size 10.
	{
		tt := NewProxyTournamentTree(
			[]storepb.SeriesSet{
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
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "cab",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "daa",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "dab",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "dac",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "dad",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "eaa",
							},
						},
					},
				},
				&mockSeriesSet{
					series: []labels.Labels{
						{
							labels.Label{
								Name:  "test",
								Value: "eab",
							},
						},
					},
				},
			},
		)

		ssLbls, _ := tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "baa"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "bab"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "caa"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "cab"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "daa"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "dab"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "dac"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "dad"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "eaa"}}, ssLbls)

		tt.Fix()
		ssLbls, _ = tt.Pop().At()
		testutil.Equals(t, labels.Labels{labels.Label{Name: "test", Value: "eab"}}, ssLbls)
		tt.Fix()

		ss := tt.Pop()
		testutil.Equals(t, nil, ss)
	}
}

func TestTournamentTreeBuild(t *testing.T) {
	for i, tcase := range []struct {
		series      []storepb.SeriesSet
		lenAuxNodes int
		loser       labels.Labels
	}{
		{
			series: []storepb.SeriesSet{
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
			series: []storepb.SeriesSet{
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
			series: []storepb.SeriesSet{
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
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			tt := NewProxyTournamentTree(tcase.series)
			testutil.Equals(t, tcase.lenAuxNodes, len(tt.auxiliaryNodes))
			loserLabels, _ := tt.auxiliaryNodes[len(tt.auxiliaryNodes)-1].ss.At()
			testutil.Equals(t, tcase.loser, loserLabels)
		})
	}
}
