// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.
package store

import (
	"github.com/segmentio/bloom"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
)

func CalculateBloom(ihr indexheader.Reader, m, k uint) (*bloom.BloomFilter, error) {
	metricNames, err := ihr.LabelValues("__name__")
	if err != nil {
		return nil, err
	}

	bf := bloom.New(m, k)

	for _, metricName := range metricNames {
		bf.Add([]byte(metricName))
	}

	return bf, nil
}
