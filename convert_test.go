package main

import (
	"reflect"
	"testing"

	"github.com/prometheus/common/model"
)

func TestGroupByTime(t *testing.T) {
	for _, test := range []struct {
		desc         string
		fingerprints []seriesTime
		groups       []*timeGroup
	}{
		{
			desc:         "empty",
			fingerprints: []seriesTime{},
			groups:       []*timeGroup{},
		},
		{
			desc: "single series",
			fingerprints: []seriesTime{
				{
					From:        0,
					To:          1,
					Fingerprint: 0,
				},
			},
			groups: []*timeGroup{
				&timeGroup{
					From: 0,
					To:   1,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
					},
				},
			},
		},
		{
			desc: "non-contiguous",
			fingerprints: []seriesTime{
				{
					From:        0,
					To:          1,
					Fingerprint: 0,
				},
				{
					From:        2,
					To:          3,
					Fingerprint: 1,
				},
			},
			groups: []*timeGroup{
				&timeGroup{
					From: 0,
					To:   1,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
					},
				},
				&timeGroup{
					From: 2,
					To:   3,
					Fingerprints: map[model.Fingerprint]bool{
						1: true,
					},
				},
			},
		},
		{
			desc: "same",
			fingerprints: []seriesTime{
				{
					From:        0,
					To:          1,
					Fingerprint: 0,
				},
				{
					From:        0,
					To:          1,
					Fingerprint: 1,
				},
			},
			groups: []*timeGroup{
				&timeGroup{
					From: 0,
					To:   1,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
						1: true,
					},
				},
			},
		},
		{
			desc: "subset",
			fingerprints: []seriesTime{
				{
					From:        0,
					To:          3,
					Fingerprint: 0,
				},
				{
					From:        1,
					To:          2,
					Fingerprint: 1,
				},
			},
			groups: []*timeGroup{
				&timeGroup{
					From: 0,
					To:   1,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
					},
				},
				&timeGroup{
					From: 1,
					To:   2,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
						1: true,
					},
				},
				&timeGroup{
					From: 2,
					To:   3,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
					},
				},
			},
		},
		{
			desc: "same start longer first",
			fingerprints: []seriesTime{
				{
					From:        0,
					To:          3,
					Fingerprint: 0,
				},
				{
					From:        0,
					To:          2,
					Fingerprint: 1,
				},
			},
			groups: []*timeGroup{
				&timeGroup{
					From: 0,
					To:   2,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
						1: true,
					},
				},
				&timeGroup{
					From: 2,
					To:   3,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
					},
				},
			},
		},
		{
			desc: "same start longer second",
			fingerprints: []seriesTime{
				{
					From:        0,
					To:          2,
					Fingerprint: 1,
				},
				{
					From:        0,
					To:          3,
					Fingerprint: 0,
				},
			},
			groups: []*timeGroup{
				&timeGroup{
					From: 0,
					To:   2,
					Fingerprints: map[model.Fingerprint]bool{
						1: true,
						0: true,
					},
				},
				&timeGroup{
					From: 2,
					To:   3,
					Fingerprints: map[model.Fingerprint]bool{
						0: true,
					},
				},
			},
		},
	} {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			groups := groupByTime(test.fingerprints)

			if !reflect.DeepEqual(groups, test.groups) {
				t.Errorf("got %v, wanted %v", groups, test.groups)
			}
		})
	}
}
