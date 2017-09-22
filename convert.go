package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/xperimental/tsdb-migrate/minilocal"
)

type seriesTime struct {
	Fingerprint model.Fingerprint
	From        model.Time
	To          model.Time
}

type fingerprintMap map[model.Fingerprint]bool

func (m fingerprintMap) Copy() fingerprintMap {
	copy := make(fingerprintMap)
	for k := range m {
		copy[k] = true
	}
	return copy
}

type timeGroup struct {
	From         model.Time
	To           model.Time
	Fingerprints fingerprintMap
}

func (g *timeGroup) String() string {
	fingerprints := &bytes.Buffer{}
	for f := range g.Fingerprints {
		fmt.Fprintf(fingerprints, "%s ", f)
	}
	return fmt.Sprintf("(%d, %d, %s)", g.From, g.To, fingerprints)
}

type metricSample struct {
	Metric model.Metric
	Time   model.Time
	Value  float64
}

func runInput(ctx context.Context, inputDir string) (chan metricSample, error) {
	ch := make(chan metricSample)
	go func() {
		defer close(ch)

		seriesMap, err := minilocal.LoadSeriesMap(inputDir)
		if err != nil {
			log.Printf("Error loading series map: %s", err)
			return
		}

		timeSortedFingerprints := sortedFingerprints(seriesMap)
		groupedByTime := groupByTime(timeSortedFingerprints)

		for i, group := range groupedByTime {
			fmt.Printf("%d -> %#v", i, group)
		}
	}()
	return ch, nil
}

func sortedFingerprints(seriesMap minilocal.SeriesMap) []seriesTime {
	fingerprints := []seriesTime{}
	for fpr, series := range seriesMap {
		fingerprints = append(fingerprints, seriesTime{
			Fingerprint: fpr,
			From:        series.First(),
			To:          series.Last(),
		})
	}

	sort.Slice(fingerprints, func(i, j int) bool {
		itemI := fingerprints[i]
		itemJ := fingerprints[j]

		if itemI.From == itemJ.From {
			return itemI.To < itemJ.To
		}

		return itemI.From < itemJ.From
	})

	return fingerprints
}

func groupByTime(series []seriesTime) []*timeGroup {
	if len(series) == 0 {
		return []*timeGroup{}
	}

	groups := []*timeGroup{
		{
			From: series[0].From,
			To:   series[0].To,
			Fingerprints: map[model.Fingerprint]bool{
				series[0].Fingerprint: true,
			},
		},
	}

	for i := 1; i < len(series); i++ {
		log.Printf("%d of %d series -> %d groups", i, len(series), len(groups))
		s := series[i]

		// Start after last group -> new group
		if s.From > groups[len(groups)-1].To {
			groups = append(groups, &timeGroup{
				From: s.From,
				To:   s.To,
				Fingerprints: map[model.Fingerprint]bool{
					s.Fingerprint: true,
				},
			})

			continue
		}

		before, including, after := findGroups(groups, s.From, s.To)

		for _, g := range including {
			g.Fingerprints[s.Fingerprint] = true
		}

		groups = combineGroups(before, including, after)
	}

	return groups
}

func findGroups(groups []*timeGroup, from, to model.Time) (before, including, after []*timeGroup) {
	before = []*timeGroup{}
	including = []*timeGroup{}
	after = []*timeGroup{}

	for i, g := range groups {
		if g.To < from {
			// Group is before timeslice
			before = append(before, g)
			continue
		}

		if g.From > to {
			// Group is after timeslice, so following are as well
			after = groups[i:]
			break
		}

		switch {
		case g.From < from && g.From < to && g.To > from && g.To < to:
			// from  {----} to
			// g  {----}
			//    {b}{i}{i}
			before = append(before, &timeGroup{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints.Copy(),
			})

			including = append(including, &timeGroup{
				From:         from,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &timeGroup{
				From:         g.To,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From < to && g.To > from && g.To < to:
			// from {-------} to
			//       g {-}
			//      {i}{i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.From,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &timeGroup{
				From:         g.To,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From < to && g.To > from && g.To > to:
			// from {----} to
			//       g {----}
			//      {i}{i}{a}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.From,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &timeGroup{
				From:         g.From,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})

			after = append(after, &timeGroup{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From == from && g.From < to && g.To > from && g.To < to:
			// from {----} to
			//    g {-}
			//      {i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &timeGroup{
				From:         g.To,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From < to && g.To > from && g.To == to:
			// from {----} to
			//       g {-}
			//      {i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.From,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &timeGroup{
				From:         g.From,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From == from && g.From < to && g.To > from && g.To > to:
			// from {---} to
			//    g {------}
			//      {-i-}{a}
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})

			after = append(after, &timeGroup{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From < from && g.From < to && g.To > from && g.To == to:
			// from {---} to
			// g {------}
			//   {b}{-i-}
			before = append(before, &timeGroup{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints.Copy(),
			})

			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From < from && g.From < to && g.To > from && g.To > to:
			// from {-} to
			// g {-------}
			//   {b}{i}{a}
			before = append(before, &timeGroup{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints.Copy(),
			})
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})
			after = append(after, &timeGroup{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From > from && g.From == to && g.To > from && g.To > to:
			// from {-} to
			//      g {-}
			//      {i}{a}
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
			after = append(after, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From < from && g.From < to && g.To == from && g.To < to:
			// from {-} to
			//  g {-}
			//    {b}{i}
			before = append(before, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From == to && g.To > from && g.To == to:
			// from {-} to
			//      g |
			//      {i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From == from && g.From < to && g.To == from && g.To < to:
			// from {-} to
			//    g |
			//    {i}{i}
			including = append(including, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From == from && g.To == to:
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})
		default:
			log.Fatalf("gf: %d gt: %d from: %d to: %d", g.From, g.To, from, to)
		}
	}

	return before, including, after
}

func combineGroups(groupGroups ...[]*timeGroup) []*timeGroup {
	if len(groupGroups) == 0 {
		return []*timeGroup{}
	}

	result := []*timeGroup{}
	var last *timeGroup
	for _, group := range groupGroups {
		if len(group) == 0 {
			continue
		}

		sort.Slice(group, func(i, j int) bool {
			itemI := group[i]
			itemJ := group[j]

			if itemI.From == itemJ.From {
				return itemI.To < itemJ.To
			}

			return itemI.From < itemJ.From
		})

		for _, g := range group {
			if last == nil {
				last = g
				continue
			}

			switch {
			case len(g.Fingerprints) == 0:
				continue
			case reflect.DeepEqual(last.Fingerprints, g.Fingerprints):
				last.To = g.To
			case last.From == g.From && last.To == g.To:
				for k := range g.Fingerprints {
					last.Fingerprints[k] = true
				}
			default:
				result = append(result, last)
				last = g
			}
		}
	}

	if last != nil {
		result = append(result, last)
	}
	return result
}
