package main

import (
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

type timeGroup struct {
	From         model.Time
	To           model.Time
	Fingerprints []model.Fingerprint
}

func (g *timeGroup) String() string {
	return fmt.Sprintf("(%d, %d, %s)", g.From, g.To, g.Fingerprints)
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
			From:         series[0].From,
			To:           series[0].To,
			Fingerprints: []model.Fingerprint{series[0].Fingerprint},
		},
	}

	for i := 1; i < len(series); i++ {
		log.Printf("%d of %d series -> %d groups", i, len(series), len(groups))
		s := series[i]

		// Start after last group -> new group
		if s.From > groups[len(groups)-1].To {
			groups = append(groups, &timeGroup{
				From:         s.From,
				To:           s.To,
				Fingerprints: []model.Fingerprint{s.Fingerprint},
			})

			continue
		}

		before, including, after := findGroups(groups, s.From, s.To)

		for _, g := range including {
			g.Fingerprints = append(g.Fingerprints, s.Fingerprint)
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
				Fingerprints: g.Fingerprints,
			})

			including = append(including, &timeGroup{
				From:         from,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			}, &timeGroup{
				From:         g.To,
				To:           to,
				Fingerprints: []model.Fingerprint{},
			})
		case g.From > from && g.From < to && g.To > from && g.To < to:
			// from {-------} to
			//       g {-}
			//      {i}{i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.From,
				Fingerprints: []model.Fingerprint{},
			}, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			}, &timeGroup{
				From:         g.To,
				To:           to,
				Fingerprints: []model.Fingerprint{},
			})
		case g.From > from && g.From < to && g.To > from && g.To > to:
			// from {----} to
			//       g {----}
			//      {i}{i}{a}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.From,
				Fingerprints: []model.Fingerprint{},
			}, &timeGroup{
				From:         g.From,
				To:           to,
				Fingerprints: g.Fingerprints,
			})

			after = append(after, &timeGroup{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			})
		case g.From == from && g.From < to && g.To > from && g.To < to:
			// from {----} to
			//    g {-}
			//      {i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			}, &timeGroup{
				From:         g.To,
				To:           to,
				Fingerprints: []model.Fingerprint{},
			})
		case g.From > from && g.From < to && g.To > from && g.To == to:
			// from {----} to
			//       g {-}
			//      {i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           g.From,
				Fingerprints: []model.Fingerprint{},
			}, &timeGroup{
				From:         g.From,
				To:           to,
				Fingerprints: g.Fingerprints,
			})
		case g.From == from && g.From < to && g.To > from && g.To > to:
			// from {---} to
			//    g {------}
			//      {-i-}{a}
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints,
			})

			after = append(after, &timeGroup{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			})
		case g.From < from && g.From < to && g.To > from && g.To == to:
			// from {---} to
			// g {------}
			//   {b}{-i-}
			before = append(before, &timeGroup{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints,
			})

			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints,
			})
		case g.From < from && g.From < to && g.To > from && g.To > to:
			// from {-} to
			// g {-------}
			//   {b}{i}{a}
			before = append(before, &timeGroup{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints,
			})
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints,
			})
			after = append(after, &timeGroup{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			})
		case g.From > from && g.From == to && g.To > from && g.To > to:
			// from {-} to
			//      g {-}
			//      {i}{a}
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: []model.Fingerprint{},
			})
			after = append(after, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			})
		case g.From < from && g.From < to && g.To == from && g.To < to:
			// from {-} to
			//  g {-}
			//    {b}{i}
			before = append(before, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			})
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: []model.Fingerprint{},
			})
		case g.From > from && g.From == to && g.To > from && g.To == to:
			// from {-} to
			//      g |
			//      {i}{i}
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: []model.Fingerprint{},
			}, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			})
		case g.From == from && g.From < to && g.To == from && g.To < to:
			// from {-} to
			//    g |
			//    {i}{i}
			including = append(including, &timeGroup{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints,
			}, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: []model.Fingerprint{},
			})
		case g.From == from && g.To == to:
			including = append(including, &timeGroup{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints,
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

		for _, g := range group {
			if last == nil {
				last = g
				continue
			}

			if reflect.DeepEqual(last.Fingerprints, g.Fingerprints) {
				last.To = g.To
			} else {
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
