package fprgroup

import (
	"bytes"
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

// Group contains fingerprints belonging to a certain time window.
type Group struct {
	From         model.Time
	To           model.Time
	Fingerprints fingerprintMap
}

func (g *Group) String() string {
	fingerprints := &bytes.Buffer{}
	for f := range g.Fingerprints {
		fmt.Fprintf(fingerprints, "%s, ", f)
	}
	return fmt.Sprintf("(%d, %d, [%s])", g.From, g.To, fingerprints)
}

// CreateGroups takes a map of time series and creates groups of fingerprints by time interval.
// The returned slice contains time intervals in ascending order.
func CreateGroups(seriesMap minilocal.SeriesMap) []*Group {
	timeSortedFingerprints := sortedFingerprints(seriesMap)
	return groupByTime(timeSortedFingerprints)
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

func groupByTime(series []seriesTime) []*Group {
	if len(series) == 0 {
		return []*Group{}
	}

	groups := []*Group{
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
			groups = append(groups, &Group{
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

func findGroups(groups []*Group, from, to model.Time) (before, including, after []*Group) {
	before = []*Group{}
	including = []*Group{}
	after = []*Group{}

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
			before = append(before, &Group{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints.Copy(),
			})

			including = append(including, &Group{
				From:         from,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &Group{
				From:         g.To,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From < to && g.To > from && g.To < to:
			// from {-------} to
			//       g {-}
			//      {i}{i}{i}
			including = append(including, &Group{
				From:         from,
				To:           g.From,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &Group{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &Group{
				From:         g.To,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From < to && g.To > from && g.To > to:
			// from {----} to
			//       g {----}
			//      {i}{i}{a}
			including = append(including, &Group{
				From:         from,
				To:           g.From,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &Group{
				From:         g.From,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})

			after = append(after, &Group{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From == from && g.From < to && g.To > from && g.To < to:
			// from {----} to
			//    g {-}
			//      {i}{i}
			including = append(including, &Group{
				From:         from,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &Group{
				From:         g.To,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From < to && g.To > from && g.To == to:
			// from {----} to
			//       g {-}
			//      {i}{i}
			including = append(including, &Group{
				From:         from,
				To:           g.From,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &Group{
				From:         g.From,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From == from && g.From < to && g.To > from && g.To > to:
			// from {---} to
			//    g {------}
			//      {-i-}{a}
			including = append(including, &Group{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})

			after = append(after, &Group{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From < from && g.From < to && g.To > from && g.To == to:
			// from {---} to
			// g {------}
			//   {b}{-i-}
			before = append(before, &Group{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints.Copy(),
			})

			including = append(including, &Group{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From < from && g.From < to && g.To > from && g.To > to:
			// from {-} to
			// g {-------}
			//   {b}{i}{a}
			before = append(before, &Group{
				From:         g.From,
				To:           from,
				Fingerprints: g.Fingerprints.Copy(),
			})
			including = append(including, &Group{
				From:         from,
				To:           to,
				Fingerprints: g.Fingerprints.Copy(),
			})
			after = append(after, &Group{
				From:         to,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From > from && g.From == to && g.To > from && g.To > to:
			// from {-} to
			//      g {-}
			//      {i}{a}
			including = append(including, &Group{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
			after = append(after, &Group{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From < from && g.From < to && g.To == from && g.To < to:
			// from {-} to
			//  g {-}
			//    {b}{i}
			before = append(before, &Group{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
			including = append(including, &Group{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From > from && g.From == to && g.To > from && g.To == to:
			// from {-} to
			//      g |
			//      {i}{i}
			including = append(including, &Group{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			}, &Group{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			})
		case g.From == from && g.From < to && g.To == from && g.To < to:
			// from {-} to
			//    g |
			//    {i}{i}
			including = append(including, &Group{
				From:         g.From,
				To:           g.To,
				Fingerprints: g.Fingerprints.Copy(),
			}, &Group{
				From:         from,
				To:           to,
				Fingerprints: make(map[model.Fingerprint]bool),
			})
		case g.From == from && g.To == to:
			including = append(including, &Group{
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

func combineGroups(groupGroups ...[]*Group) []*Group {
	if len(groupGroups) == 0 {
		return []*Group{}
	}

	result := []*Group{}
	var last *Group
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
