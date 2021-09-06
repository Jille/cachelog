// Package ranges keeps track of ranges with arbitrary Data attached.
// Add()ing a range overrides any previous entries for that same range.
// Remove() forgets about any ranges in the given range.
// Get() returns which ranges exist and which don't.
package ranges

import (
	"fmt"
	"sort"
)

// TODO(quis): Improve performance with a tree rather than keeping a sorted slice?

// Data is whatever you want to store with your ranges. Slice() is called when we're narrowing your range because of Add/Remove calls.
type Data interface {
	// Slice returns the Data for a new range with the given start/end bounds. It's guaranteed to be a subset of the previous range.
	Slice(start, end int64) Data
}

// NoData is the nil implementation of Data. It doesn't store anything but implements the Data interface.
type NoData struct{}

func (NoData) Slice(start, end int64) Data {
	return NoData{}
}

// Entry is a single range.
type Entry struct {
	Start, End int64
	Data       Data
}

func (e Entry) assertValid() {
	if e.Start > e.End {
		panic(fmt.Errorf("Invalid ranges.Entry: end is before start: %d, %d", e.Start, e.End))
	}
}

func (e Entry) isEmpty() bool {
	return e.Start == e.End
}

// Ranges contains zero or more ranges with their associated data. The zero Ranges is valid.
type Ranges struct {
	entries []Entry
	count   int64
}

// Count return the sum of all Entry.End-End.Start.
func (r Ranges) Count() int64 {
	return r.count
}

// Add a new entry. Overrides any previously existing ranges in the same interval.
func (r *Ranges) Add(e Entry) {
	if e.isEmpty() {
		return
	}
	e.assertValid()
	r.Remove(e.Start, e.End)
	if e.Data == nil {
		e.Data = NoData{}
	}
	r.entries = append(r.entries, e)
	// TODO(quis): Do a sorted insertion rather than resorting everything to improve performance.
	r.sort()
	r.count += e.End - e.Start
}

// Remove any ranges between start and end.
func (r *Ranges) Remove(start, end int64) {
	e := Entry{Start: start, End: end}
	if e.isEmpty() {
		return
	}
	e.assertValid()
	var newRanges []Entry
	for _, i := range r.entries {
		if e.Start <= i.Start && i.End <= e.End {
			r.count -= i.End - i.Start
			continue
		}
		if i.Start < e.Start && e.End < i.End {
			r.count -= e.End - e.Start
			newRanges = append(newRanges, Entry{i.Start, e.Start, i.Data.Slice(i.Start, e.Start)}, Entry{e.End, i.End, i.Data.Slice(e.End, i.End)})
			continue
		}
		changed := false
		if e.Start <= i.Start && i.Start < e.End {
			r.count -= e.End - i.Start
			i.Start = e.End
			changed = true
		}
		if e.Start < i.End && i.End <= e.End {
			r.count -= i.End - e.Start
			i.End = e.Start
			changed = true
		}
		if changed {
			i.Data = i.Data.Slice(i.Start, i.End)
		}
		newRanges = append(newRanges, i)
	}
	r.entries = newRanges
}

// Get returns all ranges between start and end; and which ranges weren't covered.
func (r *Ranges) Get(start, end int64) (found []Entry, missing []Entry) {
	e := Entry{Start: start, End: end}
	if e.isEmpty() {
		return nil, nil
	}
	e.assertValid()
	for _, i := range r.entries {
		if i.End <= start {
			continue
		}
		if i.Start >= end {
			break
		}
		ss := max(i.Start, start)
		if ss != start {
			missing = append(missing, Entry{Start: start, End: ss})
		}
		se := min(i.End, end)
		found = append(found, Entry{ss, se, i.Data.Slice(ss, se)})
		start = se
	}
	if start != end {
		missing = append(missing, Entry{Start: start, End: end})
	}
	return found, missing
}

func (r *Ranges) sort() {
	sort.Slice(r.entries, func(i, j int) bool {
		return r.entries[i].Start < r.entries[j].Start
	})
}

// MergeAdjacent merges adjacent ranges together. merge() is called to get the new Data for the merged range.
// Calls to merge() are guaranteed to have a.End == b.Start. merge() can return false to indicate these shouldn't be merged. MergeAdjacent will try to merge b the range after that if they're adjacent.
func MergeAdjacent(entries []Entry, merge func(a, b Entry) (Data, bool)) []Entry {
	if len(entries) == 0 {
		return nil
	}
	var ret []Entry
	for i, e := range entries {
		if i > 0 && entries[i-1].End == e.Start {
			if merged, ok := merge(ret[len(ret)-1], e); ok {
				ret[len(ret)-1].End = e.End
				ret[len(ret)-1].Data = merged
				continue
			}
		}
		ret = append(ret, e)
	}
	return ret
}

func min(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
