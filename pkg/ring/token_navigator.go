package ring

import (
	"errors"
	"fmt"
	"sort"
)

// A TokenRange encompasses a range of values that are owned by the ending
// token in the range - namely, the range is inclusive on the Start and
// Exclusive on the End.
type TokenRange struct {
	Start TokenDesc
	End   TokenDesc
}

// TokenNavigator provides utility methods for traversing the ring. The tokens
// in TokenNavigator should be sorted by token value, and all tokens should be
// considered "healthy" (i.e., ready for traffic).
type TokenNavigator []TokenDesc

// Predecessors finds all token ranges for which t is the nth successor to.
func (nav TokenNavigator) Predecessors(t TokenDesc, n int) ([]TokenRange, error) {
	idx, ok := nav.findTokenIndex(t)
	if !ok {
		return nil, fmt.Errorf("could not find token %d in ring", t.Token)
	} else if n == 0 {
		return []TokenRange{{Start: nav.neighbor(idx, -1), End: t}}, nil
	} else if n < 0 {
		return nil, errors.New("Predecessors must be called with a non-negative offset")
	}

	var (
		predecessors []TokenRange
		distinct     = map[string]bool{}
	)

	it := newTokenIterator(nav, idx, -1)
	for it.Next() {
		predecessor, predecessorIdx := it.Value()

		// Stop if this is a new ingester and we've already seen enough unique ingesters
		// to fill n.
		if distinct[predecessor.Ingester] && len(distinct) == n+1 {
			break
		}

		// Collect the token if its nth successor is our starting token.
		succ, err := nav.Successor(predecessor, n)
		if err != nil {
			return predecessors, err
		} else if succ.End == t {
			rg := TokenRange{Start: nav.neighbor(predecessorIdx, -1), End: predecessor}
			predecessors = append([]TokenRange{rg}, predecessors...)
		}

		distinct[predecessor.Ingester] = true
	}

	return predecessors, nil
}

// neighbor returns the immediate nth neighbor from tokenIdx, wrapping around the
// ring.
func (nav TokenNavigator) neighbor(tokenIdx int, n int) TokenDesc {
	return nav[nav.wrap(tokenIdx+n)]
}

func (nav TokenNavigator) wrap(idx int) int {
	if idx >= len(nav) {
		return idx % len(nav)
	} else if idx < 0 {
		return len(nav) - (-idx)
	}
	return idx
}

// Successor returns the first clockwise nth neighbor token range from t.
// n must be positive.
//
// Only one token per ingester is considered is considered as a neighbor.
func (nav TokenNavigator) Successor(t TokenDesc, n int) (TokenRange, error) {
	idx, ok := nav.findTokenIndex(t)
	if !ok {
		return TokenRange{}, fmt.Errorf("could not find token %d in ring", t.Token)
	}
	if n == 0 {
		return TokenRange{Start: nav.neighbor(idx, -1), End: t}, nil
	} else if n < 0 {
		return TokenRange{}, errors.New("n in Successor must be positive")
	}

	var (
		distinct = map[string]bool{}
	)

	it := newTokenIterator(nav, idx, 1)
	for it.Next() {
		successor, successorIdx := it.Value()
		if distinct[successor.Ingester] {
			continue
		}
		distinct[successor.Ingester] = true

		if len(distinct) == n {
			rg := TokenRange{Start: nav.neighbor(successorIdx, -1), End: successor}
			return rg, nil
		}
	}

	return TokenRange{}, fmt.Errorf("could not find successor %d for token %d", n, t.Token)
}

// InRange returns whether any token owned by a specific ingester is found within
// a TokenRange. Both the Start and End ranges are searched exclusively.
func (nav TokenNavigator) InRange(tr TokenRange, ingesterID string) (bool, error) {
	start, ok := nav.findTokenIndex(tr.Start)
	if !ok {
		return false, fmt.Errorf("could not find token %d in ring", tr.Start.Token)
	}

	end, ok := nav.findTokenIndex(tr.End)
	if !ok {
		return false, fmt.Errorf("could not fidn token %d in ring", tr.End.Token)
	}

	if nav[start].Ingester == ingesterID || nav[end].Ingester == ingesterID {
		return true, nil
	} else if start == end {
		return nav[start].Ingester == ingesterID, nil
	}

	it := newTokenIterator(nav, start, 1)
	for it.Next() {
		tok, tokIdx := it.Value()
		if tokIdx == end {
			return tok.Ingester == ingesterID, nil
		} else if tok.Ingester == ingesterID {
			return true, nil
		}
	}

	return false, nil
}

// findTokenIndex searches the sorted list of tokens in the TokenNavigator
// for the index of the token provided.
func (nav TokenNavigator) findTokenIndex(token TokenDesc) (int, bool) {
	i := sort.Search(len(nav), func(x int) bool {
		return nav[x].Token >= token.Token
	})
	if i >= len(nav) {
		i = 0
	}
	return i, len(nav) > i && nav[i] == token
}

// tokenIterator allows for iterating through tokens in the ring.
type tokenIterator struct {
	tokens    TokenNavigator
	idx       int // current idx
	stopIdx   int // idx to stop searching at (once the ring has been fully looped)
	direction int
}

// newTokenIterator returns a new token iterator. The starting element is not
// included in the iteration results.
func newTokenIterator(tokens []TokenDesc, start int, direction int) tokenIterator {
	return tokenIterator{tokens: tokens, direction: direction, idx: start, stopIdx: start}
}

// Next retrieves the next element and returns true if one was retrieved.
func (it *tokenIterator) Next() bool {
	it.idx = it.tokens.wrap(it.idx + it.direction)
	if it.idx == it.stopIdx {
		return false
	}

	return true
}

// Value retrieves the current element and its index from the iterator.
func (it *tokenIterator) Value() (t TokenDesc, idx int) {
	if it.idx >= 0 && it.idx < len(it.tokens) {
		return it.tokens[it.idx], it.idx
	}

	return TokenDesc{}, it.idx
}
