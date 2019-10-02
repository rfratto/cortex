package ring

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// PrintableRanges wraps a slice of TokenRanges and provides a String
// method so it can be printed and displayed to the user.
type PrintableRanges []TokenRange

func (r PrintableRanges) String() string {
	strs := make([]string, len(r))
	for i, rg := range r {
		strs[i] = fmt.Sprintf("(%d, %d)", rg.From, rg.To)
	}
	return strings.Join(strs, ", ")
}

// StatefulToken converts a TokenDesc into a StatefulToken.
func (d TokenDesc) StatefulToken() StatefulToken {
	return StatefulToken{
		Token: d.Token,
		State: d.State,
	}
}

// TokenGeneratorFunc is any function that will generate a series
// of tokens to apply to a new lifecycler.
type TokenGeneratorFunc func(numTokens int, taken []StatefulToken, state State) []StatefulToken

// GenerateTokens make numTokens unique random tokens, none of which clash
// with takenTokens.
//
// GenerateTokens is the default implementation of TokenGeneratorFunc.
func GenerateTokens(numTokens int, takenTokens []StatefulToken, state State) []StatefulToken {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	used := make(map[uint32]bool)
	for _, v := range takenTokens {
		used[v.Token] = true
	}

	tokens := []StatefulToken{}

	for i := 0; i < numTokens; {
		candidate := r.Uint32()
		if used[candidate] {
			continue
		}
		used[candidate] = true
		tokens = append(tokens, StatefulToken{
			Token: candidate,
			State: state,
		})
		i++
	}
	return tokens
}

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }
