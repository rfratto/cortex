package ring

import (
	"math/rand"
	"time"
)

// GenerateTokens make numTokens unique random tokens, none of which clash
// with takenTokens.
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
