package ring

import (
	"testing"
)

func TestGenerateTokens(t *testing.T) {
	tokens := GenerateTokens(1000000, nil, ACTIVE)

	dups := make(map[uint32]int)

	for ix, v := range tokens {
		if ox, ok := dups[v.Token]; ok {
			t.Errorf("Found duplicate token %d, tokens[%d]=%d, tokens[%d]=%d", v, ix, tokens[ix].Token, ox, tokens[ox].Token)
		} else {
			dups[v.Token] = ix
		}
	}
}

func TestGenerateTokensIgnoresOldTokens(t *testing.T) {
	first := GenerateTokens(1000000, nil, ACTIVE)
	second := GenerateTokens(1000000, first, ACTIVE)

	dups := make(map[uint32]bool)

	for _, v := range first {
		dups[v.Token] = true
	}

	for _, v := range second {
		if dups[v.Token] {
			t.Fatal("GenerateTokens returned old token")
		}
	}
}
