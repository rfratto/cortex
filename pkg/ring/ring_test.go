package ring

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	strconv "strconv"
	strings "strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	numTokens = 512
)

func BenchmarkBatch10x100(b *testing.B) {
	benchmarkBatch(b, 10, 100)
}

func BenchmarkBatch100x100(b *testing.B) {
	benchmarkBatch(b, 100, 100)
}

func BenchmarkBatch100x1000(b *testing.B) {
	benchmarkBatch(b, 100, 1000)
}

func benchmarkBatch(b *testing.B, numIngester, numKeys int) {
	// Make a random ring with N ingesters, and M tokens per ingests
	desc := NewDesc()
	var takenTokens []StatefulToken
	for i := 0; i < numIngester; i++ {
		tokens := GenerateTokens(numTokens, takenTokens, ACTIVE)
		takenTokens = append(takenTokens, tokens...)
		desc.AddIngester(fmt.Sprintf("%d", i), fmt.Sprintf("ingester%d", i), tokens, ACTIVE, false)
	}

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	r := Ring{
		name:     "ingester",
		cfg:      cfg,
		ringDesc: desc,
	}

	ctx := context.Background()
	callback := func(IngesterDesc, []int) error {
		return nil
	}
	cleanup := func() {
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := make([]uint32, numKeys)
	// Generate a batch of N random keys, and look them up
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateKeys(rnd, numKeys, keys)
		err := DoBatch(ctx, &r, keys, callback, cleanup)
		require.NoError(b, err)
	}
}

func generateKeys(r *rand.Rand, numTokens int, dest []uint32) {
	for i := 0; i < numTokens; i++ {
		dest[i] = r.Uint32()
	}
}

func TestDoBatchZeroIngesters(t *testing.T) {
	ctx := context.Background()
	numKeys := 10
	keys := make([]uint32, numKeys)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	generateKeys(rnd, numKeys, keys)
	callback := func(IngesterDesc, []int) error {
		return nil
	}
	cleanup := func() {
	}
	desc := NewDesc()
	r := Ring{
		name:     "ingester",
		cfg:      Config{},
		ringDesc: desc,
	}
	require.Error(t, DoBatch(ctx, &r, keys, callback, cleanup))
}

// GenerateRing takes a ringConfig and a description of tokens
// in the ring and prepopulates it with fake values for testing.
//
// The schema of tokens in the ring is a space-delimited list of
// letters followed by an optional number. If a letter is
// specified twice, both instances should be followed by a unique
// number.
//
// For example
//
//		A B C D1 E D2 F G
//
// creates a ring of 7 ingesters, where ingester with id "D" has
// two tokens inserted. Tokens will be generated in ascending order
// with each token containing the value of the previous token +1.
//
// Each letter-number pair can be suffixed with the following to
// affect the state of the token:
//
// .: ACTIVE
// ?: PENDING
// +: JOINING
// -: LEAVING
//
// If no suffix is specified, the default state will be ACTIVE.
//
// Tokens will be stored in denormalised form.
func GenerateRing(t *testing.T, r *Ring, desc string) *Desc {
	t.Helper()

	regex, err := regexp.Compile(
		"(?P<ingester>[A-Z])(?P<token>\\d*)(?P<state>\\+|\\-|\\?|\\.)?",
	)
	if err != nil {
		t.Fatalf("unexpected regex err %v", err)
	}

	var d *Desc = &Desc{}
	d.Ingesters = make(map[string]IngesterDesc)

	tokens := strings.Split(desc, " ")
	var lastToken uint32 = 1

	for _, tokDesc := range tokens {
		if tokDesc == "" {
			continue
		}

		submatches := regex.FindStringSubmatch(tokDesc)
		if submatches == nil {
			t.Errorf("invalid token desc %s", tokDesc)
			continue
		}

		ingester := submatches[1]
		tokenIndex := 1
		state := ACTIVE

		if submatches[2] != "" {
			tokenIndex, err = strconv.Atoi(submatches[2])
			if err != nil {
				t.Errorf("invalid token index %s in %s", submatches[2], tokDesc)
			}
		}
		if submatches[3] != "" {
			switch stateStr := submatches[3]; stateStr {
			case ".":
				state = ACTIVE
			case "?":
				state = PENDING
			case "+":
				state = JOINING
			case "-":
				state = LEAVING
			default:
				t.Errorf("invalid token state operator %s in %s", stateStr, tokDesc)
			}
		}

		ing, ok := d.Ingesters[ingester]
		if !ok {
			ing = IngesterDesc{
				Addr:           ingester,
				State:          ACTIVE,
				Timestamp:      time.Now().Unix(),
				StatefulTokens: true,
				InactiveTokens: make(map[uint32]State),
			}
		}

		if tokenIndex != len(ing.Tokens)+1 {
			t.Errorf(
				"invalid token index %d in %s, should be %d",
				tokenIndex,
				tokDesc,
				len(ing.Tokens)+1,
			)
		}

		ing.Tokens = append(ing.Tokens, lastToken)
		if state != ACTIVE {
			ing.InactiveTokens[lastToken] = state
		}

		d.Ingesters[ingester] = ing
		lastToken++
	}

	if t.Failed() {
		return nil
	}

	d.Tokens = migrateRing(d)
	for id, ing := range d.Ingesters {
		ing.Tokens = nil
		ing.InactiveTokens = nil
		d.Ingesters[id] = ing
	}

	if r != nil {
		r.KVClient.CAS(context.Background(), ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
			return d, false, nil
		})
	}

	return d
}

func TestGenerateRing(t *testing.T) {
	tt := []struct {
		desc      string
		ingesters int
		active    int
		pending   int
		leaving   int
		joining   int
	}{
		{"A B C", 3, 3, 0, 0, 0},
		{"A1 B A2 C", 3, 4, 0, 0, 0},
		{"A1. B? A2+ C-", 3, 1, 1, 1, 1},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			var ringConfig Config
			flagext.DefaultValues(&ringConfig)
			ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

			r, err := New(ringConfig, "ingester")
			require.NoError(t, err)
			defer r.Stop()

			GenerateRing(t, r, tc.desc)

			d, err := r.KVClient.Get(context.Background(), ConsulKey)
			require.NoError(t, err)
			desc, ok := d.(*Desc)
			require.True(t, ok)

			require.Equal(t, tc.active, len(desc.FindTokensByState(ACTIVE)))
			require.Equal(t, tc.pending, len(desc.FindTokensByState(PENDING)))
			require.Equal(t, tc.leaving, len(desc.FindTokensByState(LEAVING)))
			require.Equal(t, tc.joining, len(desc.FindTokensByState(JOINING)))
			require.Equal(t, tc.ingesters, len(desc.Ingesters))
		})
	}
}

func TestRingGet(t *testing.T) {
	tt := []struct {
		name   string
		desc   string
		key    uint32
		op     Operation
		expect []string
	}{
		{"all active", "A B C", 0, Read, []string{"A", "B", "C"}},
		{"wrap around", "A B C", 2, Read, []string{"C", "A", "B"}},
		{"skip joining on read", "A B+ C+ D E", 0, Read, []string{"A", "D", "E"}},
		{"skip joining on write", "A B+ C+ D E", 0, Write, []string{"A", "D", "E"}},
		{"skip leaving on write", "A B- C- D E", 0, Write, []string{"A", "D", "E"}},
		{"don't skip leaving on read", "A B- C- D E", 0, Read, []string{"A", "B", "C"}},
		{"duplicates", "A1? A2+ A3 B1 B2+ C1- C2 D1 D2", 0, Write, []string{"A", "B", "C"}},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var ringConfig Config
			flagext.DefaultValues(&ringConfig)
			ringConfig.ReplicationFactor = 3
			ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

			r, err := New(ringConfig, "ingester")
			require.NoError(t, err)
			defer r.Stop()

			r.ringDesc = GenerateRing(t, r, tc.desc)
			rs, err := r.Get(tc.key, tc.op, nil, nil)
			require.NoError(t, err)

			names := []string{}
			for _, ing := range rs.Ingesters {
				names = append(names, ing.Addr)
			}

			require.Equal(t, tc.expect, names)
		})
	}
}
