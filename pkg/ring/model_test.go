package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngesterDesc_IsHealthy(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ingester      *IngesterDesc
		timeout       time.Duration
		writeExpected bool
		readExpected  bool
	}{
		"ALIVE ingester with last keepalive newer than timeout": {
			ingester:      &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: true,
			readExpected:  true,
		},
		"ALIVE ingester with last keepalive older than timeout": {
			ingester:      &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: false,
			readExpected:  false,
		},
		"JOINING ingester with last keepalive newer than timeout": {
			ingester:      &IngesterDesc{State: JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: false,
			readExpected:  false,
		},
		"LEAVING ingester with last keepalive newer than timeout": {
			ingester:      &IngesterDesc{State: LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: false,
			readExpected:  true,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.ingester.IsHealthy(Write, testData.timeout)
			assert.Equal(t, testData.writeExpected, actual)

			actual = testData.ingester.IsHealthy(Read, testData.timeout)
			assert.Equal(t, testData.readExpected, actual)
		})
	}
}

func normalizedSource() *Desc {
	r := NewDesc()
	r.Ingesters["first"] = IngesterDesc{
		Tokens: []uint32{100, 200, 300},
	}
	r.Ingesters["second"] = IngesterDesc{}
	return r
}

func unnormalizedSource() *Desc {
	r := NewDesc()
	r.Ingesters["first"] = IngesterDesc{}
	r.Ingesters["second"] = IngesterDesc{}
	r.Tokens = []TokenDesc{
		{Token: 100, Ingester: "first"},
		{Token: 200, Ingester: "first"},
		{Token: 300, Ingester: "first"},
	}
	return r
}

func normalizedOutput() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{
			"first":  {},
			"second": {Tokens: []uint32{100, 200, 300}},
		},
		Tokens: nil,
	}
}

func unnormalizedOutput() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{
			"first":  {},
			"second": {},
		},
		Tokens: []TokenDesc{
			{Token: 100, Ingester: "second"},
			{Token: 200, Ingester: "second"},
			{Token: 300, Ingester: "second"},
		},
	}
}

func TestClaimTokensFromNormalizedToNormalized(t *testing.T) {
	r := normalizedSource()
	result := r.ClaimTokens("first", "second", true)

	assert.Equal(t, []StatefulToken{{100, ACTIVE}, {200, ACTIVE}, {300, ACTIVE}}, result)
	assert.Equal(t, normalizedOutput(), r)
}

func TestClaimTokensFromNormalizedToUnnormalized(t *testing.T) {
	r := normalizedSource()
	result := r.ClaimTokens("first", "second", false)

	assert.Equal(t, []StatefulToken{{100, ACTIVE}, {200, ACTIVE}, {300, ACTIVE}}, result)
	assert.Equal(t, unnormalizedOutput(), r)
}

func TestClaimTokensFromUnnormalizedToUnnormalized(t *testing.T) {
	r := unnormalizedSource()
	result := r.ClaimTokens("first", "second", false)

	assert.Equal(t, []StatefulToken{{100, ACTIVE}, {200, ACTIVE}, {300, ACTIVE}}, result)
	assert.Equal(t, unnormalizedOutput(), r)
}

func TestClaimTokensFromUnnormalizedToNormalized(t *testing.T) {
	r := unnormalizedSource()

	result := r.ClaimTokens("first", "second", true)

	assert.Equal(t, []StatefulToken{{100, ACTIVE}, {200, ACTIVE}, {300, ACTIVE}}, result)
	assert.Equal(t, normalizedOutput(), r)
}

func TestReady(t *testing.T) {
	now := time.Now()

	r := &Desc{
		Ingesters: map[string]IngesterDesc{
			"ing1": {
				Tokens:    []uint32{100, 200, 300},
				State:     ACTIVE,
				Timestamp: now.Unix(),
			},
		},
	}

	if err := r.Ready(now, 10*time.Second); err != nil {
		t.Fatal("expected ready, got", err)
	}

	if err := r.Ready(now.Add(5*time.Minute), 10*time.Second); err == nil {
		t.Fatal("expected !ready (no heartbeat from active ingester), but got no error")
	}

	r = &Desc{
		Ingesters: map[string]IngesterDesc{
			"ing1": {
				State:     ACTIVE,
				Timestamp: now.Unix(),
			},
		},
	}

	if err := r.Ready(now, 10*time.Second); err == nil {
		t.Fatal("expected !ready (no tokens), but got no error")
	}

	r.Tokens = []TokenDesc{
		{Token: 12345, Ingester: "some ingester"},
	}

	if err := r.Ready(now, 10*time.Second); err != nil {
		t.Fatal("expected ready, got", err)
	}
}

func TestInRange(t *testing.T) {
	tt := []struct {
		desc   string
		opts   RangeOptions
		expect bool
	}{
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C"}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "F"}, false},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A"}, false},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "E"}, false},

		// Inclusivity
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", LeftInclusive: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "E", RightInclusive: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", LeftInclusive: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", RightInclusive: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", LeftInclusive: true, RightInclusive: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "E", LeftInclusive: true, RightInclusive: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", LeftInclusive: true, RightInclusive: true}, true},
		{"A B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "F", LeftInclusive: true, RightInclusive: true}, false},

		// State Checks

		// accept leaving when reading
		{"A B C- D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, true},
		{"A B C- D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, true},
		{"A- B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Read}, false},
		{"A B C D E-", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Read}, false},

		// accept active when reading
		{"A B C. D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, true},
		{"A B C. D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, true},
		{"A. B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Read}, false},
		{"A B C D E.", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Read}, false},

		// accept pending when reading
		{"A B C? D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, true},
		{"A B C? D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, true},
		{"A? B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Read}, false},
		{"A B C D E?", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Read}, false},

		// reject joining when reading
		{"A B C+ D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, false},
		{"A B C+ D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Read}, false},
		{"A+ B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Read}, false},
		{"A B C D E+", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Read}, false},

		// reject leaving when writing
		{"A B C- D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, false},
		{"A B C- D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, false},
		{"A- B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Write}, false},
		{"A B C D E-", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Write}, false},

		// accept active when writing
		{"A B C. D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, true},
		{"A B C. D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, true},
		{"A. B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Write}, false},
		{"A B C D E.", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Write}, false},

		// reject pending when writing
		{"A B C? D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, false},
		{"A B C? D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, false},
		{"A? B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Write}, false},
		{"A B C D E?", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Write}, false},

		// reject joining when writing
		{"A B C+ D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, false},
		{"A B C+ D E", RangeOptions{Range: TokenRange{1, 5}, ID: "C", Op: Write}, false},
		{"A+ B C D E", RangeOptions{Range: TokenRange{1, 5}, ID: "A", Op: Write}, false},
		{"A B C D E+", RangeOptions{Range: TokenRange{1, 5}, ID: "E", Op: Write}, false},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			tc.opts.MaxHeartbeat = time.Second * 3600
			r := GenerateRing(t, nil, tc.desc)
			ok := r.InRange(tc.opts)
			require.Equal(t, tc.expect, ok)
		})
	}
}

func TestPredecessors(t *testing.T) {
	tt := []struct {
		desc   string
		token  int
		n      int
		op     Operation
		expect []uint32
	}{
		// Simple cases
		{"A B C", 2, 0, Read, []uint32{3}},
		{"A B C", 2, 1, Read, []uint32{2}},
		{"A B C", 2, 2, Read, []uint32{1}},

		// Handling duplicates
		{"A1 A2 B1 B2 B3 C1 C2 D", 7, 0, Read, []uint32{8}},
		{"A1 A2 B1 B2 B3 C1 C2 D", 7, 1, Read, []uint32{6, 7}},
		{"A1 A2 B1 B2 B3 C1 C2 D", 7, 2, Read, []uint32{3, 4, 5}},
		{"A1 A2 B1 B2 B3 C1 C2 D", 7, 3, Read, []uint32{1, 2}},

		{"A1 B1 C1 A2 B2 A3 Y", 6, 0, Read, []uint32{7}},
		{"A1 B1 C1 A2 B2 A3 Y", 6, 1, Read, []uint32{6}},
		{"A1 B1 C1 A2 B2 A3 Y", 6, 2, Read, []uint32{4, 5}},
		{"A1 B1 C1 A2 B2 A3 Y", 6, 3, Read, []uint32{1, 2, 3}},

		// Wrap around ring
		{"A B C", 0, 0, Read, []uint32{1}},
		{"A B C", 0, 1, Read, []uint32{3}},
		{"A B C", 0, 2, Read, []uint32{2}},

		// When read, only consider active/pending/leaving
		{"A B? C- D+ E", 4, 0, Read, []uint32{5}},
		{"A B? C- D+ E", 4, 1, Read, []uint32{3}},
		{"A B? C- D+ E", 4, 2, Read, []uint32{2}},

		// When write, only consider active
		{"A B? C- D+ E+", 4, 0, Write, []uint32{5}}, // 0 always gets self
		{"A B? C- D+ E+", 4, 1, Write, []uint32{1}},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			r := GenerateRing(t, nil, tc.desc)
			res, err := r.Predecessors(NeighborOptions{
				Start:        r.Tokens[tc.token].StatefulToken(),
				Neighbor:     tc.n,
				Op:           tc.op,
				IncludeStart: true,
				MaxHeartbeat: time.Second * 3600,
			})
			require.NoError(t, err)

			var foundTokens []uint32
			for _, res := range res {
				foundTokens = append(foundTokens, res.Token)
			}

			require.Equal(t, tc.expect, foundTokens)
		})
	}
}

func TestSuccessor(t *testing.T) {
	tt := []struct {
		desc   string
		token  int
		n      int
		op     Operation
		expect string
	}{
		// Simple cases
		{"A B C", 0, 0, Read, "A"},
		{"A B C", 0, 1, Read, "B"},
		{"A B C", 0, 2, Read, "C"},

		// Handling duplicates
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 0, Read, "A"},
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 1, Read, "B"},
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 2, Read, "C"},
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 3, Read, "D"},

		// Wrap around ring
		{"A B C", 2, 0, Read, "C"},
		{"A B C", 2, 1, Read, "A"},
		{"A B C", 2, 2, Read, "B"},

		// When Read, only consider active/leaving/pending
		{"A B? C- D+ E", 0, 0, Read, "A"},
		{"A B? C- D+ E", 0, 1, Read, "B"},
		{"A B? C- D+ E", 0, 2, Read, "C"},

		// When Write, only consider active
		{"A? B? C- D+ E", 0, 0, Write, "A"}, // always gets self
		{"A? B? C- D+ E", 0, 1, Write, "E"},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			r := GenerateRing(t, nil, tc.desc)

			res, err := r.Successor(NeighborOptions{
				Start:        r.Tokens[tc.token].StatefulToken(),
				Neighbor:     tc.n,
				Op:           tc.op,
				IncludeStart: true,
				MaxHeartbeat: time.Second * 3600,
			})
			require.NoError(t, err)
			require.Equal(t, tc.expect, res.Ingester)
		})
	}
}

func TestSuccessorSkipSelf(t *testing.T) {
	tt := []struct {
		desc   string
		token  int
		n      int
		expect string
	}{
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 0, "A"},
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 1, "A"},
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 2, "B"},
		{"A1 A2 B1 B2 B3 C1 C2 D", 0, 3, "C"},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			r := GenerateRing(t, nil, tc.desc)
			res, err := r.Successor(NeighborOptions{
				Start:        r.Tokens[tc.token].StatefulToken(),
				Neighbor:     tc.n,
				Op:           Read,
				MaxHeartbeat: time.Second * 3600,
			})
			require.NoError(t, err)
			require.Equal(t, tc.expect, res.Ingester)
		})
	}
}
