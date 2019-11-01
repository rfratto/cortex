package ring

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/test"
)

type flushTransferer struct {
	lifecycler *Lifecycler
}

func (f *flushTransferer) StopIncomingRequests() {}
func (f *flushTransferer) Flush()                {}
func (f *flushTransferer) TransferOut(ctx context.Context) error {
	if err := f.lifecycler.ClaimTokensFor(ctx, "ing1"); err != nil {
		return err
	}
	return f.lifecycler.ChangeState(ctx, ACTIVE)
}

func testLifecyclerConfig(ringConfig Config, id string) LifecyclerConfig {
	var lifecyclerConfig LifecyclerConfig
	flagext.DefaultValues(&lifecyclerConfig)
	lifecyclerConfig.Addr = "0.0.0.0"
	lifecyclerConfig.Port = 1
	lifecyclerConfig.RingConfig = ringConfig
	lifecyclerConfig.NumTokens = 1
	lifecyclerConfig.ID = id
	lifecyclerConfig.FinalSleep = 0
	lifecyclerConfig.RangeUnblockDelay = time.Duration(0)
	return lifecyclerConfig
}

func checkInRing(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok && desc.Ingesters[id].State == ACTIVE
}

func checkDenormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 0 &&
		len(desc.Tokens) == 1 &&
		desc.Tokens[0].State == ACTIVE
}

func checkNormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 1 &&
		len(desc.Ingesters[id].InactiveTokens) == 0 &&
		len(desc.Tokens) == 0
}

func TestTokenStatesNormalised(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.NormaliseTokens = true
	lifecyclerConfig1.SkipUnregister = true
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{
		allowTransfer: true,
	}, nil, "ingester")
	l1.Start()
	require.NoError(t, err)

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	l1.Shutdown()
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)

		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 1 &&
			desc.Ingesters["ing1"].State == LEAVING &&
			len(desc.Ingesters["ing1"].Tokens) == 1 &&
			len(desc.Ingesters["ing1"].InactiveTokens) == 1 &&
			desc.Ingesters["ing1"].InactiveTokens[desc.Ingesters["ing1"].Tokens[0]] == LEAVING &&
			len(desc.Tokens) == 0
	})
}

func TestTokenStatesDenormalised(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.SkipUnregister = true
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{
		allowTransfer: true,
	}, nil, "ingester")
	l1.Start()
	require.NoError(t, err)

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkDenormalised(d, "ing1")
	})

	l1.Shutdown()
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)

		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 1 &&
			desc.Ingesters["ing1"].State == LEAVING &&
			len(desc.Ingesters["ing1"].Tokens) == 0 &&
			len(desc.Tokens) == 1 &&
			desc.Tokens[0].State == LEAVING
	})
}

func TestRingNormaliseMigration(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with denormalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")

	ft := &flushTransferer{}
	l1, err := NewLifecycler(lifecyclerConfig1, ft, nil, "ingester")
	require.NoError(t, err)
	l1.Start()

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkDenormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with normalised tokens.
	var lifecyclerConfig2 = testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.JoinAfter = 100 * time.Second
	lifecyclerConfig2.NormaliseTokens = true

	l2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, nil, "ingester")
	require.NoError(t, err)
	l2.Start()

	// This will block until l1 has successfully left the ring.
	ft.lifecycler = l2 // When l1 shutsdown, call l2.ClaimTokensFor("ing1")
	l1.Shutdown()

	// Check the new ingester joined, has the same token, and is active.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing2") &&
			d.(*Desc).Ingesters["ing2"].Tokens[0] == token.Token
	})
}

func TestLifecycler_HealthyInstancesCount(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(GetCodec())

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add the first ingester to the ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig1.JoinAfter = 100 * time.Millisecond

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, &flushTransferer{}, nil, "ingester")
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler1.HealthyInstancesCount())

	lifecycler1.Start()

	// Assert the first ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 1
	})

	// Add the second ingester to the ring
	lifecyclerConfig2 := testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig2.JoinAfter = 100 * time.Millisecond

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, &flushTransferer{}, nil, "ingester")
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler2.HealthyInstancesCount())

	lifecycler2.Start()

	// Assert the second ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler2.HealthyInstancesCount() == 2
	})

	// Assert the first ingester count is updated
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 2
	})
}

type nopFlushTransferer struct {
	allowTransfer bool
}

func (f *nopFlushTransferer) StopIncomingRequests() {}
func (f *nopFlushTransferer) Flush()                {}
func (f *nopFlushTransferer) TransferOut(ctx context.Context) error {
	if !f.allowTransfer {
		panic("should not be called")
	}
	return nil
}

func TestRingRestart(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.NormaliseTokens = true
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, nil, "ingester")
	require.NoError(t, err)
	l1.Start()

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	token := l1.tokens[0]

	// Add a second ingester with the same settings, so it will think it has restarted
	l2, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{allowTransfer: true}, nil, "ingester")
	require.NoError(t, err)
	l2.Start()

	// Check the new ingester picked up the same token
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		l2Tokens := l2.getTokens()

		return checkNormalised(d, "ing1") &&
			len(l2Tokens) == 1 &&
			l2Tokens[0] == token
	})
}

type MockClient struct {
	GetFunc         func(ctx context.Context, key string) (interface{}, error)
	CASFunc         func(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error
	WatchKeyFunc    func(ctx context.Context, key string, f func(interface{}) bool)
	WatchPrefixFunc func(ctx context.Context, prefix string, f func(string, interface{}) bool)
}

func (m *MockClient) MapFunctions(client kv.Client) {
	m.GetFunc = client.Get
	m.CASFunc = client.CAS
	m.WatchKeyFunc = client.WatchKey
	m.WatchPrefixFunc = client.WatchPrefix
}

func (m *MockClient) Get(ctx context.Context, key string) (interface{}, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, key)
	}

	return nil, nil
}

func (m *MockClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	if m.CASFunc != nil {
		return m.CASFunc(ctx, key, f)
	}

	return nil
}

func (m *MockClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	if m.WatchKeyFunc != nil {
		m.WatchKeyFunc(ctx, key, f)
	}
}

func (m *MockClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	if m.WatchPrefixFunc != nil {
		m.WatchPrefixFunc(ctx, prefix, f)
	}
}

func (m *MockClient) Stop() {
	// nothing to do
}

// Ensure a check ready returns error when consul returns a nil key and the ingester already holds keys. This happens if the ring key gets deleted
func TestCheckReady(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = &MockClient{}

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()
	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 1 * time.Nanosecond
	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, nil, "ingester")
	l1.setTokens([]StatefulToken{
		{1, ACTIVE},
	})
	l1.Start()
	require.NoError(t, err)

	// Delete the ring key before checking ready
	err = l1.CheckReady(context.Background())
	require.Error(t, err)
}

func TestFindTransferWorkload(t *testing.T) {
	tt := []struct {
		name        string
		ring        string
		tokenIdx    int
		replication int
		expect      transferWorkload
	}{
		{
			name:        "joining: single new token",
			ring:        "A B C D I+ E F G H",
			tokenIdx:    4, // I+
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{2, 3}}, // transfer BC from E
				"F": []TokenRange{{3, 4}}, // transfer CD from F
				"G": []TokenRange{{4, 5}}, // transfer DI From G
			},
		},

		{
			name:        "joining: single new token around duplicates",
			ring:        "A1 A2 B1 B2 C1 C2 D1 D2 I+ E1 E2 F1 F2 G1 G2 H1 H2",
			tokenIdx:    8, // I+
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{4, 5}, {5, 6}}, // transfer B2C1, C1C2 from E
				"F": []TokenRange{{6, 7}, {7, 8}}, // transfer C2D1, D1D2 from F
				"G": []TokenRange{{8, 9}},         // transfer D2I from G
			},
		},

		{
			name:        "joining: single new token around mixed duplicates",
			ring:        "A1 B1 A2 B2 C1 D1 C2 D2 I+ E1 F1 E2 F2 G1 H1 G2 H2",
			tokenIdx:    8, // I+
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{4, 5}, {5, 6}, {6, 7}}, // transfer B2C1, D1C2, C1D1 from E
				"F": []TokenRange{{7, 8}},                 // transfer C2D2 from F
				"G": []TokenRange{{8, 9}},                 // transfer D2I from G
			},
		},

		{
			name:        "joining: token at start of list",
			ring:        "A+ B C D E F G H",
			tokenIdx:    0, // A+
			replication: 3,
			expect: transferWorkload{
				"B": []TokenRange{{6, 7}}, // transfer FG from B
				"C": []TokenRange{{7, 8}}, // transfer GH from C
				"D": []TokenRange{{8, 1}}, // transfer HA from D
			},
		},

		{
			name:        "joining: token already added on right",
			ring:        "A B C D I1+ I2 E F G H",
			tokenIdx:    4, // I1+
			replication: 3,
			expect:      transferWorkload{}, // we already own everything
		},

		{
			name:        "joining: token already added on left",
			ring:        "A B C D I1 I2+ E F G H",
			tokenIdx:    5, // I2+
			replication: 3,
			expect: transferWorkload{
				"G": []TokenRange{{5, 6}}, // transfer I1I2 from G
			},
		},

		{
			name:        "joining: token already added 2 ingesters over on right",
			ring:        "A B C I1+ D I2 E F G H",
			tokenIdx:    3, // I1+
			replication: 3,
			expect: transferWorkload{
				"D": []TokenRange{{1, 2}}, // transfer AB from D
			},
		},

		{
			name:        "joining: token already added 2 ingesters over on left",
			ring:        "A B C I1 D I2+ E F G H",
			tokenIdx:    5, // I2+
			replication: 3,
			expect: transferWorkload{
				"F": []TokenRange{{4, 5}}, // transfer ID from F
				"G": []TokenRange{{5, 6}}, // transfer DI from G
			},
		},

		{
			name:        "joining: skip over other joining ingesters",
			ring:        "A B C D I+ Z+ Y? X+ W? E F G H",
			tokenIdx:    4, // I+
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{4, 5}}, // transfer DI from E
				"W": []TokenRange{{3, 4}}, // transfer CD from W
				"Y": []TokenRange{{2, 3}}, // transfer BC from y
			},
		},

		{
			name:        "leaving: single leaving token",
			ring:        "A B C D I- E F G H",
			tokenIdx:    4, // I-
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{2, 3}}, // transfer BC to E
				"F": []TokenRange{{3, 4}}, // transfer CD to F
				"G": []TokenRange{{4, 5}}, // transfer DI to G
			},
		},

		{
			name:        "leaving: active token on right",
			ring:        "A B C D I1- I2 E F G H",
			tokenIdx:    4, // I1+
			replication: 3,
			expect:      transferWorkload{}, // we still own everything after I1 leaves
		},

		{
			name:        "leaving: active token on left",
			ring:        "A B C D I1 I2- E F G H",
			tokenIdx:    5, // I2+
			replication: 3,
			expect: transferWorkload{
				"G": []TokenRange{{5, 6}}, // transfer I1I2 to G
			},
		},

		{
			name:        "leaving: token active 2 ingesters over on right",
			ring:        "A B C I1- D I2 E F G H",
			tokenIdx:    3, // I1-
			replication: 3,
			expect: transferWorkload{
				"D": []TokenRange{{1, 2}}, // transfer AB to D
			},
		},

		{
			name:        "leaving: token active added 2 ingesters over on left",
			ring:        "A B C I1 D I2- E F G H",
			tokenIdx:    5, // I2-
			replication: 3,
			expect: transferWorkload{
				"F": []TokenRange{{4, 5}}, // transfer ID to F
				"G": []TokenRange{{5, 6}}, // transfer DI to G
			},
		},

		{
			name:        "leaving: skip over ingesters not joining/active",
			ring:        "A B C D I- Z- Y? X- W? E F G H",
			tokenIdx:    4, // I-
			replication: 3,
			expect: transferWorkload{
				"E": []TokenRange{{2, 3}}, // transfer BC to E
				"F": []TokenRange{{3, 4}}, // transfer CD to F
				"G": []TokenRange{{4, 5}}, // transfer DI to G
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1+ B1 A2 B2",
			tokenIdx:    0, // A1+
			replication: 1,
			expect: transferWorkload{
				"B": []TokenRange{{4, 1}}, // Transfer B2 A1+ from B
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1 B1+ A2 B2",
			tokenIdx:    1, // B1+
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{1, 2}}, // Transfer A1 B1+ from A
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1 B1 A2+ B2",
			tokenIdx:    2, // A2+
			replication: 1,
			expect: transferWorkload{
				"B": []TokenRange{{2, 3}},
			},
		},

		{
			name:        "joining: interleaved tokens",
			ring:        "A1 B1 A2 B2+",
			tokenIdx:    3, // B2+
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{3, 4}},
			},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1+ B2 A1 A2 B3 A3 B4 A4",
			tokenIdx:    0, // B1+
			replication: 1,
			expect:      transferWorkload{},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1 B2+ A1 A2 B3 A3 B4 A4",
			tokenIdx:    1, // B2+
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{1, 2}},
			},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1 B2 A1 A2 B3+ A3 B4 A4",
			tokenIdx:    4, // B3+
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{4, 5}},
			},
		},

		{
			name:        "joining: multiple tokens from same ingester",
			ring:        "B1 B2 A1 A2 B3 A3 B4+ A4",
			tokenIdx:    6, // B4+
			replication: 1,
			expect: transferWorkload{
				"A": []TokenRange{{6, 7}},
			},
		},

		{
			name:        "joining: target can not be same as end range",
			ring:        "A1 B1 C1+ B2 A2 D1 E1",
			tokenIdx:    2, // C1+
			replication: 3,
			expect: transferWorkload{
				"D": []TokenRange{{2, 3}, {1, 2}, {7, 1}},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			d := GenerateRing(t, nil, tc.ring)
			tok := d.Tokens[tc.tokenIdx]

			lc := &Lifecycler{
				ID: tok.Ingester,
				cfg: LifecyclerConfig{
					RingConfig: Config{
						ReplicationFactor: tc.replication,
						HeartbeatTimeout:  time.Second * 3600,
					},
				},
			}

			wl, ok := lc.findTransferWorkload(d, StatefulToken{
				Token: tok.Token,
				State: tok.State,
			})

			require.True(t, ok)
			require.Equal(t, tc.expect, wl)
		})
	}
}

type incTransferState int8

const (
	// initial state
	incTransferNewToken incTransferState = iota

	incTransferRequesting // requesting range
	incTransferUnblocking // unblocking all tokens
	incTransferBlocking   // blocking all ranges
	incTransferSending    // sending ranges
)

type mockIncrementalJoin struct {
	t        *testing.T
	blocks   []TokenRange
	blockMtx sync.Mutex

	state     incTransferState
	requested int
}

func (j *mockIncrementalJoin) StreamTokens() []uint32 { return nil }

func (j *mockIncrementalJoin) BlockRanges(_ context.Context, ranges []TokenRange, addr string) error {
	j.blockMtx.Lock()
	defer j.blockMtx.Unlock()

	if addr == "" {
		return nil
	}

	if j.state == incTransferNewToken {
		j.state = incTransferRequesting
	} else if j.state != incTransferRequesting {
		j.t.Fatal("block range called in unexpected state")
	}

	for _, rg := range ranges {
		for _, r := range j.blocks {
			if r.From == rg.From && r.To == rg.To {
				j.t.Fatal("range already blocked")
			}
		}
	}

	j.blocks = append(j.blocks, ranges...)
	return nil
}

func (j *mockIncrementalJoin) UnblockRanges(_ context.Context, ranges []TokenRange, addr string) error {
	j.blockMtx.Lock()
	defer j.blockMtx.Unlock()

	if addr == "" {
		return nil
	}

	if j.state == incTransferRequesting {
		j.state = incTransferUnblocking
	} else if j.state != incTransferUnblocking {
		j.t.Fatal("unblock range called in unexpected state")
	}

	for _, rg := range ranges {
		idx := -1

		for i, r := range j.blocks {
			if r.From == rg.From && r.To == rg.To {
				idx = i
				break
			}
		}

		if idx == -1 {
			j.t.Fatal("range not blocked")
		}

		j.blocks = append(j.blocks[:idx], j.blocks[idx+1:]...)
	}

	// Ready to move on to a new token
	if len(j.blocks) == 0 {
		j.state = incTransferNewToken
	}

	return nil
}

func (j *mockIncrementalJoin) SendChunkRanges(_ context.Context, ranges []TokenRange, addr string) error {
	j.t.Fatal("unexpected SendChunks")
	return nil
}

func (j *mockIncrementalJoin) RequestChunkRanges(_ context.Context, ranges []TokenRange, addr string, move bool) error {
	j.blockMtx.Lock()
	defer j.blockMtx.Unlock()

	if j.state != incTransferRequesting {
		j.t.Fatal("request chunks called in unexpected state")
	}

	for _, rg := range ranges {
		found := false

		for _, r := range j.blocks {
			if r.From == rg.From && r.To == rg.To {
				found = true
				break
			}
		}

		if !found {
			j.t.Fatal("range not blocked")
		}

		j.requested++
	}
	return nil
}

type casCallback = func(in interface{}) (out interface{}, retry bool, err error)
type casFunc = func(ctx context.Context, key string, f casCallback) error

// enforceSingleState wraps around a CAS function and will mark a test as
// failure if there are more than one token in the given set of states.
func enforceSingleState(t *testing.T, cas casFunc, states ...State) casFunc {
	return func(ctx context.Context, key string, f casCallback) error {
		return cas(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
			out, retry, err = f(in)

			d, ok := out.(*Desc)
			if !ok {
				return
			}

			count := 0

			for _, state := range states {
				count += len(d.FindTokensByState(state))
			}

			if count > 1 {
				t.Error("more than one token in state")
			}

			return
		})
	}
}

func TestIncrementalJoin(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()

	inMemory := consul.NewInMemoryClient(codec)
	mockClient := &MockClient{}
	mockClient.MapFunctions(inMemory)
	mockClient.CASFunc = enforceSingleState(t, inMemory.CAS, PENDING, JOINING)
	ringConfig.KVStore.Mock = mockClient

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("ing-%d", i)

		lcc := testLifecyclerConfig(ringConfig, id)
		lcc.Addr = id
		lcc.NumTokens = 64
		lc, err := NewLifecycler(lcc, &nopFlushTransferer{}, nil, id)
		require.NoError(t, err)
		lc.Start()

		// Check this ingester joined
		test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
			d, err := r.KVClient.Get(context.Background(), ConsulKey)
			require.NoError(t, err)
			if d == nil {
				return false
			}
			_, exist := d.(*Desc).Ingesters[id]
			return exist
		})
	}

	mock := mockIncrementalJoin{t: t}

	lcc := testLifecyclerConfig(ringConfig, "joiner")
	lcc.RangeUnblockDelay = time.Duration(0)
	lcc.NumTokens = 64
	lcc.JoinIncrementalTransfer = true
	lc, err := NewLifecycler(lcc, &nopFlushTransferer{}, &mock, "joiner")
	require.NoError(t, err)
	lc.Start()

	// Wait for ingester to join
	test.Poll(t, 5000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		if d == nil {
			return false
		}
		ing, exist := d.(*Desc).Ingesters["joiner"]
		return exist && ing.State == ACTIVE
	})

	mock.blockMtx.Lock()
	defer mock.blockMtx.Unlock()

	require.Greater(t, mock.requested, 0)
	require.Equal(t, incTransferNewToken, mock.state)
	require.Len(t, mock.blocks, 0)
}

type mockIncrementalLeave struct {
	t        *testing.T
	blocks   []TokenRange
	blockMtx sync.Mutex

	state incTransferState
	sent  int
}

func (j *mockIncrementalLeave) StreamTokens() []uint32 { return nil }

func (j *mockIncrementalLeave) BlockRanges(_ context.Context, ranges []TokenRange, addr string) error {
	j.blockMtx.Lock()
	defer j.blockMtx.Unlock()

	if addr == "" {
		return nil
	}

	if j.state == incTransferNewToken {
		j.state = incTransferBlocking
	} else if j.state != incTransferBlocking {
		j.t.Fatal("block range called in unexpected state")
	}

	for _, rg := range ranges {
		for _, r := range j.blocks {
			if r.From == rg.From && r.To == rg.To {
				j.t.Fatal("range already blocked")
			}
		}
	}

	j.blocks = append(j.blocks, ranges...)
	return nil
}

func (j *mockIncrementalLeave) UnblockRanges(_ context.Context, ranges []TokenRange, addr string) error {
	j.blockMtx.Lock()
	defer j.blockMtx.Unlock()

	if addr == "" {
		return nil
	}

	if j.state != incTransferSending {
		j.t.Fatal("unexpected unblock range")
	}

	for _, rg := range ranges {
		idx := -1

		for i, r := range j.blocks {
			if r.From == rg.From && r.To == rg.To {
				idx = i
				break
			}
		}

		if idx == -1 {
			j.t.Fatal("range not blocked")
		}

		j.blocks = append(j.blocks[:idx], j.blocks[idx+1:]...)

	}

	// Ready to move on to a new token
	if len(j.blocks) == 0 {
		j.state = incTransferNewToken
	}
	return nil
}

func (j *mockIncrementalLeave) SendChunkRanges(_ context.Context, ranges []TokenRange, addr string) error {
	j.blockMtx.Lock()
	defer j.blockMtx.Unlock()

	if j.state == incTransferBlocking {
		j.state = incTransferSending
	} else if j.state != incTransferSending {
		j.t.Fatal("unexpected send chunks")
	}

	for _, rg := range ranges {
		found := false

		for _, r := range j.blocks {
			if r.From == rg.From && r.To == rg.To {
				found = true
				break
			}
		}

		if !found {
			j.t.Fatal("range not blocked")
		}

		j.sent++
	}

	return nil
}

func (j *mockIncrementalLeave) RequestChunkRanges(_ context.Context, ranges []TokenRange, addr string, move bool) error {
	j.t.Fatal("unexpected RequestChunks")
	return nil
}

func TestIncrementalLeave(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()

	inMemory := consul.NewInMemoryClient(codec)
	mockClient := &MockClient{}
	mockClient.MapFunctions(inMemory)
	mockClient.CASFunc = enforceSingleState(t, inMemory.CAS, LEAVING)
	ringConfig.KVStore.Mock = mockClient

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("ing-%d", i)

		lcc := testLifecyclerConfig(ringConfig, id)
		lcc.Addr = id
		lcc.NumTokens = 64
		lc, err := NewLifecycler(lcc, &nopFlushTransferer{}, nil, id)
		require.NoError(t, err)
		lc.Start()

		// Check this ingester joined
		test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
			d, err := r.KVClient.Get(context.Background(), ConsulKey)
			require.NoError(t, err)
			if d == nil {
				return false
			}
			_, exist := d.(*Desc).Ingesters[id]
			return exist
		})
	}

	mock := mockIncrementalLeave{t: t}

	lcc := testLifecyclerConfig(ringConfig, "leaver")
	lcc.NumTokens = 64
	lcc.LeaveIncrementalTransfer = true
	lc, err := NewLifecycler(lcc, &nopFlushTransferer{}, &mock, "leaver")
	require.NoError(t, err)
	lc.Start()

	// Wait for ingester to join
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		if d == nil {
			return false
		}
		ing, exist := d.(*Desc).Ingesters["leaver"]
		return exist && ing.State == ACTIVE
	})

	lc.Shutdown()
	test.Poll(t, 5000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		if d == nil {
			return false
		}
		_, exist := d.(*Desc).Ingesters["leaver"]
		return !exist
	})

	mock.blockMtx.Lock()
	defer mock.blockMtx.Unlock()

	require.Greater(t, mock.sent, 0)
	require.Equal(t, incTransferNewToken, mock.state)
	require.Len(t, mock.blocks, 0)
}

func TestTokenStateChanges(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	codec := GetCodec()
	ringConfig.KVStore.Mock = consul.NewInMemoryClient(codec)

	r, err := New(ringConfig, "ingester")
	require.NoError(t, err)
	defer r.Stop()

	lcc := testLifecyclerConfig(ringConfig, "ingester")
	lcc.NumTokens = 128
	lc, err := NewLifecycler(lcc, &nopFlushTransferer{}, nil, "ingester")
	require.NoError(t, err)
	lc.Start()

	// Wait for ingester to join
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ConsulKey)
		require.NoError(t, err)
		if d == nil {
			return false
		}
		ing, exist := d.(*Desc).Ingesters["ingester"]
		return exist && ing.State == ACTIVE
	})

	// All tokens should be in the same state
	d, err := r.KVClient.Get(context.Background(), ConsulKey)
	require.NoError(t, err)
	require.NotNil(t, d)
	desc := d.(*Desc)

	for _, tok := range desc.Tokens {
		require.Equal(t, ACTIVE, tok.State)
	}

	// Update a single token into PENDING
	toks := lc.getTokens()
	toks[64].State = PENDING
	lc.setTokens(toks)
	err = lc.updateConsul(context.Background())
	require.NoError(t, err)

	// Only that one token should have updated
	changedTok := toks[64]

	d, err = r.KVClient.Get(context.Background(), ConsulKey)
	require.NoError(t, err)
	require.NotNil(t, d)
	desc = d.(*Desc)

	for _, tok := range desc.Tokens {
		if tok.Token == changedTok.Token {
			require.Equal(t, PENDING, tok.State)
			continue
		}

		require.Equal(t, ACTIVE, tok.State)
	}
}
