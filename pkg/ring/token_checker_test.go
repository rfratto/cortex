package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func makeSequentialTokenGenerator() TokenGeneratorFunc {
	n := 0

	return func(num int, taken []StatefulToken, state State) []StatefulToken {
		start := n
		end := start + num

		ret := make([]StatefulToken, num)
		for i := n; i < end; i++ {
			ret[i-start] = StatefulToken{
				Token: uint32(i + 1),
				State: state,
			}
		}

		n = end
		return ret
	}
}

type mockTokenCheckerTransfer struct {
	IncrementalTransferer
	tokens []uint32
}

func (t *mockTokenCheckerTransfer) StreamTokens() []uint32 {
	return t.tokens
}

func TestTokenChecker(t *testing.T) {
	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.ReplicationFactor = 1
	codec := GetCodec()

	inMemory := consul.NewInMemoryClient(codec)
	mockClient := &MockClient{}
	mockClient.MapFunctions(inMemory)
	ringConfig.KVStore.Mock = mockClient

	r, err := New(ringConfig, "ring")
	require.NoError(t, err)
	defer r.Stop()

	transfer := &mockTokenCheckerTransfer{}
	generator := makeSequentialTokenGenerator()

	var lifecyclers []*Lifecycler
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("lc-%d", i)

		lcc := testLifecyclerConfig(ringConfig, id)
		lcc.Addr = id
		lcc.NumTokens = 128
		lcc.GenerateTokens = generator

		lc, err := NewLifecycler(lcc, &nopFlushTransferer{true}, transfer, id)
		require.NoError(t, err)
		lc.Start()
		defer lc.Shutdown()

		test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
			d, err := r.KVClient.Get(context.Background(), ConsulKey)
			require.NoError(t, err)
			return checkInRing(d, lc.ID)
		})

		lifecyclers = append(lifecyclers, lc)
	}

	// Populate transfer with all tokens in lifecycler
	for _, tok := range lifecyclers[0].getTokens() {
		transfer.tokens = append(transfer.tokens, tok.Token-1)
	}

	tc := NewTokenChecker(TokenCheckerConfig{
		CheckOnCreate:   true,
		CheckOnAppend:   true,
		CheckOnTransfer: true,
		CheckOnInterval: time.Duration(50 * time.Millisecond),
	}, lifecyclers[0])
	err = tc.Start()
	require.NoError(t, err)
	defer tc.Shutdown()

	calledHandler := atomic.NewBool(false)
	tc.UnexpectedTokenHandler = func(l []uint32) {
		calledHandler.Store(true)
	}

	// Make sure CheckToken with token == 1 returns true
	test.Poll(t, time.Millisecond*500, true, func() interface{} {
		return tc.CheckToken(1)
	})

	// Make sure CheckToken with a token out of range returns false.
	require.False(t, tc.CheckToken(
		transfer.tokens[len(transfer.tokens)-1]+1,
	))

	// Make sure all tokens are valid.
	test.Poll(t, time.Millisecond*500, true, func() interface{} {
		return tc.CheckAllTokens()
	})

	require.True(t, calledHandler.Load())
}
