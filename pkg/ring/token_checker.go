package ring

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// TokenCheckerConfig is the config to configure a TokenChecker.
type TokenCheckerConfig struct {
	// Used by ingester when set to true.
	CheckOnCreate   bool `yaml:"check_on_create,omitempty"`
	CheckOnAppend   bool `yaml:"check_on_append,omitempty"`
	CheckOnTransfer bool `yaml:"check_on_transfer,omitempty"`

	// Used by TokenChecker on the interval
	CheckOnInterval time.Duration `yaml:"check_on_interval"`
}

// RegisterFlags adds flags required to configure a TokenChecker to
// the provided FlagSet.
func (cfg *TokenCheckerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config a TokenChecker to
// the given FlagSet, prefixing each flag with the value provided by prefix.
func (cfg *TokenCheckerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.CheckOnCreate, prefix+"token-checker.check-on-create", false, "Check that newly created streams fall within expected token ranges")
	f.BoolVar(&cfg.CheckOnCreate, prefix+"token-checker.check-on-append", false, "Check that existing streams appended to fall within expected token ranges")
	f.BoolVar(&cfg.CheckOnCreate, prefix+"token-checker.check-on-transfer", false, "Check that streams transferred in using the transfer mechanism fall within expected token ranges")
	f.DurationVar(&cfg.CheckOnInterval, prefix+"token-checker.check-on-interval", time.Duration(0), "Period with which to check that all in-memory streams fall within expected token ranges. 0 to disable.")
}

// A TokenChecker is responsible for validating that streams written to an
// ingester have tokens that fall within an expected range of values.
// Appropriate values depend on where an ingester's tokens are placed in the
// ring and what its neighbors are.
//
// Checking that newly created streams fall within the expected token
// ranges ensures that writes to the ingester are distributed properly,
// while checking for validity of streams on an interval ensures that
// an ingester's memory only contains appropriate tokens as the ingesters
// in the ring change over time.
type TokenChecker struct {
	// UnexpectedTokenHandler is invoked by the TokenChecker whenever
	// CheckAllTokens is called, even when no unexpected tokens are
	// found. If nil, is a no-op.
	UnexpectedTokenHandler func(tokens []uint32)

	mut        sync.Mutex
	cfg        TokenCheckerConfig
	lifecycler *Lifecycler

	// Lifecycle control
	quit   chan struct{}
	cancel context.CancelFunc

	// Updated throughout the lifetime of a TokenChecker
	ring           *Desc
	expectedRanges []TokenRange
}

// NewTokenChecker makes and starts a new TokenChecker.
func NewTokenChecker(cfg TokenCheckerConfig, lc *Lifecycler) (*TokenChecker, error) {
	tc := &TokenChecker{
		cfg:        cfg,
		lifecycler: lc,

		quit: make(chan struct{}),
	}

	// Early exit: not watching for anything
	if !tc.Enabled() {
		return tc, nil
	}

	var ctx context.Context
	ctx, tc.cancel = context.WithCancel(context.Background())
	go tc.watchRing(ctx)

	go tc.loop()
	return tc, nil
}

// Enabled returns whether the TokenChecker is watching the ring. Returns
// true only when one of the CheckOn* configuration variables are
// set to their non-zero values.
func (tc *TokenChecker) Enabled() bool {
	return tc.cfg.CheckOnInterval != time.Duration(0) ||
		tc.cfg.CheckOnCreate ||
		tc.cfg.CheckOnAppend ||
		tc.cfg.CheckOnTransfer
}

// Shutdown stops the Token Checker. It will stop watching the ring
// for changes and stop checking that tokens are valid on an interval.
func (tc *TokenChecker) Shutdown() {
	close(tc.quit)
}

// CheckToken iterates over all expected ranges and returns true
// when the token falls within one of those ranges.
func (tc *TokenChecker) CheckToken(token uint32) bool {
	tc.mut.Lock()
	defer tc.mut.Unlock()

	for _, rg := range tc.expectedRanges {
		if rg.Contains(token) {
			return true
		}
	}

	return false
}

// CheckAllTokens invokes CheckToken for all current untransferred
// tokens found in the stored IncrementalTransferer. Invokes
// tc.InvalidTokenHandler when an invalid token was found. Returns true when
// all tokens are valid.
func (tc *TokenChecker) CheckAllTokens() bool {
	toks := tc.lifecycler.incTransferer.StreamTokens()
	invalid := []uint32{}

	for _, tok := range toks {
		valid := tc.CheckToken(tok)
		if !valid {
			invalid = append(invalid, tok)
		}
	}

	numInvalid := len(invalid)

	if tc.UnexpectedTokenHandler != nil {
		tc.UnexpectedTokenHandler(invalid)
	}

	return numInvalid == 0
}

// watchRing watches the ring and updates the set of expected tokens
// for a TokenChecker.
func (tc *TokenChecker) watchRing(ctx context.Context) {
	numIngesters := 0

	tc.lifecycler.KVStore.WatchKey(ctx, ConsulKey, func(v interface{}) bool {
		if v == nil {
			level.Info(util.Logger).Log("msg", "ring doesn't exist in consul yet")
			return true
		}

		level.Debug(util.Logger).Log("msg", "updating token checker ring definition")

		desc := v.(*Desc)
		desc.Tokens = migrateRing(desc)
		tc.mut.Lock()
		tc.ring = desc
		tc.mut.Unlock()
		tc.updateExpectedRanges()

		if len(desc.Ingesters) != numIngesters {
			level.Debug(util.Logger).Log(
				"msg", "number of ingesters in ring grew",
				"ring_definition", desc.String(),
			)
			numIngesters = len(desc.Ingesters)
		}

		return true
	})
}

// updateExpectedRanges goes through the ring and finds all expected ranges
// given the current set of tokens in a Lifecycler.
func (tc *TokenChecker) updateExpectedRanges() {
	tokens := tc.lifecycler.getTokens()
	expected := []TokenRange{}

	for _, tok := range tokens {
		for replica := 0; replica < tc.lifecycler.cfg.RingConfig.ReplicationFactor; replica++ {
			endRanges, err := tc.ring.Predecessors(NeighborOptions{
				Start:        tok,
				Offset:       replica,
				Op:           Read,
				IncludeStart: true,
				MaxHeartbeat: tc.lifecycler.cfg.RingConfig.HeartbeatTimeout,
			})
			if err != nil {
				level.Error(util.Logger).Log(
					"msg", "unable to update expected token ranges",
					"err", err,
				)
				return
			}

			for _, endRange := range endRanges {
				startRange, err := tc.ring.Successor(NeighborOptions{
					Start:        endRange.StatefulToken(),
					Offset:       -1,
					Op:           Read,
					MaxHeartbeat: tc.lifecycler.cfg.RingConfig.HeartbeatTimeout,
				})
				if err != nil {
					level.Error(util.Logger).Log(
						"msg", "unable to update expected token ranges",
						"err", err,
					)
					return
				}

				level.Debug(util.Logger).Log(
					"msg", "found expected range",
					"ingester_id", tc.lifecycler.ID,
					"start_range", startRange.Token,
					"end_range", endRange.Token,
				)

				expected = append(expected, TokenRange{
					From: startRange.Token,
					To:   endRange.Token,
				})
			}
		}
	}

	tc.mut.Lock()
	tc.expectedRanges = expected
	tc.mut.Unlock()
}

// loop will invoke CheckAllTokens based on the check interval.
func (tc *TokenChecker) loop() {
	ticker := time.NewTicker(tc.cfg.CheckOnInterval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ticker.C:
			tc.CheckAllTokens()
		case <-tc.quit:
			break loop
		}
	}

	if tc.cancel != nil {
		tc.cancel()
	}
}
