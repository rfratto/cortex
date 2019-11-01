package ring

import (
	"context"
	"errors"
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

// Enabled determines whether or not a generated TokenChecker should
// spawn any goroutines, based on if anything in the TokenCheckerConfig
// is set to a non-zero value.
func (c *TokenCheckerConfig) Enabled() bool {
	return c.CheckOnInterval > 0 || c.CheckOnCreate || c.CheckOnAppend || c.CheckOnTransfer
}

// RegisterFlags adds flags required to configure a TokenChecker to
// the provided FlagSet.
func (c *TokenCheckerConfig) RegisterFlags(f *flag.FlagSet) {
	c.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config a TokenChecker to
// the given FlagSet, prefixing each flag with the value provided by prefix.
func (c *TokenCheckerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&c.CheckOnCreate, prefix+"token-checker.check-on-create", false, "Check that newly created streams fall within expected token ranges")
	f.BoolVar(&c.CheckOnAppend, prefix+"token-checker.check-on-append", false, "Check that existing streams appended to fall within expected token ranges")
	f.BoolVar(&c.CheckOnTransfer, prefix+"token-checker.check-on-transfer", false, "Check that streams transferred in using the transfer mechanism fall within expected token ranges")
	f.DurationVar(&c.CheckOnInterval, prefix+"token-checker.check-on-interval", time.Duration(0), "Period with which to check that all in-memory streams fall within expected token ranges. 0 to disable.")
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
	mut sync.Mutex

	cfg        TokenCheckerConfig
	lifecycler *Lifecycler

	// Lifecycle control
	ctx    context.Context
	cancel context.CancelFunc

	// Updated throughout the lifetime of a TokenChecker
	ring           *Desc
	expectedRanges []TokenRange

	// UnexpectedTokenHandler is invoked by the TokenChecker whenever
	// CheckAllTokens is called, even when no unexpected tokens are
	// found. If nil, is a no-op.
	UnexpectedTokenHandler func(tokens []uint32)
}

// NewTokenChecker makes and starts a new TokenChecker.
func NewTokenChecker(cfg TokenCheckerConfig, lc *Lifecycler) *TokenChecker {
	tc := &TokenChecker{
		cfg:        cfg,
		lifecycler: lc,
	}

	return tc
}

// Start starts the TokenChecker. Fails if TokenChecker has already started.
func (tc *TokenChecker) Start() error {
	// Early exit: not watching for anything
	if !tc.cfg.Enabled() {
		return nil
	} else if tc.cancel != nil {
		return errors.New("TokenChecker already running")
	}

	tc.ctx, tc.cancel = context.WithCancel(context.Background())

	go tc.watchRing(tc.ctx)
	go tc.loop()
	return nil
}

// Shutdown stops the Token Checker. It will stop watching the ring
// for changes and stop checking that tokens are valid on an interval.
func (tc *TokenChecker) Shutdown() {
	tc.mut.Lock()
	defer tc.mut.Unlock()

	if tc.cancel == nil {
		return
	}

	tc.cancel()
	tc.cancel = nil
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
	var invalid []uint32

	toks := tc.lifecycler.incTransferer.StreamTokens()
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
	tc.lifecycler.KVStore.WatchKey(ctx, ConsulKey, func(v interface{}) bool {
		if v == nil {
			return true
		}

		desc := v.(*Desc)
		desc.Tokens = migrateRing(desc)
		tc.mut.Lock()
		tc.ring = desc
		tc.mut.Unlock()
		tc.updateExpectedRanges()

		return true
	})
}

// updateExpectedRanges goes through the ring and finds all expected ranges
// given the current set of tokens in a Lifecycler.
func (tc *TokenChecker) updateExpectedRanges() {
	var expected []TokenRange

	tokens := tc.lifecycler.getTokens()
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
				level.Error(util.Logger).Log("msg", "unable to update expected token ranges", "err", err)
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
					level.Error(util.Logger).Log("msg", "unable to update expected token ranges", "err", err)
					return
				}

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
		case <-tc.ctx.Done():
			break loop
		}
	}
}
