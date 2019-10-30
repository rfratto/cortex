package ring

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	consulHeartbeats = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_member_consul_heartbeats_total",
		Help: "The total number of heartbeats sent to consul.",
	}, []string{"name"})
	tokensOwned = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_member_ring_tokens_owned",
		Help: "The number of tokens owned in the ring.",
	}, []string{"name"})
	tokensToOwn = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_member_ring_tokens_to_own",
		Help: "The number of tokens to own in the ring.",
	}, []string{"name"})

	transferDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_incremental_transfer_duration",
		Help:    "Duration (in seconds) of incremental transfer (i.e., join or leave)",
		Buckets: prometheus.ExponentialBuckets(10, 2, 8), // Biggest bucket is 10*2^(9-1) = 2560, or 42 mins.
	}, []string{"op", "status", "name"})

	shutdownDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_shutdown_duration_seconds",
		Help:    "Duration (in seconds) of cortex shutdown procedure (ie transfer or flush).",
		Buckets: prometheus.ExponentialBuckets(10, 2, 8), // Biggest bucket is 10*2^(9-1) = 2560, or 42 mins.
	}, []string{"op", "status", "name"})
)

// LifecyclerConfig is the config to build a Lifecycler.
type LifecyclerConfig struct {
	RingConfig Config `yaml:"ring,omitempty"`

	// Config for the ingester lifecycle control
	ListenPort               *int
	NumTokens                int           `yaml:"num_tokens,omitempty"`
	HeartbeatPeriod          time.Duration `yaml:"heartbeat_period,omitempty"`
	ObservePeriod            time.Duration `yaml:"observe_period,omitempty"`
	JoinAfter                time.Duration `yaml:"join_after,omitempty"`
	MinReadyDuration         time.Duration `yaml:"min_ready_duration,omitempty"`
	UnusedFlag               bool          `yaml:"claim_on_rollout,omitempty"` // DEPRECATED - left for backwards-compatibility
	JoinIncrementalTransfer  bool          `yaml:"join_incremental_transfer,omitempty"`
	LeaveIncrementalTransfer bool          `yaml:"leave_incremental_transfer,omitempty"`
	UpdateRingDuringTransfer bool          `yaml:"update_ring_during_transfer,omitempty"`
	RangeUnblockDelay        time.Duration `yaml:"range_unblock_delay,omitempty"`
	NormaliseTokens          bool          `yaml:"normalise_tokens,omitempty"`
	InfNames                 []string      `yaml:"interface_names"`
	FinalSleep               time.Duration `yaml:"final_sleep"`

	// For testing, you can override the address and ID of this ingester
	Addr           string `yaml:"address"`
	Port           int
	ID             string
	SkipUnregister bool

	// Function used to generate tokens, can be mocked from
	// tests
	GenerateTokens TokenGeneratorFunc `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *LifecyclerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet.
func (cfg *LifecyclerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RingConfig.RegisterFlagsWithPrefix(prefix, f)

	// In order to keep backwards compatibility all of these need to be prefixed
	// with "ingester."
	if prefix == "" {
		prefix = "ingester."
	}

	f.IntVar(&cfg.NumTokens, prefix+"num-tokens", 128, "Number of tokens for each ingester.")
	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 5*time.Second, "Period at which to heartbeat to consul.")
	f.DurationVar(&cfg.JoinAfter, prefix+"join-after", 0*time.Second, "Period to wait for a claim from another member; will join automatically after this.")
	f.DurationVar(&cfg.ObservePeriod, prefix+"observe-period", 0*time.Second, "Observe tokens after generating to resolve collisions. Useful when using gossiping ring.")
	f.DurationVar(&cfg.MinReadyDuration, prefix+"min-ready-duration", 1*time.Minute, "Minimum duration to wait before becoming ready. This is to work around race conditions with ingesters exiting and updating the ring.")
	flagext.DeprecatedFlag(f, prefix+"claim-on-rollout", "DEPRECATED. This feature is no longer optional.")
	f.BoolVar(&cfg.JoinIncrementalTransfer, prefix+"join-incremental-transfer", false, "Request chunks from neighboring ingesters on join. Disables the handoff process when set and ignores the -ingester.join-after flag.")
	f.BoolVar(&cfg.LeaveIncrementalTransfer, prefix+"leave-incremental-transfer", false, "Send chunks to neighboring ingesters on leave. Takes precedence over chunk flushing when set and disables handoff.")
	f.BoolVar(&cfg.UpdateRingDuringTransfer, prefix+"update-ring-during-transfer", true, "Send ring updates for each token that incrementally transfers. Makes transfer slower but helps the ingester receive writes faster.")
	f.DurationVar(&cfg.RangeUnblockDelay, prefix+"range-unblock-delay", 5*time.Second, "How long after the incremental join process should ranges be unblocked from target ingesters. Set to a value to provide enough time for distributors to receieve consul update.")
	f.BoolVar(&cfg.NormaliseTokens, prefix+"normalise-tokens", false, "Store tokens in a normalised fashion to reduce allocations.")
	f.DurationVar(&cfg.FinalSleep, prefix+"final-sleep", 30*time.Second, "Duration to sleep for before exiting, to ensure metrics are scraped.")

	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	cfg.InfNames = []string{"eth0", "en0"}
	f.Var((*flagext.Strings)(&cfg.InfNames), prefix+"lifecycler.interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.Addr, prefix+"lifecycler.addr", "", "IP address to advertise in consul.")
	f.IntVar(&cfg.Port, prefix+"lifecycler.port", 0, "port to advertise in consul (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.ID, prefix+"lifecycler.ID", hostname, "ID to register into consul.")
}

// FlushTransferer controls the shutdown of an ingester.
type FlushTransferer interface {
	StopIncomingRequests()
	Flush()
	TransferOut(ctx context.Context) error
}

// TokenRange represents a range of tokens, starting inclusively
// with From and ending exclusively at To.
type TokenRange struct {
	From uint32
	To   uint32
}

// Contains indicates that a key falls within a given range.
func (r TokenRange) Contains(key uint32) bool {
	if r.From > r.To {
		// Wraps around the ring. It's in the range as long as the
		// key is in the range of [from, 2<<32-1] or [0, to).
		return key >= r.From || key < r.To
	}

	return key >= r.From && key < r.To
}

// IncrementalTransferer controls partial transfer of chunks as the tokens in a
// ring grows or shrinks.
type IncrementalTransferer interface {
	// BlockRanges should inform an ingester at targetAddr to no longer accept any
	// writes in ranges of [from, to). If targetAddr is an empty string, should
	// affect the logal ingester.
	BlockRanges(ctx context.Context, ranges []TokenRange, targetAddr string) error

	// UnblockRanges should inform an ingester at targetAddr that it can remove
	// a block caused by BlockRange. If targetAddr is an empty string, should
	// affect the logal ingester.
	UnblockRanges(ctx context.Context, ranges []TokenRange, targetAddr string) error

	// SendChunkRanges should connect to the target addr and send all chunks for
	// streams whose fingerprint falls within the provided token ranges.
	SendChunkRanges(ctx context.Context, ranges []TokenRange, targetAddr string) error

	// RequestChunkRanges should connect to the target addr and request all chunks
	// for streams whose fingerprint falls within the provided token ranges.
	//
	// If move is true, transferred data should be removed from the target's memory.
	RequestChunkRanges(ctx context.Context, ranges []TokenRange, targetAddr string, move bool) error

	// StreamTokens should return a list of tokens corresponding to in-memory
	// streams for the ingester. Used for reporting purposes.
	StreamTokens() []uint32
}

// Lifecycler is responsible for managing the lifecycle of entries in the ring.
type Lifecycler struct {
	cfg             LifecyclerConfig
	flushTransferer FlushTransferer
	incTransferer   IncrementalTransferer
	KVStore         kv.Client

	// Controls the lifecycle of the ingester
	quit      chan struct{}
	done      sync.WaitGroup
	actorChan chan func()
	joined    chan struct{}

	// These values are initialised at startup, and never change
	ID       string
	Addr     string
	RingName string

	// We need to remember the ingester state just in case consul goes away and comes
	// back empty.  And it changes during lifecycle of ingester.
	stateMtx            sync.Mutex
	state               State
	transitioningTokens []StatefulToken
	tokens              []StatefulToken

	// Controls the ready-reporting
	readyLock sync.Mutex
	startTime time.Time
	ready     bool

	// Keeps stats updated at every heartbeat period
	countersLock          sync.RWMutex
	healthyInstancesCount int

	generateTokens TokenGeneratorFunc
}

// NewLifecycler makes and starts a new Lifecycler.
func NewLifecycler(cfg LifecyclerConfig, flushTransferer FlushTransferer, incTransferer IncrementalTransferer, name string) (*Lifecycler, error) {
	addr := cfg.Addr
	if addr == "" {
		var err error
		addr, err = util.GetFirstAddressOf(cfg.InfNames)
		if err != nil {
			return nil, err
		}
	}
	port := cfg.Port
	if port == 0 {
		port = *cfg.ListenPort
	}
	codec := GetCodec()
	store, err := kv.NewClient(cfg.RingConfig.KVStore, codec)
	if err != nil {
		return nil, err
	}

	l := &Lifecycler{
		cfg:             cfg,
		flushTransferer: flushTransferer,
		incTransferer:   incTransferer,
		KVStore:         store,

		Addr:     fmt.Sprintf("%s:%d", addr, port),
		ID:       cfg.ID,
		RingName: name,

		quit:      make(chan struct{}),
		actorChan: make(chan func()),
		joined:    make(chan struct{}),

		state:          PENDING,
		startTime:      time.Now(),
		generateTokens: cfg.GenerateTokens,
	}

	if l.generateTokens == nil {
		l.generateTokens = GenerateTokens
	}

	tokensToOwn.WithLabelValues(l.RingName).Set(float64(cfg.NumTokens))

	return l, nil
}

// Start the lifecycler
func (i *Lifecycler) Start() {
	i.done.Add(1)
	go i.loop()
}

// WaitJoined blocks until the Lifecycler is ACTIVE in the ring.
func (i *Lifecycler) WaitJoined() {
	<-i.joined
}

// CheckReady is used to rate limit the number of ingesters that can be coming or
// going at any one time, by only returning true if all ingesters are active.
// The state latches: once we have gone ready we don't go un-ready
func (i *Lifecycler) CheckReady(ctx context.Context) error {
	i.readyLock.Lock()
	defer i.readyLock.Unlock()

	if i.ready {
		return nil
	}

	// Ingester always take at least minReadyDuration to become ready to work
	// around race conditions with ingesters exiting and updating the ring
	if time.Now().Sub(i.startTime) < i.cfg.MinReadyDuration {
		return fmt.Errorf("waiting for %v after startup", i.cfg.MinReadyDuration)
	}

	desc, err := i.KVStore.Get(ctx, ConsulKey)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error talking to consul", "err", err)
		return fmt.Errorf("error talking to consul: %s", err)
	}

	if len(i.getTokens()) == 0 {
		return fmt.Errorf("this ingester owns no tokens")
	}

	ringDesc, ok := desc.(*Desc)
	if !ok || ringDesc == nil {
		return fmt.Errorf("no ring returned from consul")
	}

	if err := ringDesc.Ready(time.Now(), i.cfg.RingConfig.HeartbeatTimeout); err != nil {
		return err
	}

	i.ready = true
	return nil
}

// GetState returns the state of this ingester.
func (i *Lifecycler) GetState() State {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	return i.state
}

func (i *Lifecycler) setState(state State) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	i.state = state
}

func (i *Lifecycler) setTokensState(state State) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	for n := range i.tokens {
		i.tokens[n].State = state
	}
}

// ChangeState of the ingester, for use off of the loop() goroutine.
func (i *Lifecycler) ChangeState(ctx context.Context, state State) error {
	err := make(chan error)
	i.actorChan <- func() {
		err <- i.changeState(ctx, state)
	}
	return <-err
}

func (i *Lifecycler) getTransitioningTokens() []StatefulToken {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	ret := make([]StatefulToken, len(i.transitioningTokens))
	copy(ret, i.transitioningTokens)
	return ret
}

func (i *Lifecycler) getTokens() []StatefulToken {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	ret := make([]StatefulToken, len(i.tokens))
	copy(ret, i.tokens)
	return ret
}

func (i *Lifecycler) addToken(token StatefulToken) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	// Update token if it already exists
	for idx, t := range i.tokens {
		if t.Token == token.Token {
			i.tokens[idx] = token
			return
		}
	}

	tokensOwned.WithLabelValues(i.RingName).Inc()
	i.tokens = append(i.tokens, token)
}

func (i *Lifecycler) removeToken(token StatefulToken) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	idx := -1
	for i, tok := range i.tokens {
		if tok.Token == token.Token {
			idx = i
			break
		}
	}

	if idx == -1 {
		return
	}

	tokensOwned.WithLabelValues(i.RingName).Dec()
	i.tokens = append(i.tokens[:idx], i.tokens[idx+1:]...)
}

func (i *Lifecycler) setTransitioningTokens(tokens []StatefulToken) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	i.transitioningTokens = make([]StatefulToken, len(tokens))
	copy(i.transitioningTokens, tokens)
}

func (i *Lifecycler) setTokens(tokens []StatefulToken) {
	tokensOwned.WithLabelValues(i.RingName).Set(float64(len(tokens)))

	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()

	i.tokens = make([]StatefulToken, len(tokens))
	copy(i.tokens, tokens)
}

// ClaimTokensFor takes all the tokens for the supplied ingester and assigns them to this ingester.
//
// For this method to work correctly (especially when using gossiping), source ingester (specified by
// ingesterID) must be in the LEAVING state, otherwise ring's merge function may detect token conflict and
// assign token to the wrong ingester. While we could check for that state here, when this method is called,
// transfers have already finished -- it's better to check for this *before* transfers start.
func (i *Lifecycler) ClaimTokensFor(ctx context.Context, ingesterID string) error {
	err := make(chan error)

	i.actorChan <- func() {
		var tokens []StatefulToken

		claimTokens := func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc, ok := in.(*Desc)
			if !ok || ringDesc == nil {
				return nil, false, fmt.Errorf("Cannot claim tokens in an empty ring")
			}

			tokens = ringDesc.ClaimTokens(ingesterID, i.ID, i.cfg.NormaliseTokens)
			// update timestamp to give gossiping client a chance to register ring change.
			ing := ringDesc.Ingesters[i.ID]
			ing.Timestamp = time.Now().Unix()
			ringDesc.Ingesters[i.ID] = ing

			// Set the state of the tokens to our ingester's state.
			state := i.GetState()
			for n := range tokens {
				tokens[n].State = state
			}

			return ringDesc, true, nil
		}

		if err := i.KVStore.CAS(ctx, ConsulKey, claimTokens); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to write to consul", "err", err)
		}

		i.setTokens(tokens)
		close(i.joined)
		err <- nil
	}

	return <-err
}

// HealthyInstancesCount returns the number of healthy instances in the ring, updated
// during the last heartbeat period
func (i *Lifecycler) HealthyInstancesCount() int {
	i.countersLock.RLock()
	defer i.countersLock.RUnlock()

	return i.healthyInstancesCount
}

// Shutdown the lifecycle.  It will:
// - send chunks to another ingester, if it can.
// - otherwise, flush chunks to the chunk store.
// - remove config from Consul.
// - block until we've successfully shutdown.
func (i *Lifecycler) Shutdown() {
	i.flushTransferer.StopIncomingRequests()

	// closing i.quit triggers loop() to exit, which in turn will trigger
	// the removal of our tokens etc
	close(i.quit)
	i.done.Wait()
}

func (i *Lifecycler) loop() {
	defer func() {
		level.Info(util.Logger).Log("msg", "member.loop() exited gracefully")
		i.done.Done()
	}()

	// First, see if we exist in the cluster, update our state to match if we do,
	// and add ourselves (without tokens) if we don't.
	if err := i.initRing(context.Background()); err != nil {
		level.Error(util.Logger).Log("msg", "failed to join consul", "err", err)
		os.Exit(1)
	}

	// We do various period tasks
	autoJoinTimer := time.NewTimer(i.cfg.JoinAfter)
	autoJoinAfter := autoJoinTimer.C
	var observeChan <-chan time.Time = nil

	heartbeatTicker := time.NewTicker(i.cfg.HeartbeatPeriod)
	defer heartbeatTicker.Stop()

	if i.cfg.JoinIncrementalTransfer {
		autoJoinTimer.Stop()

		level.Info(util.Logger).Log("msg", "joining cluster")
		if err := i.waitCleanRing(context.Background()); err != nil {
			// If this fails, we'll get spill over of data, but we can
			// safely continue here.
			level.Error(util.Logger).Log("msg", "failed to wait for a clean ring to join", "err", err)
		}

		if err := i.autoJoin(context.Background(), PENDING); err != nil {
			level.Error(util.Logger).Log("msg", "failed to pick tokens in consul", "err", err)
			os.Exit(1)
		}

		transferStart := time.Now()
		if err := i.joinIncrementalTransfer(context.Background()); err != nil {
			transferDuration.WithLabelValues("join", "fail", i.RingName).Observe(time.Since(transferStart).Seconds())

			level.Error(util.Logger).Log("msg", "failed to obtain chunks on join", "err", err)
		} else {
			transferDuration.WithLabelValues("join", "success", i.RingName).Observe(time.Since(transferStart).Seconds())
		}

		close(i.joined)
	}

loop:
	for {
		select {
		case <-autoJoinAfter:
			level.Debug(util.Logger).Log("msg", "JoinAfter expired")
			// Will only fire once, after auto join timeout.  If we haven't entered "JOINING" state,
			// then pick some tokens and enter ACTIVE state.
			if i.GetState() == PENDING {
				level.Info(util.Logger).Log("msg", "auto-joining cluster after timeout")

				if i.cfg.ObservePeriod > 0 {
					// let's observe the ring. By using JOINING state, this ingester will be ignored by LEAVING
					// ingesters, but we also signal that it is not fully functional yet.
					if err := i.autoJoin(context.Background(), JOINING); err != nil {
						level.Error(util.Logger).Log("msg", "failed to pick tokens in consul", "err", err)
						os.Exit(1)
					}

					level.Info(util.Logger).Log("msg", "observing tokens before going ACTIVE")
					observeChan = time.After(i.cfg.ObservePeriod)
				} else {
					if err := i.autoJoin(context.Background(), ACTIVE); err != nil {
						level.Error(util.Logger).Log("msg", "failed to pick tokens in consul", "err", err)
						os.Exit(1)
					}
				}
			}

		case <-observeChan:
			// if observeChan is nil, this case is ignored. We keep updating observeChan while observing the ring.
			// When observing is done, observeChan is set to nil.

			observeChan = nil
			if s := i.GetState(); s != JOINING {
				level.Error(util.Logger).Log("msg", "unexpected state while observing tokens", "state", s)
			}

			if i.verifyTokens(context.Background()) {
				level.Info(util.Logger).Log("msg", "token verification successful")

				err := i.changeState(context.Background(), ACTIVE)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to set state to ACTIVE", "err", err)
					continue
				}

				close(i.joined)
			} else {
				level.Info(util.Logger).Log("msg", "token verification failed, observing")
				// keep observing
				observeChan = time.After(i.cfg.ObservePeriod)
			}
		case <-heartbeatTicker.C:
			consulHeartbeats.WithLabelValues(i.RingName).Inc()
			if err := i.updateConsul(context.Background()); err != nil {
				level.Error(util.Logger).Log("msg", "failed to write to consul, sleeping", "err", err)
			}

		case f := <-i.actorChan:
			f()

		case <-i.quit:
			break loop
		}
	}

	// Mark ourselved as Leaving so no more samples are send to us.
	i.changeState(context.Background(), LEAVING)

	// Do the transferring / flushing on a background goroutine so we can continue
	// to heartbeat to consul.
	done := make(chan struct{})
	go func() {
		i.processShutdown(context.Background())
		close(done)
	}()

heartbeatLoop:
	for {
		select {
		case <-heartbeatTicker.C:
			consulHeartbeats.WithLabelValues(i.RingName).Inc()
			if err := i.updateConsul(context.Background()); err != nil {
				level.Error(util.Logger).Log("msg", "failed to write to consul, sleeping", "err", err)
			}

		case <-done:
			break heartbeatLoop
		}
	}

	if !i.cfg.SkipUnregister {
		if err := i.unregister(context.Background()); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to unregister from consul", "err", err)
			os.Exit(1)
		}
		level.Info(util.Logger).Log("msg", "ingester removed from consul")
	}

	i.KVStore.Stop()
}

// waitCleanRing incrementally reads from the KV store and waits
// until there are no JOINING or LEAVING ingesters.
func (i *Lifecycler) waitCleanRing(ctx context.Context) error {
	// Sleep for a random period up to 2s. Used to stagger
	// multiple nodes all waiting for the ring to be clean.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	sleepMs := r.Int31n(2000)
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)

	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
	})

	for backoff.Ongoing() {
		select {
		case <-i.quit:
			return errors.New("shutting down")
		default:
		}

		ok, err := i.checkCleanRing(ctx)
		if err != nil {
			return err
		} else if ok {
			return nil
		}

		backoff.Wait()
	}

	return backoff.Err()
}

// checkCleanRing returns true when the ring has no JOINING
// or LEAVING ingesters. "clean" implies that it is safe for a
// new node to join.
func (i *Lifecycler) checkCleanRing(ctx context.Context) (bool, error) {
	d, err := i.KVStore.Get(ctx, ConsulKey)
	if err != nil {
		return false, err
	} else if d == nil {
		return false, nil
	}

	desc, ok := d.(*Desc)
	if !ok {
		return false, fmt.Errorf("could not convert ring to Desc")
	}

	unclean := 0
	for k, ing := range desc.Ingesters {
		if k == i.ID {
			continue
		}
		if ing.State == JOINING || ing.State == LEAVING {
			unclean++
		}
	}

	return unclean == 0, nil
}

// initRing is the first thing we do when we start. It:
// - add an ingester entry to the ring
// - copies out our state and tokens if they exist
func (i *Lifecycler) initRing(ctx context.Context) error {
	var ringDesc *Desc

	err := i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[i.ID]
		if !ok {
			// Either we are a new ingester, or consul must have restarted
			level.Info(util.Logger).Log("msg", "entry not found in ring, adding with no tokens")
			ringDesc.AddIngester(i.ID, i.Addr, nil, i.GetState(), i.cfg.NormaliseTokens)
			return ringDesc, true, nil
		}

		// We exist in the ring, so assume the ring is right and copy out tokens & state out of there.
		i.setState(ingesterDesc.State)
		tokens, _ := ringDesc.TokensFor(i.ID)
		i.setTokens(tokens)

		level.Info(util.Logger).Log("msg", "existing entry found in ring", "state", i.GetState(), "tokens", len(tokens))
		// we haven't modified the ring, don't try to store it.
		return nil, true, nil
	})

	// Update counters
	if err == nil {
		i.updateCounters(ringDesc)
	}

	return err
}

// Verifies that tokens that this ingester has registered to the ring still belong to it.
// Gossiping ring may change the ownership of tokens in case of conflicts.
// If ingester doesn't own its tokens anymore, this method generates new tokens and puts them to the ring.
func (i *Lifecycler) verifyTokens(ctx context.Context) bool {
	result := false

	err := i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		// At this point, we should have the same tokens as we have registered before
		ringTokens, takenTokens := ringDesc.TokensFor(i.ID)

		if !i.compareTokens(ringTokens) {
			// uh, oh... our tokens are not our anymore. Let's try new ones.
			needTokens := i.cfg.NumTokens - len(ringTokens)

			level.Info(util.Logger).Log("msg", "generating new tokens", "count", needTokens)
			newTokens := GenerateTokens(needTokens, takenTokens, i.GetState())

			ringTokens = append(ringTokens, newTokens...)
			sort.Sort(ByStatefulTokens(ringTokens))

			ringDesc.AddIngester(i.ID, i.Addr, ringTokens, i.GetState(), i.cfg.NormaliseTokens)

			i.setTokens(ringTokens)

			return ringDesc, true, nil
		}

		// all is good, this ingester owns its tokens
		result = true
		return nil, true, nil
	})

	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to verify tokens", "err", err)
		return false
	}

	return result
}

func (i *Lifecycler) compareTokens(fromRing []StatefulToken) bool {
	sort.Sort(ByStatefulTokens(fromRing))

	tokens := i.getTokens()
	sort.Sort(ByStatefulTokens(tokens))

	if len(tokens) != len(fromRing) {
		return false
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i] != fromRing[i] {
			return false
		}
	}
	return true
}

// autoJoin selects random tokens & moves state to targetState
func (i *Lifecycler) autoJoin(ctx context.Context, targetState State) error {
	var ringDesc *Desc

	err := i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		// At this point, we should not have any tokens, and we should be in PENDING state.
		myTokens, takenTokens := ringDesc.TokensFor(i.ID)
		if len(myTokens) > 0 {
			level.Error(util.Logger).Log("msg", "tokens already exist for this ingester - wasn't expecting any!", "num_tokens", len(myTokens))
		}

		newTokens := i.generateTokens(i.cfg.NumTokens-len(myTokens), takenTokens, targetState)
		i.setState(targetState)

		// When we're incrementally joining the ring, tokens are only inserted
		// incrementally during the join process.
		insertTokens := newTokens
		if i.cfg.JoinIncrementalTransfer {
			insertTokens = nil
		}

		ringDesc.AddIngester(i.ID, i.Addr, insertTokens, i.GetState(), i.cfg.NormaliseTokens)

		tokens := append(myTokens, newTokens...)
		sort.Sort(ByStatefulTokens(tokens))

		if i.cfg.JoinIncrementalTransfer {
			i.setTransitioningTokens(tokens)
		} else {
			i.setTokens(tokens)
		}

		return ringDesc, true, nil
	})

	// Update counters
	if err == nil {
		i.updateCounters(ringDesc)
	}

	return err
}

// updateConsul updates our entries in consul, heartbeating and dealing with
// consul restarts.
func (i *Lifecycler) updateConsul(ctx context.Context) error {
	var ringDesc *Desc

	err := i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[i.ID]
		if !ok {
			// consul must have restarted
			level.Info(util.Logger).Log("msg", "found empty ring, inserting tokens")
			ringDesc.AddIngester(i.ID, i.Addr, i.getTokens(), i.GetState(), i.cfg.NormaliseTokens)
		} else {
			ingesterDesc.Timestamp = time.Now().Unix()
			ingesterDesc.State = i.GetState()
			ingesterDesc.Addr = i.Addr
			ringDesc.Ingesters[i.ID] = ingesterDesc
		}

		// Re-sync token states for the current lifecycler if they've changed.
		ringDesc.RefreshTokenState(i.ID, i.tokens, i.cfg.NormaliseTokens)
		return ringDesc, true, nil
	})

	// Update counters
	if err == nil {
		i.updateCounters(ringDesc)
	}

	return err
}

// changeState updates consul with state transitions for us.  NB this must be
// called from loop()!  Use ChangeState for calls from outside of loop().
func (i *Lifecycler) changeState(ctx context.Context, state State) error {
	currState := i.GetState()
	// Only the following state transitions can be triggered externally
	if !((currState == PENDING && state == JOINING) || // triggered by TransferChunks at the beginning
		(currState == JOINING && state == PENDING) || // triggered by TransferChunks on failure
		(currState == JOINING && state == ACTIVE) || // triggered by TransferChunks on success
		(currState == PENDING && state == ACTIVE) || // triggered by autoJoin
		(currState == ACTIVE && state == LEAVING)) { // triggered by shutdown
		return fmt.Errorf("Changing ingester state from %v -> %v is disallowed", currState, state)
	}

	level.Info(util.Logger).Log("msg", "changing ingester state from", "old_state", currState, "new_state", state)
	i.setState(state)

	// If we're joining the ring and we don't have the incremental join enabled,
	// we'll set all of our tokens to the same state we just changed into.
	//
	// Otherwise, if we're leaving the ring and don't have incremental leave
	// enabled, we'll do the same.
	if !i.cfg.JoinIncrementalTransfer && state != LEAVING {
		i.setTokensState(state)
	} else if !i.cfg.LeaveIncrementalTransfer && state == LEAVING {
		i.setTokensState(state)
	}

	return i.updateConsul(ctx)
}

func (i *Lifecycler) updateCounters(ringDesc *Desc) {
	// Count the number of healthy instances for Write operation
	healthyInstancesCount := 0

	if ringDesc != nil {
		for _, ingester := range ringDesc.Ingesters {
			if ingester.IsHealthy(Write, i.cfg.RingConfig.HeartbeatTimeout) {
				healthyInstancesCount++
			}
		}
	}

	// Update counters
	i.countersLock.Lock()
	i.healthyInstancesCount = healthyInstancesCount
	i.countersLock.Unlock()
}

func (i *Lifecycler) processShutdown(ctx context.Context) {
	flushRequired := true
	transferStart := time.Now()

	if i.cfg.LeaveIncrementalTransfer {
		if err := i.leaveIncrementalTransfer(ctx); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to incrementally transfer chunks to another ingester", "err", err)
			shutdownDuration.WithLabelValues("incremental_transfer", "fail", i.RingName).Observe(time.Since(transferStart).Seconds())
			transferDuration.WithLabelValues("leave", "fail", i.RingName).Observe(time.Since(transferStart).Seconds())
		} else {
			// If the ingester incorrectly receieved writes for streams
			// not in any of its expected token ranges, we may still
			// have data remaining that wasn't transferred out. This
			// data should be flushed to disk so it's not lost.
			shutdownDuration.WithLabelValues("incremental_transfer", "success", i.RingName).Observe(time.Since(transferStart).Seconds())
			transferDuration.WithLabelValues("leave", "success", i.RingName).Observe(time.Since(transferStart).Seconds())
		}
	} else {
		if err := i.flushTransferer.TransferOut(ctx); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to transfer chunks to another ingester", "err", err)
			shutdownDuration.WithLabelValues("transfer", "fail", i.RingName).Observe(time.Since(transferStart).Seconds())
		} else {
			flushRequired = false
			shutdownDuration.WithLabelValues("transfer", "success", i.RingName).Observe(time.Since(transferStart).Seconds())
		}
	}

	if flushRequired {
		flushStart := time.Now()
		i.flushTransferer.Flush()
		shutdownDuration.WithLabelValues("flush", "success", i.RingName).Observe(time.Since(flushStart).Seconds())
	}

	// Sleep so the shutdownDuration metric can be collected.
	time.Sleep(i.cfg.FinalSleep)
}

// getDenormalisedRing is a helper method to grab the ring, denormalise the
// tokens and sort them.
func (i *Lifecycler) getDenormalisedRing(ctx context.Context) (*Desc, error) {
	d, err := i.KVStore.Get(ctx, ConsulKey)
	if err != nil {
		return nil, err
	}
	desc, ok := d.(*Desc)
	if !ok {
		return nil, fmt.Errorf("could not convert ring to Desc")
	}
	desc.Tokens = migrateRing(desc)
	return desc, nil
}

type transferWorkload map[string][]TokenRange

// findTransferWorkload will find all affected ranges by token that require
// a transfer in or out. If findTransferWorkload returns an error, there
// may still be sets found in the workload that should be processed.
func (i *Lifecycler) findTransferWorkload(d *Desc, token StatefulToken) (transferWorkload, bool) {
	ret := make(transferWorkload)
	rf := i.cfg.RingConfig.ReplicationFactor

	op := Read
	if token.State == LEAVING {
		op = Write
	}

	var lastErr error

	// Collect all the ranges that we will transfer data for
	// and the remote ingester to send or receieve the data.
	//
	// When joining, the target ingester is the old end of the
	// replica set (i.e., the last in the set) and when leaving,
	// the target is the new end of the replica set.
	for replica := 0; replica < rf; replica++ {
		// The original token ranges we might want to request data
		// for is defined by [token-(replica+1), token-replica). If ingesters
		// have multiple tokens, there may be up to #tokens*replica number of
		// combinations here for which our token is the replica'th successor.
		endRanges, err := d.Predecessors(NeighborOptions{
			Start:        token,
			Neighbor:     replica,
			Op:           op,
			MaxHeartbeat: i.cfg.RingConfig.HeartbeatTimeout,
		})
		if err != nil {
			lastErr = err
			continue
		}

		for _, endRange := range endRanges {
			startRange, err := d.Successor(NeighborOptions{
				Start:        endRange.StatefulToken(),
				Neighbor:     -1,
				Op:           op,
				MaxHeartbeat: i.cfg.RingConfig.HeartbeatTimeout,
			})
			if err != nil {
				lastErr = err
				continue
			}

			target, err := d.Successor(NeighborOptions{
				Start:        token,
				Neighbor:     rf - replica,
				Op:           op,
				MaxHeartbeat: i.cfg.RingConfig.HeartbeatTimeout,
			})
			if err != nil {
				lastErr = err
				continue
			}

			// No transfer necessary if we're in the replica set. If we're
			// joining, it means we already have the data. If we're leaving,
			// it means we should still have the data.
			//
			// We want to check the replica set _ignoring_ our current token,
			// which we do by checking two ranges: [endRange, token) and
			// (token, target].

			lhsRange := RangeOptions{
				Range: TokenRange{From: endRange.Token, To: token.Token},

				ID:            i.ID,
				Op:            op,
				LeftInclusive: true,
				MaxHeartbeat:  i.cfg.RingConfig.HeartbeatTimeout,
			}
			rhsRange := RangeOptions{
				Range: TokenRange{From: token.Token, To: target.Token},

				ID:             i.ID,
				Op:             op,
				RightInclusive: true,
				MaxHeartbeat:   i.cfg.RingConfig.HeartbeatTimeout,
			}

			if d.InRange(lhsRange) || d.InRange(rhsRange) {
				continue
			}

			addr := d.Ingesters[target.Ingester].Addr
			ret[addr] = append(ret[addr], TokenRange{
				From: startRange.Token,
				To:   endRange.Token,
			})
		}
	}

	if lastErr != nil {
		level.Error(util.Logger).Log(
			"msg", fmt.Sprintf("failed to find complete transfer set for %d", token.Token),
			"err", lastErr,
		)
	}
	return ret, lastErr == nil
}

// joinIncrementalTransfer will attempt to incrementally obtain chunks from
// neighboring lifecyclers that contain data for token ranges they will
// no longer receive writes for.
func (i *Lifecycler) joinIncrementalTransfer(ctx context.Context) error {
	// Make sure that we set all tokens to ACTIVE, even
	// when we fail.
	defer func() {
		i.setTokens(i.getTransitioningTokens())
		i.setTokensState(ACTIVE)
		i.changeState(ctx, ACTIVE)
		i.updateConsul(ctx)
	}()

	r, err := i.getDenormalisedRing(ctx)
	if err != nil {
		return fmt.Errorf("failed to read ring: %v", err)
	}

	replicationFactor := i.cfg.RingConfig.ReplicationFactor
	if active := r.FindIngestersByState(ACTIVE); len(active) < replicationFactor {
		if len(active) == 0 {
			return fmt.Errorf("no ingesters to request data from")
		}

		// Request an entire copy of all chunks from the first replica.
		fullRange := []TokenRange{{0, math.MaxUint32}}
		err := i.incTransferer.RequestChunkRanges(ctx, fullRange, active[0].Addr, false)
		if err != nil {
			level.Error(util.Logger).Log("msg",
				fmt.Sprintf("failed to request copy of data from %s", active[0].Addr))
		}

		return nil
	}

	pendingUnblocks := []struct {
		addr string
		rgs  []TokenRange
	}{}

	var wg sync.WaitGroup

	for tokidx, token := range i.transitioningTokens {
		// For findTransferWorkload to find the full list of tokens to transfer,
		// we must first set the token to JOINING locally and add it into our
		// copy of the ring.
		i.stateMtx.Lock()
		token.State = JOINING
		i.transitioningTokens[tokidx] = token
		i.stateMtx.Unlock()

		r.AddToken(i.ID, token, false)

		workload, _ := i.findTransferWorkload(r, token)

		for addr, ranges := range workload {
			wg.Add(1)
			go func(addr string, ranges []TokenRange) {
				err := i.incTransferer.BlockRanges(ctx, ranges, addr)
				if err == nil {
					err = i.incTransferer.RequestChunkRanges(ctx, ranges, addr, true)
				}

				if err != nil {
					level.Error(util.Logger).Log(
						"msg", "failed to request chunks",
						"target_addr", addr,
						"ranges", PrintableRanges(ranges),
						"err", err,
					)
				}

				wg.Done()
			}(addr, ranges)
		}
		wg.Wait()

		// Add the token into the ring.
		token.State = ACTIVE
		i.addToken(token)
		r.RefreshTokenState(i.ID, i.getTokens(), false)

		if i.cfg.UpdateRingDuringTransfer {
			err = i.updateConsul(ctx)
			if err != nil {
				level.Error(util.Logger).Log("msg",
					fmt.Sprintf("failed to update consul when changing token %d to ACTIVE", token))
			}
		}

		for addr, ranges := range workload {
			pendingUnblocks = append(pendingUnblocks, struct {
				addr string
				rgs  []TokenRange
			}{addr, ranges})
		}
	}

	go func() {
		if i.cfg.RangeUnblockDelay != time.Duration(0) {
			<-time.After(i.cfg.RangeUnblockDelay)
		}

		ctx := context.Background()

		for _, block := range pendingUnblocks {
			err := i.incTransferer.UnblockRanges(ctx, block.rgs, block.addr)
			if err != nil {
				level.Error(util.Logger).Log(
					"msg", "failed to unblock transferred ranges",
					"target_addr", block.addr,
					"ranges", PrintableRanges(block.rgs),
					"err", err,
				)
			}
		}
	}()

	return nil
}

// leaveIncrementalTransfer will attempt to incrementally send chunks to
// neighboring lifecyclers that should contain data for token ranges the
// leaving lifecycler will no longer receive writes for.
func (i *Lifecycler) leaveIncrementalTransfer(ctx context.Context) error {
	// Make sure all tokens are set to leaving, even when we
	// fail.
	defer func() {
		i.setTokens(nil)
		i.setState(LEAVING)
		i.updateConsul(ctx)

		remainingTokens := i.incTransferer.StreamTokens()
		if len(remainingTokens) > 0 {
			level.Warn(util.Logger).Log(
				"msg", "not all tokens transferred out",
				"streams_remaining", len(remainingTokens),
			)

			printTokens := remainingTokens
			if len(printTokens) > 20 {
				printTokens = printTokens[:20]
			}

			level.Debug(util.Logger).Log(
				"msg", "non-transferred tokens",
				"tokens", printTokens,
			)
		}
	}()

	r, err := i.getDenormalisedRing(ctx)
	if err != nil {
		return fmt.Errorf("failed to read ring: %v", err)
	}

	replicationFactor := i.cfg.RingConfig.ReplicationFactor
	if active := r.FindIngestersByState(ACTIVE); len(active) <= replicationFactor {
		return fmt.Errorf("not transferring out; number of ingesters less than or equal to replication factor")
	}

	success := true

	i.setTransitioningTokens(i.getTokens())
	tokens := i.getTransitioningTokens()

	var wg sync.WaitGroup

	for tokidx, token := range tokens {
		// For findTransferWorkload to find the full list of tokens to transfer,
		// we first set the token to LEAVING locally and refresh it in our
		// copy of the ring.
		i.stateMtx.Lock()
		token.State = LEAVING
		tokens[tokidx] = token
		i.stateMtx.Unlock()

		r.RefreshTokenState(i.ID, tokens, false)

		workload, ok := i.findTransferWorkload(r, token)
		if !ok {
			success = false
		}

		for addr, ranges := range workload {
			wg.Add(1)
			go func(addr string, ranges []TokenRange) {
				err := i.incTransferer.BlockRanges(ctx, ranges, addr)
				if err == nil {
					// Also block the range locally. Will not be unblocked.
					err = i.incTransferer.BlockRanges(ctx, ranges, "")
				}

				if err != nil {
					level.Error(util.Logger).Log(
						"msg", "failed to send chunks",
						"target_addr", addr,
						"ranges", PrintableRanges(ranges),
						"err", err,
					)
					success = false
				}

				wg.Done()
			}(addr, ranges)
		}
		wg.Wait()

		i.addToken(token)

		if i.cfg.UpdateRingDuringTransfer {
			// Notify Consul of the token change.
			err = i.updateConsul(ctx)
			if err != nil {
				level.Error(util.Logger).Log("msg",
					fmt.Sprintf("failed to update consul when changing token %d to LEAVING", token),
					"err", err)
				success = false
			}
		}

		for addr, ranges := range workload {
			wg.Add(1)
			go func(addr string, ranges []TokenRange) {
				err := i.incTransferer.SendChunkRanges(ctx, ranges, addr)
				if err != nil {
					level.Error(util.Logger).Log(
						"msg", "failed to send chunks",
						"target_addr", addr,
						"ranges", PrintableRanges(ranges),
						"err", err,
					)
					success = false
				}

				err = i.incTransferer.UnblockRanges(ctx, ranges, addr)
				if err != nil {
					level.Error(util.Logger).Log(
						"msg", "failed to unblock ranges",
						"target_addr", addr,
						"ranges", PrintableRanges(ranges),
						"err", err,
					)
					success = false
				}

				wg.Done()
			}(addr, ranges)
		}
		wg.Wait()

		// Finally, remove the token from the ring.
		i.removeToken(token)
		err = i.updateConsul(ctx)
		if err != nil {
			level.Error(util.Logger).Log("msg",
				fmt.Sprintf("failed to update consul when removing token %d", token))
			success = false
		}
	}

	if !success {
		return fmt.Errorf("incremental transfer out incomplete")
	}
	return nil
}

// unregister removes our entry from consul.
func (i *Lifecycler) unregister(ctx context.Context) error {
	level.Debug(util.Logger).Log("msg", "unregistering member from ring")

	return i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.RemoveIngester(i.ID)
		return ringDesc, true, nil
	})
}
