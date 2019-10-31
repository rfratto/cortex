package ring

// Based on https://raw.githubusercontent.com/stathat/consistent/master/consistent.go

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	unhealthy = "Unhealthy"

	// ConsulKey is the key under which we store the ring in consul.
	ConsulKey = "ring"
)

// ReadRing represents the read inferface to the ring.
type ReadRing interface {
	prometheus.Collector

	// Get returns n (or more) ingesters which form the replicas for the given
	// key. ingesters and tokens are buffers to be overwritten so that generating
	// the resulting ReplicationSet can avoid memory allocations; can be nil.
	Get(key uint32, op Operation, ingesters []IngesterDesc, tokens []TokenDesc) (ReplicationSet, error)
	GetAll() (ReplicationSet, error)
	ReplicationFactor() int
	IngesterCount() int
}

// Operation can be Read or Write
type Operation int

// Values for Operation
const (
	Read Operation = iota
	Write
	Reporting // Special value for inquiring about health
)

type uint32s []uint32

func (x uint32s) Len() int           { return len(x) }
func (x uint32s) Less(i, j int) bool { return x[i] < x[j] }
func (x uint32s) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// ErrEmptyRing is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyRing = errors.New("empty ring")

// Config for a Ring
type Config struct {
	KVStore           kv.Config     `yaml:"kvstore,omitempty"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout,omitempty"`
	ReplicationFactor int           `yaml:"replication_factor,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.KVStore.RegisterFlagsWithPrefix(prefix, f)

	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes.")
	f.IntVar(&cfg.ReplicationFactor, prefix+"distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
}

// Ring holds the information about the members of the consistent hash ring.
type Ring struct {
	name     string
	cfg      Config
	KVClient kv.Client
	done     chan struct{}
	quit     context.CancelFunc

	mtx      sync.RWMutex
	ringDesc *Desc

	memberOwnershipDesc *prometheus.Desc
	numMembersDesc      *prometheus.Desc
	totalTokensDesc     *prometheus.Desc
	numTokensDesc       *prometheus.Desc
	oldestTimestampDesc *prometheus.Desc
}

// New creates a new Ring
func New(cfg Config, name string) (*Ring, error) {
	if cfg.ReplicationFactor <= 0 {
		return nil, fmt.Errorf("ReplicationFactor must be greater than zero: %d", cfg.ReplicationFactor)
	}
	codec := codec.Proto{Factory: ProtoDescFactory}
	store, err := kv.NewClient(cfg.KVStore, codec)
	if err != nil {
		return nil, err
	}

	r := &Ring{
		name:     name,
		cfg:      cfg,
		KVClient: store,
		done:     make(chan struct{}),
		ringDesc: &Desc{},
		memberOwnershipDesc: prometheus.NewDesc(
			"cortex_ring_member_ownership_percent",
			"The percent ownership of the ring by member",
			[]string{"member", "name"}, nil,
		),
		numMembersDesc: prometheus.NewDesc(
			"cortex_ring_members",
			"Number of members in the ring",
			[]string{"state", "name"}, nil,
		),
		totalTokensDesc: prometheus.NewDesc(
			"cortex_ring_tokens_total",
			"Number of tokens in the ring",
			[]string{"name"}, nil,
		),
		numTokensDesc: prometheus.NewDesc(
			"cortex_ring_tokens_owned",
			"The number of tokens in the ring owned by the member",
			[]string{"member", "name"}, nil,
		),
		oldestTimestampDesc: prometheus.NewDesc(
			"cortex_ring_oldest_member_timestamp",
			"Timestamp of the oldest member in the ring.",
			[]string{"state", "name"}, nil,
		),
	}
	var ctx context.Context
	ctx, r.quit = context.WithCancel(context.Background())
	go r.loop(ctx)
	return r, nil
}

// Stop the distributor.
func (r *Ring) Stop() {
	r.quit()
	<-r.done
}

func (r *Ring) loop(ctx context.Context) {
	defer close(r.done)
	r.KVClient.WatchKey(ctx, ConsulKey, func(value interface{}) bool {
		if value == nil {
			level.Info(util.Logger).Log("msg", "ring doesn't exist in consul yet")
			return true
		}

		ringDesc := value.(*Desc)
		ringDesc.Tokens = migrateRing(ringDesc)
		r.mtx.Lock()
		defer r.mtx.Unlock()
		r.ringDesc = ringDesc
		return true
	})

	r.KVClient.Stop()
}

// migrateRing will denormalise the ring's tokens if stored in normal form.
func migrateRing(desc *Desc) []TokenDesc {
	numTokens := len(desc.Tokens)
	for _, ing := range desc.Ingesters {
		numTokens += len(ing.Tokens)
	}
	tokens := make([]TokenDesc, len(desc.Tokens), numTokens)
	copy(tokens, desc.Tokens)
	for key, ing := range desc.Ingesters {
		for _, token := range ing.Tokens {
			td := TokenDesc{
				Token:    token,
				Ingester: key,
				State:    ACTIVE,
			}

			if s, ok := ing.InactiveTokens[token]; ok {
				td.State = s
			}

			tokens = append(tokens, td)
		}
	}

	// Backwards compatibility: if the ingester doesn't use
	// stateful tokens, force token state to ingester state.
	for i, tok := range tokens {
		ing := desc.Ingesters[tok.Ingester]
		if !ing.StatefulTokens {
			tokens[i].State = ing.State
		}
	}

	sort.Sort(ByToken(tokens))
	return tokens
}

type indexedToken struct {
	TokenDesc
	idx int
}

// Get returns n (or more) ingesters which form the replicas for the given key.
func (r *Ring) Get(key uint32, op Operation, ingesters []IngesterDesc,
	tokens []TokenDesc) (ReplicationSet, error) {

	r.mtx.RLock()
	defer r.mtx.RUnlock()
	if r.ringDesc == nil || len(r.ringDesc.Tokens) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	// Reset buffers
	ingesters = ingesters[:0]
	tokens = tokens[:0]

	var (
		n             = r.cfg.ReplicationFactor
		distinctHosts = map[string]indexedToken{}
		start         = r.search(key)
		iterations    = 0
	)
	for i := start; len(distinctHosts) < n && iterations < len(r.ringDesc.Tokens); i++ {
		iterations++
		// Wrap i around in the ring.
		i %= len(r.ringDesc.Tokens)

		token := r.ringDesc.Tokens[i]

		if itok, ok := distinctHosts[token.Ingester]; ok {
			// We want to collect a set of tokens from n distinct ingesters, but,
			// a subset of tokens in an ingester may be in an invalid state while
			// another subset is valid. If the previous token we found was invalid
			// and the current token is valid, we want to do the following:
			//
			// - Decrease the replica set size by 1 (offseting for the previous
			//   invalid token).
			// - Replace the invalid token with the valid one.
			if !validateTokenState(itok.TokenDesc, op) && validateTokenState(token, op) {
				n--
				tokens[itok.idx] = token
				distinctHosts[token.Ingester] = indexedToken{token, itok.idx}
				continue
			}

			// Otherwise, we want to continue on to the next token.
			continue
		}

		// We do not want to operate on ingesters that are not in an appropriate
		// state, but we still do want to operate on a replica somewhere. To
		// handle this, we increase the size of the set of replicas for the key.
		// NB dead ingesters will be filtered later (by replication_strategy.go).
		if !validateTokenState(token, op) {
			n++
		}

		tokens = append(tokens, token)
		distinctHosts[token.Ingester] = indexedToken{token, len(tokens) - 1}
	}

	liveTokens, maxFailure, err := r.replicationStrategy(tokens, op)
	if err != nil {
		return ReplicationSet{}, err
	}

	liveIngesters := ingesters[:0]
	for _, tok := range liveTokens {
		ing := r.ringDesc.Ingesters[tok.Ingester]
		liveIngesters = append(liveIngesters, ing)
	}

	return ReplicationSet{
		Ingesters: liveIngesters,
		MaxErrors: maxFailure,
	}, nil
}

// validateTokenState ensures that a given state is appropriate for
// specific operation. Writes should not happen on states that are
// not ACTIVE, while reads should go to anything that is not ACTIVE
// or LEAVING.
func validateTokenState(tok TokenDesc, op Operation) bool {
	if op == Write && tok.State != ACTIVE {
		return false
	} else if op == Read && (tok.State != ACTIVE && tok.State != LEAVING) {
		return false
	}

	return true
}

// GetAll returns all available ingesters in the ring.
func (r *Ring) GetAll() (ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.ringDesc == nil || len(r.ringDesc.Tokens) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	ingesters := make([]IngesterDesc, 0, len(r.ringDesc.Ingesters))
	maxErrors := r.cfg.ReplicationFactor / 2

	for _, ingester := range r.ringDesc.Ingesters {
		if !IsHealthy(&ingester, Read, r.cfg.HeartbeatTimeout) {
			maxErrors--
			continue
		}
		ingesters = append(ingesters, ingester)
	}

	if maxErrors < 0 {
		return ReplicationSet{}, fmt.Errorf("too many failed ingesters")
	}

	return ReplicationSet{
		Ingesters: ingesters,
		MaxErrors: maxErrors,
	}, nil
}

func (r *Ring) search(key uint32) int {
	i := sort.Search(len(r.ringDesc.Tokens), func(x int) bool {
		return r.ringDesc.Tokens[x].Token > key
	})
	if i >= len(r.ringDesc.Tokens) {
		i = 0
	}
	return i
}

// Describe implements prometheus.Collector.
func (r *Ring) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.memberOwnershipDesc
	ch <- r.numMembersDesc
	ch <- r.totalTokensDesc
	ch <- r.numTokensDesc
}

func countTokens(ringDesc *Desc) (map[string]uint32, map[string]uint32) {
	tokens := ringDesc.Tokens

	owned := map[string]uint32{}
	numTokens := map[string]uint32{}
	for i, token := range tokens {
		var diff uint32
		if i+1 == len(tokens) {
			diff = (math.MaxUint32 - token.Token) + tokens[0].Token
		} else {
			diff = tokens[i+1].Token - token.Token
		}
		numTokens[token.Ingester] = numTokens[token.Ingester] + 1
		owned[token.Ingester] = owned[token.Ingester] + diff
	}

	for id := range ringDesc.Ingesters {
		if _, ok := owned[id]; !ok {
			owned[id] = 0
			numTokens[id] = 0
		}
	}

	return numTokens, owned
}

// Collect implements prometheus.Collector.
func (r *Ring) Collect(ch chan<- prometheus.Metric) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	numTokens, ownedRange := countTokens(r.ringDesc)
	for id, totalOwned := range ownedRange {
		ch <- prometheus.MustNewConstMetric(
			r.memberOwnershipDesc,
			prometheus.GaugeValue,
			float64(totalOwned)/float64(math.MaxUint32),
			id,
			r.name,
		)
		ch <- prometheus.MustNewConstMetric(
			r.numTokensDesc,
			prometheus.GaugeValue,
			float64(numTokens[id]),
			id,
			r.name,
		)
	}

	numByState := map[string]int{}
	oldestTimestampByState := map[string]int64{}

	// Initialised to zero so we emit zero-metrics (instead of not emitting anything)
	for _, s := range []string{unhealthy, ACTIVE.String(), LEAVING.String(), PENDING.String(), JOINING.String()} {
		numByState[s] = 0
		oldestTimestampByState[s] = 0
	}

	for _, ingester := range r.ringDesc.Ingesters {
		s := ingester.State.String()
		if !IsHealthy(&ingester, Reporting, r.cfg.HeartbeatTimeout) {
			s = unhealthy
		}
		numByState[s]++
		if oldestTimestampByState[s] == 0 || ingester.Timestamp < oldestTimestampByState[s] {
			oldestTimestampByState[s] = ingester.Timestamp
		}
	}

	for state, count := range numByState {
		ch <- prometheus.MustNewConstMetric(
			r.numMembersDesc,
			prometheus.GaugeValue,
			float64(count),
			state,
			r.name,
		)
	}
	for state, timestamp := range oldestTimestampByState {
		ch <- prometheus.MustNewConstMetric(
			r.oldestTimestampDesc,
			prometheus.GaugeValue,
			float64(timestamp),
			state,
			r.name,
		)
	}

	ch <- prometheus.MustNewConstMetric(
		r.totalTokensDesc,
		prometheus.GaugeValue,
		float64(len(r.ringDesc.Tokens)),
		r.name,
	)
}
