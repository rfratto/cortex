package ring

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
)

// StatefulToken is a token with a state.
type StatefulToken struct {
	Token uint32
	State State
}

// ByStatefulTokens is a sortable list of StatefulToken
type ByStatefulTokens []StatefulToken

func (ts ByStatefulTokens) Len() int           { return len(ts) }
func (ts ByStatefulTokens) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts ByStatefulTokens) Less(i, j int) bool { return ts[i].Token < ts[j].Token }

// ByToken is a sortable list of TokenDescs
type ByToken []TokenDesc

func (ts ByToken) Len() int           { return len(ts) }
func (ts ByToken) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts ByToken) Less(i, j int) bool { return ts[i].Token < ts[j].Token }

// ProtoDescFactory makes new Descs
func ProtoDescFactory() proto.Message {
	return NewDesc()
}

// GetCodec returns the codec used to encode and decode data being put by ring.
func GetCodec() codec.Codec {
	return codec.Proto{Factory: ProtoDescFactory}
}

// NewDesc returns an empty ring.Desc
func NewDesc() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{},
	}
}

// AddToken adds the given token into the ring for a specific ingester.
// If the token already exists for that ingester, it will be overwritten.
func (d *Desc) AddToken(id string, token StatefulToken, normaliseTokens bool) {
	ingester := d.Ingesters[id]

	if normaliseTokens {
		if ingester.InactiveTokens == nil {
			ingester.InactiveTokens = make(map[uint32]State)
		}

		found := false
		for _, t := range ingester.Tokens {
			if token.Token == t {
				if token.State != ACTIVE {
					ingester.InactiveTokens[t] = token.State
				} else {
					delete(ingester.InactiveTokens, t)
				}

				found = true
				break
			}
		}

		if !found {
			ingester.Tokens = append(ingester.Tokens, token.Token)
			if token.State != ACTIVE {
				ingester.InactiveTokens[token.Token] = token.State
			}
		}
	} else {
		found := false
		for i, t := range d.Tokens {
			if t.Token == token.Token {
				d.Tokens[i].State = token.State
				found = true
				break
			}
		}
		if !found {
			d.Tokens = append(d.Tokens, TokenDesc{
				Token:    token.Token,
				Ingester: id,
				State:    token.State,
			})
		}
		sort.Sort(ByToken(d.Tokens))
	}

	d.Ingesters[id] = ingester
}

// AddIngester adds the given ingester to the ring.
func (d *Desc) AddIngester(id, addr string, tokens []StatefulToken, state State, normaliseTokens bool) {
	if d.Ingesters == nil {
		d.Ingesters = map[string]IngesterDesc{}
	}

	ingester := IngesterDesc{
		Addr:           addr,
		Timestamp:      time.Now().Unix(),
		State:          state,
		StatefulTokens: true,
	}

	if normaliseTokens {
		ingester.Tokens = make([]uint32, len(tokens))
		ingester.InactiveTokens = make(map[uint32]State)

		for i, tok := range tokens {
			ingester.Tokens[i] = tok.Token
			if tok.State != ACTIVE {
				ingester.InactiveTokens[tok.Token] = tok.State
			}
		}
	} else {
		for _, pair := range tokens {
			d.Tokens = append(d.Tokens, TokenDesc{
				Token:    pair.Token,
				Ingester: id,
				State:    pair.State,
			})
		}
		sort.Sort(ByToken(d.Tokens))
	}

	d.Ingesters[id] = ingester
}

// RemoveIngester removes the given ingester and all its tokens.
func (d *Desc) RemoveIngester(id string) {
	delete(d.Ingesters, id)
	output := []TokenDesc{}
	for i := 0; i < len(d.Tokens); i++ {
		if d.Tokens[i].Ingester != id {
			output = append(output, d.Tokens[i])
		}
	}
	d.Tokens = output
}

// ClaimTokens transfers all the tokens from one ingester to another,
// returning the claimed token.
// This method assumes that Ring is in the correct state, 'from' ingester has no tokens anywhere,
// and 'to' ingester uses either normalised or non-normalised tokens, but not both. Tokens list must
// be sorted properly. If all of this is true, everything will be fine.
func (d *Desc) ClaimTokens(from, to string, normaliseTokens bool) []StatefulToken {
	var result []StatefulToken

	if normaliseTokens {
		// If the ingester we are claiming from is normalising, get its tokens then erase them from the ring.
		if fromDesc, found := d.Ingesters[from]; found {
			for _, tok := range fromDesc.Tokens {
				stok := StatefulToken{
					Token: tok,
					State: ACTIVE,
				}

				if s, ok := fromDesc.InactiveTokens[tok]; ok {
					stok.State = s
				}

				result = append(result, stok)
			}

			fromDesc.Tokens = nil
			d.Ingesters[from] = fromDesc
		}

		// If we are storing the tokens in a normalise form, we need to deal with
		// the migration from denormalised by removing the tokens from the tokens
		// list.
		// When all ingesters are in normalised mode, d.Tokens is empty here
		for i := 0; i < len(d.Tokens); {
			if d.Tokens[i].Ingester == from {
				result = append(result, StatefulToken{
					Token: d.Tokens[i].Token,
					State: d.Tokens[i].State,
				})

				d.Tokens = append(d.Tokens[:i], d.Tokens[i+1:]...)
				continue
			}
			i++
		}

		sort.Sort(ByStatefulTokens(result))
		ing := d.Ingesters[to]

		ing.Tokens = make([]uint32, len(result))

		for i, tok := range result {
			ing.Tokens[i] = tok.Token

			if tok.State != ACTIVE {
				if ing.InactiveTokens == nil {
					ing.InactiveTokens = make(map[uint32]State)
				}

				ing.InactiveTokens[tok.Token] = tok.State
			}
		}

		d.Ingesters[to] = ing
	} else {
		// If source ingester is normalising, copy its tokens to d.Tokens, and set new owner
		if fromDesc, found := d.Ingesters[from]; found {
			for _, t := range fromDesc.Tokens {
				st := StatefulToken{Token: t, State: ACTIVE}
				if s, ok := fromDesc.InactiveTokens[t]; ok {
					st.State = s
				}
				result = append(result, st)
			}

			fromDesc.Tokens = nil
			d.Ingesters[from] = fromDesc

			for _, t := range result {
				d.Tokens = append(d.Tokens, TokenDesc{Ingester: to, Token: t.Token, State: t.State})
			}

			sort.Sort(ByToken(d.Tokens))
		}

		// if source was normalising, this should not find new tokens
		for i := 0; i < len(d.Tokens); i++ {
			if d.Tokens[i].Ingester == from {
				d.Tokens[i].Ingester = to
				result = append(result, StatefulToken{
					Token: d.Tokens[i].Token,
					State: d.Tokens[i].State,
				})
			}
		}
	}

	// not necessary, but makes testing simpler
	if len(d.Tokens) == 0 {
		d.Tokens = nil
	}

	return result
}

// FindTokensByState returns the list of tokens in the given state
func (d *Desc) FindTokensByState(state State) []TokenDesc {
	var result []TokenDesc
	for _, token := range migrateRing(d) {
		if token.State == state {
			result = append(result, token)
		}
	}
	return result
}

// FindIngestersByState returns the list of ingesters in the given state
func (d *Desc) FindIngestersByState(state State) []IngesterDesc {
	var result []IngesterDesc
	for _, ing := range d.Ingesters {
		if ing.State == state {
			result = append(result, ing)
		}
	}
	return result
}

// Ready returns no error when all ingesters are active and healthy.
func (d *Desc) Ready(now time.Time, heartbeatTimeout time.Duration) error {
	numTokens := len(d.Tokens)
	for id, ingester := range d.Ingesters {
		if now.Sub(time.Unix(ingester.Timestamp, 0)) > heartbeatTimeout {
			return fmt.Errorf("ingester %s past heartbeat timeout", id)
		} else if ingester.State != ACTIVE {
			return fmt.Errorf("ingester %s in state %v", id, ingester.State)
		}
		numTokens += len(ingester.Tokens)
	}

	if numTokens == 0 {
		return fmt.Errorf("Not ready: no tokens in ring")
	}
	return nil
}

// RefreshTokenState updates the tokens in the ring with the state from the
// provided tokens. RefreshTokenState will remove denormalised tokens for id
// that are no longer in the list provided by tokens.
func (d *Desc) RefreshTokenState(id string, tokens []StatefulToken, normalise bool) {
	if d.Ingesters == nil {
		d.Ingesters = make(map[string]IngesterDesc)
	}

	ing, ok := d.Ingesters[id]
	if !ok {
		return
	}

	if normalise {
		ing.Tokens = make([]uint32, len(tokens))
		ing.InactiveTokens = make(map[uint32]State)

		for i, tok := range tokens {
			ing.Tokens[i] = tok.Token

			if tok.State != ACTIVE {
				ing.InactiveTokens[tok.Token] = tok.State
			}
		}

		d.Ingesters[id] = ing
	} else {
		newTokens := make([]TokenDesc, 0, len(d.Tokens))

		// Copy all tokens from other ingesters
		for _, tok := range d.Tokens {
			if tok.Ingester != id {
				newTokens = append(newTokens, tok)
				continue
			}
		}

		// Add back in our tokens
		for _, tok := range tokens {
			newTokens = append(newTokens, TokenDesc{
				Token:    tok.Token,
				State:    tok.State,
				Ingester: id,
			})
		}

		d.Tokens = newTokens

		// Re-sort the list
		sort.Sort(ByToken(d.Tokens))
	}
}

// TokensFor partitions the tokens into those for the given ID, and those for others.
func (d *Desc) TokensFor(id string) (tokens, other []StatefulToken) {
	var takenTokens, myTokens []StatefulToken

	for _, token := range migrateRing(d) {
		stok := StatefulToken{
			Token: token.Token,
			State: token.State,
		}

		takenTokens = append(takenTokens, stok)
		if token.Ingester == id {
			myTokens = append(myTokens, stok)
		}
	}

	return myTokens, takenTokens
}

// IsHealthyState checks whether an state is in a valid state and whether an
// ingester is heartbeating.
//
// IsHealthyState is used for validating a token state. For validating the
// overall ingester state, use IsHealthy.
func (i *IngesterDesc) IsHealthyState(op Operation, state State, heartbeatTimeout time.Duration) bool {
	if op == Write && state != ACTIVE {
		return false
	} else if op == Read && state == JOINING {
		return false
	}
	return time.Now().Sub(time.Unix(i.Timestamp, 0)) <= heartbeatTimeout
}

// IsHealthy checks whether the ingester appears to be alive and heartbeating
func (i *IngesterDesc) IsHealthy(op Operation, heartbeatTimeout time.Duration) bool {
	return i.IsHealthyState(op, i.State, heartbeatTimeout)
}

// Merge merges other ring into this one. Returns sub-ring that represents the change,
// and can be sent out to other clients.
//
// This merge function depends on the timestamp of the ingester. For each ingester,
// it will choose more recent state from the two rings, and put that into this ring.
// There is one exception: we accept LEFT state even if Timestamp hasn't changed.
//
// localCAS flag tells the merge that it can use incoming ring as a full state, and detect
// missing ingesters based on it. Ingesters from incoming ring will cause ingester
// to be marked as LEFT and gossiped about.
//
// If multiple ingesters end up owning the same tokens, Merge will do token conflict resolution
// (see resolveConflicts).
//
// This method is part of memberlist.Mergeable interface, and is only used by gossiping ring.
func (d *Desc) Merge(mergeable memberlist.Mergeable, localCAS bool) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*Desc)
	if !ok {
		// This method only deals with non-nil rings.
		return nil, fmt.Errorf("expected *ring.Desc, got %T", mergeable)
	}

	if other == nil {
		return nil, nil
	}

	thisIngesterMap := buildNormalizedIngestersMap(d)
	otherIngesterMap := buildNormalizedIngestersMap(other)

	var updated []string

	for name, oing := range otherIngesterMap {
		ting := thisIngesterMap[name]
		// firstIng.Timestamp will be 0, if there was no such ingester in our version
		if oing.Timestamp > ting.Timestamp {
			oing.Tokens = append([]uint32(nil), oing.Tokens...) // make a copy of tokens
			thisIngesterMap[name] = oing
			updated = append(updated, name)
		} else if oing.Timestamp == ting.Timestamp && ting.State != LEFT && oing.State == LEFT {
			// we accept LEFT even if timestamp hasn't changed
			thisIngesterMap[name] = oing // has no tokens already
			updated = append(updated, name)
		}
	}

	if localCAS {
		// This breaks commutativity! But we only do it locally, not when gossiping with others.
		for name, ting := range thisIngesterMap {
			if _, ok := otherIngesterMap[name]; !ok && ting.State != LEFT {
				// missing, let's mark our ingester as LEFT
				ting.State = LEFT
				ting.Tokens = nil
				thisIngesterMap[name] = ting

				updated = append(updated, name)
			}
		}
	}

	// No updated ingesters
	if len(updated) == 0 {
		return nil, nil
	}

	// resolveConflicts allocates lot of memory, so if we can avoid it, do that.
	if conflictingTokensExist(thisIngesterMap) {
		resolveConflicts(thisIngesterMap)
	}

	// Let's build a "change" for returning
	out := NewDesc()
	for _, u := range updated {
		ing := thisIngesterMap[u]
		out.Ingesters[u] = ing
	}

	// Keep ring normalized.
	d.Ingesters = thisIngesterMap
	d.Tokens = nil

	return out, nil
}

// MergeContent describes content of this Mergeable.
// Ring simply returns list of ingesters that it includes.
func (d *Desc) MergeContent() []string {
	result := []string(nil)
	for k := range d.Ingesters {
		result = append(result, k)
	}
	return result
}

// buildNormalizedIngestersMap will do the following:
// - moves all tokens from r.Tokens into individual ingesters
// - sorts tokens and removes duplicates (only within single ingester)
// - it doesn't modify input ring
func buildNormalizedIngestersMap(inputRing *Desc) map[string]IngesterDesc {
	out := map[string]IngesterDesc{}

	// Make sure LEFT ingesters have no tokens
	for n, ing := range inputRing.Ingesters {
		if ing.State == LEFT {
			ing.Tokens = nil
		}
		out[n] = ing
	}

	for _, t := range inputRing.Tokens {
		// if ingester doesn't exist, we will add empty one (with tokens only)
		ing := out[t.Ingester]

		// don't add tokens to the LEFT ingesters. We skip such tokens.
		if ing.State != LEFT {
			ing.Tokens = append(ing.Tokens, t.Token)
			out[t.Ingester] = ing
		}
	}

	// Sort tokens, and remove duplicates
	for name, ing := range out {
		if ing.Tokens == nil {
			continue
		}

		if !sort.IsSorted(sortableUint32(ing.Tokens)) {
			sort.Sort(sortableUint32(ing.Tokens))
		}

		seen := make(map[uint32]bool)

		n := 0
		for _, v := range ing.Tokens {
			if !seen[v] {
				seen[v] = true
				ing.Tokens[n] = v
				n++
			}
		}
		ing.Tokens = ing.Tokens[:n]

		// write updated value back to map
		out[name] = ing
	}

	return out
}

func conflictingTokensExist(normalizedIngesters map[string]IngesterDesc) bool {
	count := 0
	for _, ing := range normalizedIngesters {
		count += len(ing.Tokens)
	}

	tokensMap := make(map[uint32]bool, count)
	for _, ing := range normalizedIngesters {
		for _, t := range ing.Tokens {
			if tokensMap[t] {
				return true
			}
			tokensMap[t] = true
		}
	}
	return false
}

// This function resolves token conflicts, if there are any.
//
// We deal with two possibilities:
// 1) if one node is LEAVING or LEFT and the other node is not, LEVING/LEFT one loses the token
// 2) otherwise node names are compared, and node with "lower" name wins the token
//
// Modifies ingesters map with updated tokens.
func resolveConflicts(normalizedIngesters map[string]IngesterDesc) {
	size := 0
	for _, ing := range normalizedIngesters {
		size += len(ing.Tokens)
	}
	tokens := make([]uint32, 0, size)
	tokenToIngester := make(map[uint32]string, size)

	for ingKey, ing := range normalizedIngesters {
		if ing.State == LEFT {
			// LEFT ingesters don't use tokens anymore
			continue
		}

		for _, token := range ing.Tokens {
			prevKey, found := tokenToIngester[token]
			if !found {
				tokens = append(tokens, token)
				tokenToIngester[token] = ingKey
			} else {
				// there is already ingester for this token, let's do conflict resolution
				prevIng := normalizedIngesters[prevKey]

				winnerKey := ingKey
				switch {
				case ing.State == LEAVING && prevIng.State != LEAVING:
					winnerKey = prevKey
				case prevIng.State == LEAVING && ing.State != LEAVING:
					winnerKey = ingKey
				case ingKey < prevKey:
					winnerKey = ingKey
				case prevKey < ingKey:
					winnerKey = prevKey
				}

				tokenToIngester[token] = winnerKey
			}
		}
	}

	sort.Sort(sortableUint32(tokens))

	// let's store the resolved result back
	newTokenLists := map[string][]uint32{}
	for key := range normalizedIngesters {
		// make sure that all ingesters start with empty list
		// especially ones that will no longer have any tokens
		newTokenLists[key] = nil
	}

	// build list of tokens for each ingester
	for _, token := range tokens {
		key := tokenToIngester[token]
		newTokenLists[key] = append(newTokenLists[key], token)
	}

	// write tokens back
	for key, tokens := range newTokenLists {
		ing := normalizedIngesters[key]
		ing.Tokens = tokens
		normalizedIngesters[key] = ing
	}
}

// RemoveTombstones removes LEFT ingesters older than given time limit. If time limit is zero, remove all LEFT ingesters.
func (d *Desc) RemoveTombstones(limit time.Time) {
	removed := 0
	for n, ing := range d.Ingesters {
		if ing.State == LEFT && (limit.IsZero() || time.Unix(ing.Timestamp, 0).Before(limit)) {
			// remove it
			delete(d.Ingesters, n)
			removed++
		}
	}
}

// search searches the ring for a token. Assumes that the
// ring is already denormalised.
func (d *Desc) search(key uint32) int {
	i := sort.Search(len(d.Tokens), func(x int) bool {
		return d.Tokens[x].Token >= key
	})
	if i >= len(d.Tokens) {
		i = 0
	}
	return i
}

// NeighborOptions holds options to configure how a ring
// will be searched for predecessors or successors.
type NeighborOptions struct {
	// Start is the starting token to search from.
	Start StatefulToken

	// Neighbor is the number neighbor to search for.
	// For example, a Neighbor value of 1 means to
	// search for the first successor to Start.
	Neighbor int

	// Op is the operation for which the search is
	// being performed.
	Op Operation

	// IncludeStart determines whether or not the
	// token defined by Start should be counted as one of the
	// unique ingesters.
	IncludeStart bool

	// MaxHeartbeat is the maximum heartbeat age to consider an ingester healthy.
	MaxHeartbeat time.Duration
}

// Predecessors is a function that acts similarly to Successor but
// searches the ring in counter-clockwise order. Unlike calling Successor,
// Predecessors returns multiple tokens: Predecessors is the equivalent of
// finding all tokens of which tok is successor n to.
func (d *Desc) Predecessors(opts NeighborOptions) ([]TokenDesc, error) {
	idx := d.search(opts.Start.Token)
	if len(d.Tokens) == 0 || d.Tokens[idx].Token != opts.Start.Token {
		return nil, fmt.Errorf("could not find token %d in ring", opts.Start.Token)
	}
	if opts.Neighbor == 0 {
		return []TokenDesc{d.Tokens[idx]}, nil
	} else if opts.Neighbor < 0 {
		return nil, errors.New("Predecessors may only be called with a positive Neighbor value")
	}

	// Temporarily make the start token ACTIVE since we want it to
	// be seen in the ring as in the replica set.
	oldState := d.Tokens[idx].State
	d.Tokens[idx].State = ACTIVE
	defer func() {
		d.Tokens[idx].State = oldState
	}()

	// Naive solution: go over every token in the ring and see if its
	// opts.Neighbor successor is opts.Start.
	predecessors := []TokenDesc{}
	for _, t := range d.Tokens {
		ing := d.Ingesters[t.Ingester]
		if !IsHealthyState(&ing, t.State, opts.Op, opts.MaxHeartbeat) {
			continue
		}

		succ, err := d.Successor(NeighborOptions{
			Start:        t.StatefulToken(),
			Neighbor:     opts.Neighbor,
			Op:           opts.Op,
			MaxHeartbeat: opts.MaxHeartbeat,
			IncludeStart: true,
		})
		if err != nil {
			return predecessors, err
		} else if succ.Token == opts.Start.Token {
			predecessors = append(predecessors, t)
		}
	}

	return predecessors, nil
}

// Successor moves around the ring to find the nth neighbors of tok.
// The healthiness of the ingester and the state of the token defines
// which tokens are considered as successors.
//
// Tokens are only considered once based on their ingester; if
// opts.IncludeStart is true, then the ingster for the opts.Start
// argument will not be considered as a neighbor (i.e., it will act
// as if we've already seen it).
//
// If Successor is called with a positive opts.Neighbor value, Successor
// searches the ring clockwise. Otherwise, if called with a negative value,
// Successor searches the ring counter-clockwise.
func (d *Desc) Successor(opts NeighborOptions) (TokenDesc, error) {
	idx := d.search(opts.Start.Token)
	if d.Tokens[idx].Token != opts.Start.Token {
		return TokenDesc{}, fmt.Errorf("could not find token %d in ring", opts.Start.Token)
	}
	if opts.Neighbor == 0 {
		return d.Tokens[idx], nil
	}

	numSuccessors := opts.Neighbor
	if numSuccessors < 0 {
		numSuccessors = -numSuccessors
	}

	successors := make([]TokenDesc, 0, numSuccessors)
	distinct := map[string]struct{}{}
	if opts.IncludeStart {
		distinct[d.Tokens[idx].Ingester] = struct{}{}
	}

	startIdx := idx

	direction := 1
	if opts.Neighbor < 0 {
		direction = -1
	}

	for {
		idx += direction
		if idx < 0 {
			idx = len(d.Tokens) - 1
		} else {
			idx %= len(d.Tokens)
		}

		// Stop if we've completely circled the ring
		if idx == startIdx {
			break
		}

		successor := d.Tokens[idx]
		if _, ok := distinct[successor.Ingester]; ok {
			continue
		}
		ing := d.Ingesters[successor.Ingester]

		if IsHealthyState(&ing, successor.State, opts.Op, opts.MaxHeartbeat) {
			successors = append(successors, successor)
			if len(successors) == numSuccessors {
				return successors[numSuccessors-1], nil
			}

			distinct[successor.Ingester] = struct{}{}
		}
	}

	return TokenDesc{},
		fmt.Errorf("could not find neighbor #%d for token %d", opts.Neighbor, opts.Start.Token)
}

// RangeOptions configures the search parameters of Desc.InRange.
type RangeOptions struct {
	// Range of tokens to search.
	Range TokenRange

	// ID is the ingester ID to search for.
	ID string

	// Op determines which token are included in the range and
	// which are skipped.
	Op Operation

	// LeftInclusive determines that the left hand side of the
	// pair is inclusive.
	LeftInclusive bool

	// RightInclusive determines that the right hand side of the
	// pair is inclusive.
	RightInclusive bool

	// MaxHeartbeat is the maximum heartbeat age to consider an ingester healthy.
	MaxHeartbeat time.Duration
}

// InRange checks to see if a given ingester ID (specified by
// opts.ID) is in the range from opts.From to opts.To. The
// inclusivity of each side of the range is determined by
// opts.LeftInclusive and opts.RightInclusive.
func (d *Desc) InRange(opts RangeOptions) bool {
	start := d.search(opts.Range.From)
	startTok := d.Tokens[start]
	startIng := d.Ingesters[startTok.Ingester]

	if opts.LeftInclusive && startTok.Ingester == opts.ID &&
		IsHealthyState(&startIng, startTok.State, opts.Op, opts.MaxHeartbeat) {

		return true
	} else if startTok.Token == opts.Range.To {
		return opts.RightInclusive && startTok.Ingester == opts.ID &&
			IsHealthyState(&startIng, startTok.State, opts.Op, opts.MaxHeartbeat)
	}

	idx := start
	for {
		idx++
		idx %= len(d.Tokens)

		tok := d.Tokens[idx]

		if idx == start {
			break
		} else if tok.Token >= opts.Range.To || tok.Token <= opts.Range.From {
			break
		} else if tok.Ingester == opts.ID {
			ing := d.Ingesters[tok.Ingester]
			healthy := IsHealthyState(&ing, tok.State, opts.Op, opts.MaxHeartbeat)
			if healthy {
				return true
			}
		}
	}

	tok := d.Tokens[idx]
	ing := d.Ingesters[tok.Ingester]
	return opts.RightInclusive && tok.Ingester == opts.ID &&
		IsHealthyState(&ing, tok.State, opts.Op, opts.MaxHeartbeat)
}
