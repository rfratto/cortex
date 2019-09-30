package ring

import (
	"fmt"
)

// replicationStrategy decides, given the set of ingesters eligible for a key,
// which ingesters you will try and write to and how many failures you will
// tolerate.
// - Filters out dead ingesters so the one doesn't even try to write to them.
// - Checks there is enough ingesters for an operation to succeed.
// The ingesters argument may be overwritten.
func (r *Ring) replicationStrategy(tokens []TokenDesc, op Operation) ([]TokenDesc, int, error) {

	// We need a response from a quorum of ingesters, which is n/2 + 1.  In the
	// case of a node joining/leaving, the actual replica set might be bigger
	// than the replication factor, so use the bigger or the two.
	replicationFactor := r.cfg.ReplicationFactor
	if len(tokens) > replicationFactor {
		replicationFactor = len(tokens)
	}
	minSuccess := (replicationFactor / 2) + 1
	maxFailure := replicationFactor - minSuccess

	// Skip those that have not heartbeated in a while. NB these are still
	// included in the calculation of minSuccess, so if too many failed ingesters
	// will cause the whole write to fail.
	for i := 0; i < len(tokens); {
		ing := r.ringDesc.Ingesters[tokens[i].Ingester]

		if r.IsHealthyState(&ing, tokens[i].State, op) {
			i++
		} else {
			tokens = append(tokens[:i], tokens[i+1:]...)
			maxFailure--
		}
	}

	// This is just a shortcut - if there are not minSuccess available ingesters,
	// after filtering out dead ones, don't even bother trying.
	if maxFailure < 0 || len(tokens) < minSuccess {
		err := fmt.Errorf("at least %d live ingesters required, could only find %d",
			minSuccess, len(tokens))
		return nil, 0, err
	}

	return tokens, maxFailure, nil
}

// IsHealthyState checks whether an state is in a valid state and whether an
// ingester is heartbeating.
//
// IsHealthyState is used for validating a token state. For validating the
// overall ingester state, use IsHealthy.
func (r *Ring) IsHealthyState(ingester *IngesterDesc, state State, op Operation) bool {
	return ingester.IsHealthyState(op, state, r.cfg.HeartbeatTimeout)
}

// IsHealthy checks whether an ingester appears to be alive and heartbeating
func (r *Ring) IsHealthy(ingester *IngesterDesc, op Operation) bool {
	return ingester.IsHealthy(op, r.cfg.HeartbeatTimeout)
}

// ReplicationFactor of the ring.
func (r *Ring) ReplicationFactor() int {
	return r.cfg.ReplicationFactor
}

// IngesterCount is number of ingesters in the ring
func (r *Ring) IngesterCount() int {
	r.mtx.Lock()
	c := len(r.ringDesc.Ingesters)
	r.mtx.Unlock()
	return c
}
