package ring

import (
	"fmt"
	"time"
)

// replicationStrategy decides, given the set of tokens eligible for a key,
// which tokens will be used for an operation and how many failures will
// be tolerated. Tokens is expected to be a list where each token belongs
// to a unique ingester.
//
// replicationStrategy will check to see if there's enough tokens for
// an operation to succeed; unhealthy tokens are filtered out using
// IsHealthyState.
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

		if IsHealthyState(&ing, tokens[i].State, op, r.cfg.HeartbeatTimeout) {
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

// IsHealthyState checks whether an state is in a valid state and whether an
// ingester is heartbeating.
//
// IsHealthyState is used for validating a specific state, and is useful for
// checking individual tokens.
func IsHealthyState(ingester *IngesterDesc, state State, op Operation, maxHeartbeat time.Duration) bool {
	return ingester.IsHealthyState(op, state, maxHeartbeat)
}

// IsHealthy checks whether an ingester appears to be alive and heartbeating.
// The heartbeat must be no older than maxHeartbeat to be considered alive.
func IsHealthy(ingester *IngesterDesc, op Operation, maxHeartbeat time.Duration) bool {
	return ingester.IsHealthy(op, maxHeartbeat)
}
