package ring

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// transferWorkload is a map of target ingester addresses to the set
// of token ranges that "affect" that target ingester. When joining
// the ring, chunks should be requested from the target ingester
// based on the token ranges. When leaving the ring, chunks should be
// sent to the target ingester.
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
			Offset:       replica,
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
				Offset:       -1,
				Op:           op,
				MaxHeartbeat: i.cfg.RingConfig.HeartbeatTimeout,
			})
			if err != nil {
				lastErr = err
				continue
			}

			target, err := d.Successor(NeighborOptions{
				Start:        startRange.StatefulToken(),
				Offset:       rf,
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
