package ingester

import (
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

var (
	blockedRanges = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_blocked_ranges",
		Help: "The current number of ranges that will not accept writes by this ingester.",
	})

	incSentChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_sent_chunks",
		Help: "The total number of chunks sent by this ingester whilst leaving.",
	})
	incReceivedChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_received_chunks",
		Help: "The total number of chunks received by this ingester whilst joining",
	})

	incSentSeries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_sent_series",
		Help: "The total number of series sent by this ingester whilst leaving.",
	})
	incReceivedSeries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_received_series",
		Help: "The total number of series received by this ingester whilst joining",
	})
)

func init() {
	prometheus.MustRegister(blockedRanges)
	prometheus.MustRegister(incSentChunks)
	prometheus.MustRegister(incReceivedChunks)
	prometheus.MustRegister(incSentSeries)
	prometheus.MustRegister(incReceivedSeries)
}

// BlockTokenRange handles a remote request for a token range to be blocked.
// Blocked ranges will automatically be unblocked after the RangeBlockPeriod
// configuration variable.
//
// When a range is blocked, the Ingester will no longer accept pushes
// for any streams whose tokens falls within the blocked ranges. Unblocking
// the range re-enables those pushes.
func (i *Ingester) BlockTokenRange(ctx context.Context, req *client.RangeRequest) (*client.BlockRangeResponse, error) {
	i.blockedTokenMtx.Lock()
	defer i.blockedTokenMtx.Unlock()

	for _, rg := range req.Ranges {
		for _, r := range i.blockedTokens {
			if r.From == rg.StartRange && r.To == rg.EndRange {
				// Multiple clients may request a block simultaneously. We silently
				// exit so they can continue processing as normal.
				level.Warn(util.Logger).Log("msg", "token range already blocked",
					"start_token", rg.StartRange, "end_token", rg.EndRange)

				continue
			}
		}

		i.blockedTokens = append(i.blockedTokens, ring.TokenRange{
			From: rg.StartRange,
			To:   rg.EndRange,
		})

		blockedRanges.Inc()
	}

	go func() {
		<-time.After(i.cfg.RangeBlockPeriod)
		i.blockedTokenMtx.Lock()
		defer i.blockedTokenMtx.Unlock()

		for _, rg := range req.Ranges {
			idx := -1

			for n, r := range i.blockedTokens {
				if r.From == rg.StartRange && r.To == rg.EndRange {
					idx = n
					break
				}
			}

			if idx == -1 {
				continue
			}

			level.Warn(util.Logger).Log("msg", "auto-removing blocked range",
				"start_token", rg.StartRange, "end_token", rg.EndRange)
			i.blockedTokens = append(i.blockedTokens[:idx], i.blockedTokens[idx+1:]...)
			blockedRanges.Dec()
		}
	}()

	return &client.BlockRangeResponse{}, nil
}

// UnblockTokenRange handles a remote request for a token range to be
// unblocked.
func (i *Ingester) UnblockTokenRange(ctx context.Context, req *client.RangeRequest) (*client.UnblockRangeResponse, error) {
	i.blockedTokenMtx.Lock()
	defer i.blockedTokenMtx.Unlock()

	for _, rg := range req.Ranges {
		idx := -1

		for n, r := range i.blockedTokens {
			if r.From == rg.StartRange && r.To == rg.EndRange {
				idx = n
				break
			}
		}

		if idx == -1 {
			// Multiple clients may request an unblock simultaneously. We want to silently fail
			// so they can continue processing as normal.
			level.Warn(util.Logger).Log("msg", "token range not blocked",
				"start_token", rg.StartRange, "end_token", rg.EndRange)
			continue
		}

		i.blockedTokens = append(i.blockedTokens[:idx], i.blockedTokens[idx+1:]...)
		blockedRanges.Dec()
	}

	return &client.UnblockRangeResponse{}, nil
}

// BlockRanges connects to the ingester at targetAddr and informs them
// to stop accepting writes in a series of token ranges specified
// as [from, to).
func (i *Ingester) BlockRanges(ctx context.Context, ranges []ring.TokenRange, targetAddr string) error {

	if targetAddr == "" {
		_, err := i.BlockTokenRange(ctx, &client.RangeRequest{
			Ranges: makeProtoRanges(ranges),
		})
		return err
	}

	c, err := i.cfg.ingesterClientFactory(targetAddr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, "-1")
	_, err = c.BlockTokenRange(ctx, &client.RangeRequest{
		Ranges: makeProtoRanges(ranges),
	})

	if err != nil {
		err = errors.Wrap(err, "BlockTokenRange")
	}
	return nil
}

// UnblockRanges connects to the ingester at targetAddr and informs them
// to remove one or more blocks previously set by BlockRanges.
func (i *Ingester) UnblockRanges(ctx context.Context, ranges []ring.TokenRange,
	targetAddr string) error {

	if targetAddr == "" {
		_, err := i.UnblockTokenRange(ctx, &client.RangeRequest{
			Ranges: makeProtoRanges(ranges),
		})
		return err
	}

	c, err := i.cfg.ingesterClientFactory(targetAddr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, "-1")
	_, err = c.UnblockTokenRange(ctx, &client.RangeRequest{
		Ranges: makeProtoRanges(ranges),
	})

	if err != nil {
		err = errors.Wrap(err, "BlockTokenRange")
	}
	return nil
}

// SendChunkRanges connects to the ingester at targetAddr and sends all
// chunks for streams whose fingerprint falls within the series of
// specified ranges.
func (i *Ingester) SendChunkRanges(ctx context.Context, ranges []ring.TokenRange,
	targetAddr string) error {

	userStatesCopy := i.userStates.cp()
	if len(userStatesCopy) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	level.Debug(util.Logger).Log(
		"msg", "sending chunks in range",
		"to_ingester", targetAddr,
		"ranges", ring.PrintableRanges(ranges),
	)

	c, err := i.cfg.ingesterClientFactory(targetAddr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, "-1")
	stream, err := c.SendChunks(ctx)
	if err != nil {
		return errors.Wrap(err, "SendChunks")
	}

	_, err = i.pushChunks(pushChunksOptions{
		States:        userStatesCopy,
		Stream:        stream,
		DisallowFlush: true,
		SentChunks:    incSentChunks,
		SentSeries:    incSentSeries,

		Filter: func(pair *fingerprintSeriesPair) bool {
			return !inAnyRange(pair.series.token, ranges)
		},
	})
	if err != nil {
		return err
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "CloseAndRecv")
	}

	level.Debug(util.Logger).Log(
		"msg", "sent chunks in range",
		"to_ingester", targetAddr,
		"ranges", ring.PrintableRanges(ranges),
	)
	return nil
}

// RequestChunkRanges connects to the ingester at targetAddr and requests all
// chunks for streams whose fingerprint falls within the specified token
// ranges.
//
// If move is true, the target ingester should remove sent chunks from
// local memory if the transfer succeeds.
func (i *Ingester) RequestChunkRanges(ctx context.Context, ranges []ring.TokenRange,
	targetAddr string, move bool) error {

	c, err := i.cfg.ingesterClientFactory(targetAddr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	level.Debug(util.Logger).Log(
		"msg", "requesting chunks in range",
		"from_ingester", targetAddr,
		"ranges", ring.PrintableRanges(ranges),
	)

	ctx = user.InjectOrgID(ctx, "-1")
	stream, err := c.GetChunks(ctx, &client.GetChunksRequest{
		Ranges:         makeProtoRanges(ranges),
		Move:           move,
		FromIngesterId: i.lifecycler.ID,
	})
	if err != nil {
		return errors.Wrap(err, "GetChunks")
	}

	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()

	_, seriesReceived, err := i.acceptChunks(acceptChunksOptions{
		UserStates:     i.userStates,
		Stream:         stream,
		ReceivedChunks: incReceivedChunks,
		ReceivedSeries: incReceivedSeries,
	})
	if err != nil {
		return err
	}

	level.Debug(util.Logger).Log(
		"msg", "received chunks in ranges",
		"from_ingester", targetAddr,
		"series_received", seriesReceived,
		"ranges", ring.PrintableRanges(ranges),
	)

	return nil
}

// StreamTokens returns series of tokens for in-memory streams.
func (i *Ingester) StreamTokens() []uint32 {
	ret := []uint32{}

	userStatesCopy := i.userStates.cp()
	for _, state := range userStatesCopy {
		for pair := range state.fpToSeries.iter() {
			// Skip when there's no chunks in a series. Used to avoid a panic on
			// calling head.
			if len(pair.series.chunkDescs) == 0 {
				continue
			}

			if !pair.series.head().flushed {
				ret = append(ret, pair.series.token)
			}
		}
	}

	return ret
}

func makeRingRanges(ranges []client.TokenRange) []ring.TokenRange {
	ret := make([]ring.TokenRange, len(ranges))
	for i, rg := range ranges {
		ret[i] = ring.TokenRange{
			From: rg.StartRange,
			To:   rg.EndRange,
		}
	}
	return ret
}

func makeProtoRanges(ranges []ring.TokenRange) []client.TokenRange {
	ret := make([]client.TokenRange, len(ranges))
	for i, rg := range ranges {
		ret[i] = client.TokenRange{
			StartRange: rg.From,
			EndRange:   rg.To,
		}
	}
	return ret
}

func inAnyRange(tok uint32, ranges []ring.TokenRange) bool {
	for _, rg := range ranges {
		if rg.Contains(tok) {
			return true
		}
	}
	return false
}
