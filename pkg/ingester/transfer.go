package ingester

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/shipper"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/user"
)

var (
	blockedRanges = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_blocked_ranges",
		Help: "The current number of ranges that will not accept writes by this ingester.",
	})

	sentChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_sent_chunks",
		Help: "The total number of chunks sent by this ingester whilst leaving.",
	})
	receivedChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_received_chunks",
		Help: "The total number of chunks received by this ingester whilst joining",
	})
	sentFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_sent_files",
		Help: "The total number of files sent by this ingester whilst leaving.",
	})
	receivedFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_received_files",
		Help: "The total number of files received by this ingester whilst joining",
	})

	incSentChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_sent_chunks",
		Help: "The total number of chunks sent by this ingester whilst leaving.",
	})
	incReceivedChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_received_chunks",
		Help: "The total number of chunks received by this ingester whilst joining",
	})

	incIgnoredSeries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_ignored_series",
		Help: "The total number of chunks ignored by this ingester",
	})
	incSentSeries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_sent_series",
		Help: "The total number of series sent by this ingester whilst leaving.",
	})
	incReceivedSeries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_incremental_received_series",
		Help: "The total number of series received by this ingester whilst joining",
	})

	once *sync.Once
)

func init() {
	once = &sync.Once{}
	prometheus.MustRegister(blockedRanges)
	prometheus.MustRegister(sentChunks)
	prometheus.MustRegister(receivedChunks)
	prometheus.MustRegister(sentFiles)
	prometheus.MustRegister(receivedFiles)
	prometheus.MustRegister(incSentChunks)
	prometheus.MustRegister(incIgnoredSeries)
	prometheus.MustRegister(incReceivedChunks)
	prometheus.MustRegister(incSentSeries)
	prometheus.MustRegister(incReceivedSeries)
}

type chunkStream interface {
	Context() context.Context
	Recv() (*client.TimeSeriesChunk, error)
}

type acceptChunksOptions struct {
	UserStates            *userStates
	Stream                chunkStream
	ReceivedChunks        prometheus.Counter
	ReceivedSeries        prometheus.Counter
	ValidateRemoteLeaving bool
}

func (i *Ingester) acceptChunks(opts acceptChunksOptions) (fromIngesterID string, seriesReceived int, err error) {
	for {
		wireSeries, err := opts.Stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fromIngesterID, seriesReceived, err
		}

		// We can't send "extra" fields with a streaming call, so we repeat
		// wireSeries.FromIngesterId and assume it is the same every time
		// round this loop.
		if fromIngesterID == "" {
			fromIngesterID = wireSeries.FromIngesterId
			level.Info(util.Logger).Log("msg", "processing TransferChunks request", "from_ingester", fromIngesterID)
		}
		if opts.ValidateRemoteLeaving {
			// Before transfer, make sure 'from' ingester is in correct state to call ClaimTokensFor later.
			err := i.checkFromIngesterIsInLeavingState(opts.Stream.Context(), fromIngesterID)
			if err != nil {
				return fromIngesterID, seriesReceived, err
			}
		}

		descs, err := fromWireChunks(wireSeries.Chunks)
		if err != nil {
			return fromIngesterID, seriesReceived, err
		}

		state, fp, series, newSeries, err := opts.UserStates.getOrCreateSeries(opts.Stream.Context(), wireSeries.UserId, wireSeries.Labels, wireSeries.Token)
		if err != nil {
			return fromIngesterID, seriesReceived, err
		}

		if i.cfg.TokenCheckerConfig.CheckOnTransfer {
			if ok := i.tokenChecker.CheckToken(wireSeries.Token); !ok {
				level.Warn(util.Logger).Log(
					"msg", "unexpected token transferred to ingester",
					"token", wireSeries.Token,
				)
				i.metrics.unexpectedSeriesTotal.WithLabelValues("transfer").Inc()
			}
		}

		prevNumChunks := len(series.chunkDescs)

		if newSeries {
			// Make sure we don't accidentally overwrite data we didn't receive
			// by only accepting writes to series that we don't currently have in
			// memory.
			err = series.setChunks(descs)
		} else {
			incIgnoredSeries.Inc()
		}

		state.fpLocker.Unlock(fp) // acquired in getOrCreateSeries
		if err != nil {
			return fromIngesterID, seriesReceived, err
		}

		seriesReceived++
		memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))

		if opts.ReceivedChunks != nil {
			opts.ReceivedChunks.Add(float64(len(descs)))
		}
		if opts.ReceivedSeries != nil {
			opts.ReceivedSeries.Inc()
		}
	}

	return fromIngesterID, seriesReceived, nil
}

type chunkPushStream interface {
	Send(*client.TimeSeriesChunk) error
}

type pushChunksOptions struct {
	States        map[string]*userState
	Stream        chunkPushStream
	Filter        func(pair *fingerprintSeriesPair) bool
	DisallowFlush bool

	SentChunks prometheus.Counter
	SentSeries prometheus.Counter
}

func (i *Ingester) pushChunks(opts pushChunksOptions) (sent int, err error) {
	for userID, state := range opts.States {
		for pair := range state.fpToSeries.iter() {
			if opts.Filter != nil && opts.Filter(&pair) {
				continue
			}

			state.fpLocker.Lock(pair.fp)

			if len(pair.series.chunkDescs) == 0 {
				state.fpLocker.Unlock(pair.fp)
				continue
			}

			chunks, err := toWireChunks(pair.series.chunkDescs)
			if err != nil {
				state.fpLocker.Unlock(pair.fp)
				return sent, errors.Wrap(err, "toWireChunks")
			}

			err = opts.Stream.Send(&client.TimeSeriesChunk{
				FromIngesterId: i.lifecycler.ID,
				UserId:         userID,
				Labels:         client.FromLabelsToLabelAdapters(pair.series.metric),
				Chunks:         chunks,
				Token:          pair.series.token,
			})
			if err == nil && opts.DisallowFlush {
				// Mark all the chunks as "flushed". They'll retain in
				// memory until the idle limit kicks in.
				for _, desc := range pair.series.chunkDescs {
					desc.flushed = true
				}
			}

			state.fpLocker.Unlock(pair.fp)

			if err != nil {
				return sent, errors.Wrap(err, "Send")
			}

			sent += len(chunks)

			if opts.SentChunks != nil {
				opts.SentChunks.Add(float64(len(chunks)))
			}
			if opts.SentSeries != nil {
				opts.SentSeries.Inc()
			}
		}
	}

	return sent, nil
}

// TransferChunks receives all the chunks from another ingester.
func (i *Ingester) TransferChunks(stream client.Ingester_TransferChunksServer) error {
	fromIngesterID := ""
	seriesReceived := 0

	xfer := func() error {
		userStates := newUserStates(i.limiter, i.cfg)

		fromIngesterID, seriesReceived, err := i.acceptChunks(acceptChunksOptions{
			UserStates:            userStates,
			Stream:                stream,
			ReceivedChunks:        receivedChunks,
			ValidateRemoteLeaving: true,
		})
		if err != nil {
			return err
		}

		if seriesReceived == 0 {
			level.Error(util.Logger).Log("msg", "received TransferChunks request with no series", "from_ingester", fromIngesterID)
			return fmt.Errorf("TransferChunks: no series")
		}

		if fromIngesterID == "" {
			level.Error(util.Logger).Log("msg", "received TransferChunks request with no ID from ingester")
			return fmt.Errorf("no ingester id")
		}

		if err := i.lifecycler.ClaimTokensFor(stream.Context(), fromIngesterID); err != nil {
			return errors.Wrap(err, "TransferChunks: ClaimTokensFor")
		}

		i.userStatesMtx.Lock()
		defer i.userStatesMtx.Unlock()

		i.userStates = userStates
		return nil
	}

	if err := i.transfer(stream.Context(), xfer); err != nil {
		return err
	}

	// Close the stream last, as this is what tells the "from" ingester that
	// it's OK to shut down.
	if err := stream.SendAndClose(&client.TransferChunksResponse{}); err != nil {
		level.Error(util.Logger).Log("msg", "Error closing TransferChunks stream", "from_ingester", fromIngesterID, "err", err)
		return err
	}
	level.Info(util.Logger).Log("msg", "Successfully transferred chunks", "from_ingester", fromIngesterID, "series_received", seriesReceived)

	return nil
}

// Ring gossiping: check if "from" ingester is in LEAVING state. It should be, but we may not see that yet
// when using gossip ring. If we cannot see ingester is the LEAVING state yet, we don't accept this
// transfer, as claiming tokens would possibly end up with this ingester owning no tokens, due to conflict
// resolution in ring merge function. Hopefully the leaving ingester will retry transfer again.
func (i *Ingester) checkFromIngesterIsInLeavingState(ctx context.Context, fromIngesterID string) error {
	v, err := i.lifecycler.KVStore.Get(ctx, ring.ConsulKey)
	if err != nil {
		return errors.Wrap(err, "TransferChunks: get ring")
	}
	if v == nil {
		return fmt.Errorf("TransferChunks: ring not found when checking state of source ingester")
	}
	r, ok := v.(*ring.Desc)
	if !ok || r == nil {
		return fmt.Errorf("TransferChunks: ring not found, got %T", v)
	}

	if r.Ingesters == nil || r.Ingesters[fromIngesterID].State != ring.LEAVING {
		return fmt.Errorf("TransferChunks: source ingester is not in a LEAVING state, found state=%v", r.Ingesters[fromIngesterID].State)
	}

	if r.Ingesters == nil || r.Ingesters[fromIngesterID].State != ring.LEAVING {
		err = fmt.Errorf("source ingester is not in a LEAVING state, found state=%v", r.Ingesters[fromIngesterID].State)
		util.Logger.Log("msg", "TransferChunks error", "err", err)
		return err
	}

	// all fine
	return nil
}

func (i *Ingester) transfer(ctx context.Context, xfer func() error) error {
	// Enter JOINING state (only valid from PENDING)
	if err := i.lifecycler.ChangeState(ctx, ring.JOINING); err != nil {
		return err
	}

	// The ingesters state effectively works as a giant mutex around this whole
	// method, and as such we have to ensure we unlock the mutex.
	defer func() {
		state := i.lifecycler.GetState()
		if state == ring.ACTIVE {
			return
		}

		level.Error(util.Logger).Log("msg", "TransferChunks failed, not in ACTIVE state.", "state", state)

		// Enter PENDING state (only valid from JOINING)
		if state == ring.JOINING {
			if err := i.lifecycler.ChangeState(ctx, ring.PENDING); err != nil {
				level.Error(util.Logger).Log("msg", "error rolling back failed TransferChunks", "err", err)
				os.Exit(1)
			}
		}
	}()

	if err := xfer(); err != nil {
		return err
	}

	if err := i.lifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
		return errors.Wrap(err, "Transfer: ChangeState")
	}

	return nil
}

// TransferTSDB receives all the file chunks from another ingester, and writes them to tsdb directories
func (i *Ingester) TransferTSDB(stream client.Ingester_TransferTSDBServer) error {
	fromIngesterID := ""

	xfer := func() error {
		filesXfer := 0

		files := make(map[string]*os.File)
		defer func() {
			for _, f := range files {
				if err := f.Close(); err != nil {
					level.Warn(util.Logger).Log("msg", "failed to close xfer file", "err", err)
				}
			}
		}()
		for {
			f, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "TransferTSDB: Recv")
			}
			if fromIngesterID == "" {
				fromIngesterID = f.FromIngesterId
				level.Info(util.Logger).Log("msg", "processing TransferTSDB request", "from_ingester", fromIngesterID)

				// Before transfer, make sure 'from' ingester is in correct state to call ClaimTokensFor later
				err := i.checkFromIngesterIsInLeavingState(stream.Context(), fromIngesterID)
				if err != nil {
					return err
				}
			}
			filesXfer++

			// TODO(thor) To avoid corruption from errors, it's probably best to write to a temp dir, and then move that to the final location
			createfile := func(f *client.TimeSeriesFile) (*os.File, error) {
				dir := filepath.Join(i.cfg.TSDBConfig.Dir, filepath.Dir(f.Filename))
				if err := os.MkdirAll(dir, 0777); err != nil {
					return nil, errors.Wrap(err, "TransferTSDB: MkdirAll")
				}
				file, err := os.Create(filepath.Join(i.cfg.TSDBConfig.Dir, f.Filename))
				if err != nil {
					return nil, errors.Wrap(err, "TransferTSDB: Create")
				}

				_, err = file.Write(f.Data)
				return file, errors.Wrap(err, "TransferTSDB: Write")
			}

			// Create or get existing open file
			file, ok := files[f.Filename]
			if !ok {
				file, err = createfile(f)
				if err != nil {
					return err
				}

				files[f.Filename] = file
			} else {

				// Write to existing file
				if _, err := file.Write(f.Data); err != nil {
					return errors.Wrap(err, "TransferTSDB: Write")
				}
			}
		}

		if err := i.lifecycler.ClaimTokensFor(stream.Context(), fromIngesterID); err != nil {
			return errors.Wrap(err, "TransferTSDB: ClaimTokensFor")
		}

		receivedFiles.Add(float64(filesXfer))
		level.Error(util.Logger).Log("msg", "Total files xfer", "from_ingester", fromIngesterID, "num", filesXfer)

		return nil
	}

	if err := i.transfer(stream.Context(), xfer); err != nil {
		return err
	}

	// Close the stream last, as this is what tells the "from" ingester that
	// it's OK to shut down.
	if err := stream.SendAndClose(&client.TransferTSDBResponse{}); err != nil {
		level.Error(util.Logger).Log("msg", "Error closing TransferTSDB stream", "from_ingester", fromIngesterID, "err", err)
		return err
	}
	level.Info(util.Logger).Log("msg", "Successfully transferred tsdbs", "from_ingester", fromIngesterID)

	return nil
}

func toWireChunks(descs []*desc) ([]client.Chunk, error) {
	wireChunks := make([]client.Chunk, 0, len(descs))
	for _, d := range descs {
		wireChunk := client.Chunk{
			StartTimestampMs: int64(d.FirstTime),
			EndTimestampMs:   int64(d.LastTime),
			Encoding:         int32(d.C.Encoding()),
		}

		buf := bytes.NewBuffer(make([]byte, 0, d.C.Size()))
		if err := d.C.Marshal(buf); err != nil {
			return nil, err
		}

		wireChunk.Data = buf.Bytes()
		wireChunks = append(wireChunks, wireChunk)
	}
	return wireChunks, nil
}

func fromWireChunks(wireChunks []client.Chunk) ([]*desc, error) {
	descs := make([]*desc, 0, len(wireChunks))
	for _, c := range wireChunks {
		desc := &desc{
			FirstTime:  model.Time(c.StartTimestampMs),
			LastTime:   model.Time(c.EndTimestampMs),
			LastUpdate: model.Now(),
		}

		var err error
		desc.C, err = encoding.NewForEncoding(encoding.Encoding(byte(c.Encoding)))
		if err != nil {
			return nil, err
		}

		if err := desc.C.UnmarshalFromBuf(c.Data); err != nil {
			return nil, err
		}

		descs = append(descs, desc)
	}
	return descs, nil
}

// TransferOut finds an ingester in PENDING state and transfers our chunks to it.
// Called as part of the ingester shutdown process.
func (i *Ingester) TransferOut(ctx context.Context) error {
	if i.cfg.MaxTransferRetries < 0 {
		return fmt.Errorf("transfers disabled")
	}
	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
		MaxRetries: i.cfg.MaxTransferRetries,
	})

	for backoff.Ongoing() {
		err := i.transferOut(ctx)
		if err == nil {
			return nil
		}

		level.Error(util.Logger).Log("msg", "transfer failed", "err", err)
		backoff.Wait()
	}

	return backoff.Err()
}

func (i *Ingester) transferOut(ctx context.Context) error {
	if i.cfg.TSDBEnabled {
		return i.v2TransferOut(ctx)
	}

	userStatesCopy := i.userStates.cp()
	if len(userStatesCopy) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	targetIngester, err := i.findTargetIngester(ctx)
	if err != nil {
		return fmt.Errorf("cannot find ingester to transfer chunks to: %v", err)
	}

	level.Info(util.Logger).Log("msg", "sending chunks", "to_ingester", targetIngester.Addr)
	c, err := i.cfg.ingesterClientFactory(targetIngester.Addr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, "-1")
	stream, err := c.TransferChunks(ctx)
	if err != nil {
		return errors.Wrap(err, "TransferChunks")
	}

	_, err = i.pushChunks(pushChunksOptions{
		States:        userStatesCopy,
		Stream:        stream,
		DisallowFlush: true,
		SentChunks:    sentChunks,
	})
	if err != nil {
		return err
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "CloseAndRecv")
	}

	// Close & empty all the flush queues, to unblock waiting workers.
	for _, flushQueue := range i.flushQueues {
		flushQueue.DiscardAndClose()
	}
	i.flushQueuesDone.Wait()

	level.Info(util.Logger).Log("msg", "successfully sent chunks", "to_ingester", targetIngester.Addr)
	return nil
}

func (i *Ingester) v2TransferOut(ctx context.Context) error {
	if len(i.TSDBState.dbs) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	// Close all user databases
	wg := &sync.WaitGroup{}
	// Only perform a shutdown once
	once.Do(func() {
		wg.Add(len(i.TSDBState.dbs))
		for _, db := range i.TSDBState.dbs {
			go func(closer io.Closer) {
				defer wg.Done()
				if err := closer.Close(); err != nil {
					level.Warn(util.Logger).Log("msg", "failed to close db", "err", err)
				}
			}(db)
		}
	})

	targetIngester, err := i.findTargetIngester(ctx)
	if err != nil {
		return fmt.Errorf("cannot find ingester to transfer blocks to: %v", err)
	}

	level.Info(util.Logger).Log("msg", "sending blocks", "to_ingester", targetIngester.Addr)
	c, err := i.cfg.ingesterClientFactory(targetIngester.Addr, i.clientConfig)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx = user.InjectOrgID(ctx, "-1")
	stream, err := c.TransferTSDB(ctx)
	if err != nil {
		return errors.Wrap(err, "TransferTSDB")
	}

	wg.Wait() // wait for all databases to have closed

	// Grab a list of all blocks that need to be shipped
	blocks, err := unshippedBlocks(i.cfg.TSDBConfig.Dir)
	if err != nil {
		return err
	}

	for user, blockIDs := range blocks {
		// Transfer the users TSDB
		// TODO(thor) transferring users can be done concurrently
		transferUser(ctx, stream, i.cfg.TSDBConfig.Dir, i.lifecycler.ID, user, blockIDs)
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "CloseAndRecv")
	}

	return nil
}

// findTargetIngester finds an ingester in PENDING state.
func (i *Ingester) findTargetIngester(ctx context.Context) (*ring.IngesterDesc, error) {
	ringDesc, err := i.lifecycler.KVStore.Get(ctx, ring.ConsulKey)
	if err != nil {
		return nil, err
	}

	ingesters := ringDesc.(*ring.Desc).FindIngestersByState(ring.PENDING)
	if len(ingesters) <= 0 {
		return nil, fmt.Errorf("no pending ingesters")
	}

	return &ingesters[0], nil
}

// unshippedBlocks returns a ulid list of blocks that haven't been shipped
func unshippedBlocks(dir string) (map[string][]string, error) {
	userIDs, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	blocks := make(map[string][]string, len(userIDs))
	for _, user := range userIDs {
		userID := user.Name()
		blocks[userID] = []string{} // seed the map with the userID to ensure we xfer the WAL, even if all blocks are shipped

		blockIDs, err := ioutil.ReadDir(filepath.Join(dir, userID))
		if err != nil {
			return nil, err
		}

		m, err := shipper.ReadMetaFile(filepath.Join(dir, userID))
		if err != nil {
			return nil, err
		}

		shipped := make(map[string]bool)
		for _, u := range m.Uploaded {
			shipped[u.String()] = true
		}

		for _, blockID := range blockIDs {
			_, err := ulid.Parse(blockID.Name())
			if err != nil {
				continue
			}

			if _, ok := shipped[blockID.Name()]; !ok {
				blocks[userID] = append(blocks[userID], blockID.Name())
			}
		}
	}

	return blocks, nil
}

func transferUser(ctx context.Context, stream client.Ingester_TransferTSDBClient, dir, ingesterID, userID string, blocks []string) {
	level.Info(util.Logger).Log("msg", "xfer user", "user", userID)
	// Transfer all blocks
	for _, blk := range blocks {
		err := filepath.Walk(filepath.Join(dir, userID, blk), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}

			if info.IsDir() {
				return nil
			}

			b, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			p, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}

			if err := batchSend(1024*1024, b, stream, &client.TimeSeriesFile{
				FromIngesterId: ingesterID,
				UserId:         userID,
				Filename:       p,
			}); err != nil {
				return err
			}

			sentFiles.Add(1)
			return nil
		})
		if err != nil {
			level.Warn(util.Logger).Log("msg", "failed to transfer all user blocks", "err", err)
		}
	}

	// Transfer WAL
	err := filepath.Walk(filepath.Join(dir, userID, "wal"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if info.IsDir() {
			return nil
		}

		b, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		p, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		if err := batchSend(1024*1024, b, stream, &client.TimeSeriesFile{
			FromIngesterId: ingesterID,
			UserId:         userID,
			Filename:       p,
		}); err != nil {
			return err
		}

		sentFiles.Add(1)
		return nil
	})

	if err != nil {
		level.Warn(util.Logger).Log("msg", "failed to transfer user wal", "err", err)
	}

	level.Info(util.Logger).Log("msg", "xfer user complete", "user", userID)
}

func batchSend(batch int, b []byte, stream client.Ingester_TransferTSDBClient, tsfile *client.TimeSeriesFile) error {
	// Split file into smaller blocks for xfer
	i := 0
	for ; i+batch < len(b); i += batch {
		tsfile.Data = b[i : i+batch]
		err := stream.Send(tsfile)
		if err != nil {
			return err
		}
	}

	// Send final data
	if i < len(b) {
		tsfile.Data = b[i:]
		err := stream.Send(tsfile)
		if err != nil {
			return err
		}
	}

	return nil
}

// SendChunks accepts chunks from a client and moves them into the local Ingester.
func (i *Ingester) SendChunks(stream client.Ingester_SendChunksServer) error {
	i.userStatesMtx.Lock()
	defer i.userStatesMtx.Unlock()

	fromIngesterID, seriesReceived, err := i.acceptChunks(acceptChunksOptions{
		UserStates:     i.userStates,
		Stream:         stream,
		ReceivedChunks: incReceivedChunks,
		ReceivedSeries: incReceivedSeries,
	})
	if err != nil {
		return err
	}

	// Close the stream last, as this is what tells the "from" ingester that
	// it's OK to shut down.
	if err := stream.SendAndClose(&client.SendChunksResponse{}); err != nil {
		level.Error(util.Logger).Log("msg", "Error closing SendChunks stream", "from_ingester", fromIngesterID, "err", err)
		return err
	}

	// It's valid for an ingester not to have any tokens to send in a range,
	// just ignore it here.
	if seriesReceived == 0 {
		return nil
	}

	if fromIngesterID == "" {
		level.Error(util.Logger).Log("msg", "received TransferChunks request with no ID from ingester")
		return fmt.Errorf("no ingester id")
	}

	level.Debug(util.Logger).Log("msg", "Successfully received chunks", "from_ingester", fromIngesterID, "series_received", seriesReceived)
	return nil
}

// GetChunks accepts a get request from a client and sends all chunks from the serving ingester
// that fall within the given range to the client.
func (i *Ingester) GetChunks(req *client.GetChunksRequest, stream client.Ingester_GetChunksServer) error {
	userStatesCopy := i.userStates.cp()
	if len(userStatesCopy) == 0 {
		level.Info(util.Logger).Log("msg", "nothing to transfer")
		return nil
	}

	sent, err := i.pushChunks(pushChunksOptions{
		States:        userStatesCopy,
		Stream:        stream,
		DisallowFlush: req.Move,
		SentChunks:    incSentChunks,
		SentSeries:    incSentSeries,

		Filter: func(pair *fingerprintSeriesPair) bool {
			token := pair.series.token

			inRange := false
			for _, rg := range req.Ranges {
				if token >= rg.StartRange && token < rg.EndRange {
					inRange = true
					break
				}
			}
			return !inRange
		},
	})
	if err != nil {
		return err
	}

	level.Debug(util.Logger).Log(
		"msg", "sent chunks in range",
		"to_ingester", req.FromIngesterId,
		"ranges", ring.PrintableRanges(makeRingRanges(req.Ranges)),
		"sent_chunks", sent,
	)
	return nil
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
			token := pair.series.token

			inRange := false
			for _, rg := range ranges {
				if token >= rg.From && token < rg.To {
					inRange = true
					break
				}
			}
			return !inRange
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
