package derive

import (
	"context"
	"io"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum/log"
)

type ChannelStage struct {
	log     log.Logger
	spec    *rollup.ChainSpec
	metrics Metrics

	channel *Channel

	prev NextFrameProvider
}

var _ ResettableStage = (*ChannelStage)(nil)

// NewChannelStage creates a Holocene ChannelStage.
// It must only be used for derivation from Holocene activation.
func NewChannelStage(log log.Logger, cfg *rollup.Config, prev NextFrameProvider, m Metrics) *ChannelStage {
	return &ChannelStage{
		log:     log,
		spec:    rollup.NewChainSpec(cfg),
		metrics: m,
		prev:    prev,
	}
}

func (cs *ChannelStage) Log() log.Logger {
	return cs.log.New("stage", "channel", "origin", cs.Origin())
}

func (cs *ChannelStage) Origin() eth.L1BlockRef {
	return cs.prev.Origin()
}

func (cs *ChannelStage) Reset(context.Context, eth.L1BlockRef, eth.SystemConfig) error {
	cs.resetChannel()
	return io.EOF
}

func (cs *ChannelStage) resetChannel() {
	cs.channel = nil
}

// Returns whether the current staging channel is timed out. Panics if there's no current channel.
func (cs *ChannelStage) channelTimedOut() bool {
	return cs.channel.OpenBlockNumber()+cs.spec.ChannelTimeout(cs.Origin().Time) < cs.Origin().Number
}

func (cs *ChannelStage) NextData(ctx context.Context) ([]byte, error) {
	if cs.channel != nil && cs.channelTimedOut() {
		cs.metrics.RecordChannelTimedOut()
		cs.resetChannel()
	}

	lgr := cs.Log()
	origin := cs.Origin()

	// Note that if the current channel was already completed, we would have forwarded its data
	// already. So we start by reading in frames.
	if cs.channel != nil && cs.channel.IsReady() {
		panic("unexpected ready channel")
	}

	// ingest frames until we either hit an error (probably io.EOF) or complete a channel
	// TODO: ChannelBank uses a different pattern where it doesn't enter a loop but instead returns
	// NotEnoughData to cause the Driver to make a next step. I don't see right now why I can't just
	// eagerly drain the frame queue in a loop...
	for {
		frame, err := cs.prev.NextFrame(ctx)
		if err != nil { // includes io.EOF; a last frame broke the loop already
			return nil, err
		}

		// first frames always start a new channel, discarding an existing one
		if frame.FrameNumber == 0 {
			cs.metrics.RecordHeadChannelOpened()
			cs.channel = NewChannel(frame.ID, origin, true)
		}
		if frame.FrameNumber > 0 && cs.channel == nil {
			lgr.Warn("dropping non-first frame without channel",
				"frame_channel", frame.ID, "frame_number", frame.FrameNumber)
			continue // read more frames
		}

		// Catches Holocene ordering rules. Note that even though the frame queue is guaranteed to
		// only hold ordered frames in the current queue, it cannot guarantee this w.r.t. frames
		// that already got dequeued. So ordering has to be checked here again.
		if err := cs.channel.AddFrame(frame, origin); err != nil {
			lgr.Warn("failed to add frame to channel",
				"channel", cs.channel.ID(), "frame_channel", frame.ID,
				"frame_number", frame.FrameNumber, "err", err)
			continue // read more frames
		}
		cs.metrics.RecordFrame()

		if frame.IsLast {
			break // forward current complete channel
		}
	}

	ch := cs.channel
	// Note that if we exit the frame ingestion loop, we're guaranteed to have a ready channel.
	if ch == nil || !ch.IsReady() {
		panic("unexpected non-ready channel")
	}

	cs.resetChannel()
	r := ch.Reader()
	// error always nil, as we're reading from simple bytes readers
	data, _ := io.ReadAll(r)
	return data, nil
}
