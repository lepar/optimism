package derive

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ethereum-optimism/optimism/op-service/eth"
)

type BatchStage struct {
	baseBatchStage
}

func (bs *BatchStage) FlushChannel() {
	bs.nextSpan = bs.nextSpan[:0]
	bs.prev.FlushChannel()
}

func (bs *BatchStage) NextBatch(ctx context.Context, parent eth.L2BlockRef) (*SingularBatch, bool, error) {
	// with Holocene, we can always update (and prune) the origins because we don't backwards-invalidate.
	bs.updateOrigins(parent)

	// if origin behind, we drain previous stage(s), and then return
	if bs.originBehind(parent) {
		if _, err := bs.prev.NextBatch(ctx); err != nil {
			// includes io.EOF and NotEnoughData
			return nil, false, err
		}
		// continue draining
		return nil, false, NotEnoughData
	}

	batch, err := bs.nextSingularBatchCandidate(ctx, parent)
	if err == io.EOF {
		empty, err := bs.deriveNextEmptyBatch(ctx, true, parent)
		return empty, false, err
	} else if err != nil {
		return nil, false, err
	}
	// parent epoch check (see deriveNextBatch)

	// len(bs.l1Blocks) > 2 check

	// check candidate validity
	validity := checkSingularBatch(bs.config, bs.Log(), bs.l1Blocks, parent, batch, bs.origin)
	switch validity {
	case BatchAccept: // continue
		batch.LogContext(bs.Log()).Debug("Found next singular batch")
		return batch, len(bs.nextSpan) == 0, nil
	case BatchPast:
		batch.LogContext(bs.Log()).Warn("Dropping past singular batch")
		// NotEnoughData to read in next batch until we're through all past batches
		return nil, false, NotEnoughData
	case BatchDrop: // drop, flush, move onto next channel
		batch.LogContext(bs.Log()).Warn("Dropping invalid singular batch, flushing channel")
		bs.FlushChannel()
		// NotEnoughData will cause derivation from previous stages until they're empty, at which
		// point empty batch derivation will happen.
		return nil, false, NotEnoughData
	case BatchUndecided: // l2 fetcher error, try again
		batch.LogContext(bs.Log()).Warn("Undecided span batch")
		return nil, false, NotEnoughData
	case BatchFuture: // panic, can't happen
		return nil, false, NewCriticalError(fmt.Errorf("impossible batch validity: %v", validity))
	default:
		return nil, false, NewCriticalError(fmt.Errorf("unknown batch validity type: %d", validity))
	}
}

func (bs *BatchStage) nextSingularBatchCandidate(ctx context.Context, parent eth.L2BlockRef) (*SingularBatch, error) {
	// First check for next span-derived batch
	nextBatch, _ := bs.nextFromSpanBatch(parent)

	if nextBatch != nil {
		return nextBatch, nil
	}

	// If the next batch is a singular batch, we forward it as the candidate.
	// If it is a span batch, we check its validity and then forward its first singular batch.
	batch, err := bs.prev.NextBatch(ctx)
	if err != nil { // includes io.EOF
		return nil, err
	}
	switch typ := batch.GetBatchType(); typ {
	case SingularBatchType:
		singularBatch, ok := batch.AsSingularBatch()
		if !ok {
			return nil, NewCriticalError(errors.New("failed type assertion to SingularBatch"))
		}
		return singularBatch, nil
	case SpanBatchType:
		spanBatch, ok := batch.AsSpanBatch()
		if !ok {
			return nil, NewCriticalError(errors.New("failed type assertion to SpanBatch"))
		}

		validity, _ := checkSpanBatchPrefix(ctx, bs.config, bs.Log(), bs.l1Blocks, parent, spanBatch, bs.origin, bs.l2)
		switch validity {
		case BatchAccept: // continue
			spanBatch.LogContext(bs.Log()).Info("Found next valid span batch")
		case BatchDrop: // drop, try next
			spanBatch.LogContext(bs.Log()).Warn("Dropping invalid span batch")
			return nil, NotEnoughData
		case BatchUndecided: // l2 fetcher error, try again
			spanBatch.LogContext(bs.Log()).Warn("Undecided span batch")
			return nil, NotEnoughData
		case BatchFuture: // can't happen with Holocene
			return nil, NewCriticalError(errors.New("impossible future batch validity"))
		}

		// If next batch is SpanBatch, convert it to SingularBatches.
		// TODO: maybe create iterator here instead, save to nextSpan
		// TODO: need to make sure this doesn't error where the iterator wouldn't,
		//   otherwise this wouldn't be correctly implementing partial span batch invalidation.
		//   From what I can tell, it is fine because the only error case is if the l1Blocks are
		//   missing a block, which would be a logic error. Although, if the node restarts mid-way
		//   through a span batch and the sync start only goes back one channel timeout from the
		//   mid-way safe block, it may actually miss l1 blocks! Need to check.
		//   We could fix this by fast-dropping past batches from the span batch.
		singularBatches, err := spanBatch.GetSingularBatches(bs.l1Blocks, parent)
		if err != nil {
			return nil, NewCriticalError(err)
		}
		bs.nextSpan = singularBatches
		// span-batches are non-empty, so the below pop is safe.
		return bs.popNextBatch(parent), nil
	default:
		return nil, NewCriticalError(fmt.Errorf("unrecognized batch type: %d", typ))
	}
}
