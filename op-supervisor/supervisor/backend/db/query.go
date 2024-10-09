package db

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/entrydb"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/db/logs"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

func (db *ChainsDB) FindSealedBlock(chain types.ChainID, block eth.BlockID) (nextEntry entrydb.EntryIdx, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	logDB, ok := db.logDBs[chain]
	if !ok {
		return 0, fmt.Errorf("%w: %v", ErrUnknownChain, chain)
	}
	return logDB.FindSealedBlock(block)
}

// LatestBlockNum returns the latest fully-sealed block number that has been recorded to the logs db
// for the given chain. It does not contain safety guarantees.
// The block number might not be available (empty database, or non-existent chain).
func (db *ChainsDB) LatestBlockNum(chain types.ChainID) (num uint64, ok bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	logDB, knownChain := db.logDBs[chain]
	if !knownChain {
		return 0, false
	}
	return logDB.LatestSealedBlockNum()
}

func (db *ChainsDB) LocalUnsafe(chainID types.ChainID) (types.HeadPointer, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	eventsDB, ok := db.logDBs[chainID]
	if !ok {
		return types.HeadPointer{}, ErrUnknownChain
	}
	// TODO get tip of events DB
	return types.HeadPointer{}, nil
}

func (db *ChainsDB) CrossUnsafe(chainID types.ChainID) (types.HeadPointer, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result, ok := db.crossUnsafe[chainID]
	if !ok {
		return types.HeadPointer{}, ErrUnknownChain
	}
	return result, nil
}

func (db *ChainsDB) LocalSafe(chainID types.ChainID) (derivedFrom eth.BlockID, derived eth.BlockID, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	localDB, ok := db.localDBs[chainID]
	if !ok {
		return eth.BlockID{}, eth.BlockID{}, ErrUnknownChain
	}
	// TODO get tip of local DB
	return eth.BlockID{}, eth.BlockID{}, nil
}

func (db *ChainsDB) CrossSafe(chainID types.ChainID) (derivedFrom eth.BlockID, derived eth.BlockID, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	crossDB, ok := db.crossDBs[chainID]
	if !ok {
		return eth.BlockID{}, eth.BlockID{}, ErrUnknownChain
	}
	// TODO get tip of cross DB
	return eth.BlockID{}, eth.BlockID{}, nil
}

func (db *ChainsDB) Finalized(chainID types.ChainID) (eth.BlockID, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	finalizedL1 := db.finalizedL1
	derived, err := db.LastDerivedFrom(chainID, finalizedL1.ID())
	if err != nil {
		return eth.BlockID{}, fmt.Errorf("could not find what was last derived from the finalized L1 block")
	}
	return derived, nil
}

func (db *ChainsDB) LastDerivedFrom(chainID types.ChainID, derivedFrom eth.BlockID) (derived eth.BlockID, err error) {
	crossDB, ok := db.crossDBs[chainID]
	if !ok {
		return eth.BlockID{}, ErrUnknownChain
	}
	// TODO
	crossDB.LastDerived()
}

func (db *ChainsDB) DerivedFrom(chainID types.ChainID, derived eth.BlockID) (derivedFrom eth.BlockID, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	localDB, ok := db.localDBs[chainID]
	if !ok {
		return eth.BlockRef{}, ErrUnknownChain
	}
	// TODO
	localDB.DerivedFrom()
}

// Check calls the underlying logDB to determine if the given log entry exists at the given location.
// If the block-seal of the block that includes the log is known, it is returned. It is fully zeroed otherwise, if the block is in-progress.
func (db *ChainsDB) Check(chain types.ChainID, blockNum uint64, logIdx uint32, logHash common.Hash) (includedIn eth.BlockID, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	logDB, ok := db.logDBs[chain]
	if !ok {
		return eth.BlockID{}, fmt.Errorf("%w: %v", ErrUnknownChain, chain)
	}
	_, err := logDB.Contains(blockNum, logIdx, logHash)
	if err != nil {
		return eth.BlockID{}, err
	}
	// TODO fix this for cross-safe to work
	return eth.BlockID{}, nil
}

// Safest returns the strongest safety level that can be guaranteed for the given log entry.
// it assumes the log entry has already been checked and is valid, this function only checks safety levels.
// Cross-safety levels are all considered to be more safe than any form of local-safety.
func (db *ChainsDB) Safest(chainID types.ChainID, blockNum uint64, index uint32) (safest types.SafetyLevel, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if finalized, err := db.Finalized(chainID); err == nil {
		if finalized.Number >= blockNum {
			return types.Finalized, nil
		}
	}
	_, crossSafe, err := db.CrossSafe(chainID)
	if err != nil {
		return types.Invalid, err
	}
	if crossSafe.Number >= blockNum {
		return types.CrossSafe, nil
	}
	crossUnsafe, err := db.CrossUnsafe(chainID)
	if err != nil {
		return types.Invalid, err
	}
	if crossUnsafe.WithinRange(blockNum, index) {
		return types.CrossUnsafe, nil
	}
	_, localSafe, err := db.LocalSafe(chainID)
	if err != nil {
		return types.Invalid, err
	}
	if localSafe.Number >= blockNum {
		return types.LocalSafe, nil
	}
	return types.LocalUnsafe, nil
}

func (db *ChainsDB) IteratorStartingAt(chain types.ChainID, sealedNum uint64, logIndex uint32) (logs.Iterator, error) {
	logDB, ok := db.logDBs[chain]
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrUnknownChain, chain)
	}
	return logDB.IteratorStartingAt(sealedNum, logIndex)
}
