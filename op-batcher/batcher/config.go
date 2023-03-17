package batcher

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli"

	"github.com/ethereum-optimism/optimism/op-batcher/flags"
	"github.com/ethereum-optimism/optimism/op-batcher/rpc"
	"github.com/ethereum-optimism/optimism/op-node/eth"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	oppprof "github.com/ethereum-optimism/optimism/op-service/pprof"
	"github.com/ethereum-optimism/optimism/op-service/txmgr"
	opsigner "github.com/ethereum-optimism/optimism/op-signer/client"
)

// L1DataProvider is a minimal interface that allows the batch submitter to query
// the L1 chain, and is coalesced into a [txmgr.ETHBackend] for the [TransactionManager].
//
//go:generate mockery --name L1DataProvider --output ./mocks
type L1DataProvider interface {
	// BlockNumber returns the most recent block number.
	BlockNumber(ctx context.Context) (uint64, error)
	SuggestGasTipCap(ctx context.Context) (*big.Int, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error)
	BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error)
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
}

// L2DataProvider is a minimal interface that allows the batch submitter to query L2.
//
//go:generate mockery --name L2DataProvider --output ./mocks
type L2DataProvider interface {
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
}

// RollupNodeConfigProvider is a minimal interface that allows the batch submitter to query
// the rollup node.
//
//go:generate mockery --name RollupNodeConfigProvider --output ./mocks
type RollupNodeConfigProvider interface {
	SyncStatus(ctx context.Context) (*eth.SyncStatus, error)
}

type Config struct {
	log             log.Logger
	L1Client        L1DataProvider
	L2Client        L2DataProvider
	RollupNode      RollupNodeConfigProvider
	PollInterval    time.Duration
	TxManagerConfig txmgr.Config
	From            common.Address

	// RollupConfig is queried at startup
	Rollup *rollup.Config

	// Channel creation parameters
	Channel ChannelConfig
}

// Check ensures that the [Config] is valid.
func (c *Config) Check() error {
	if err := c.Rollup.Check(); err != nil {
		return err
	}
	if err := c.Channel.Check(); err != nil {
		return err
	}
	return nil
}

type CLIConfig struct {
	/* Required Params */

	// L1EthRpc is the HTTP provider URL for L1.
	L1EthRpc string

	// L2EthRpc is the HTTP provider URL for the L2 execution engine.
	L2EthRpc string

	// RollupRpc is the HTTP provider URL for the L2 rollup node.
	RollupRpc string

	// MaxChannelDuration is the maximum duration (in #L1-blocks) to keep a
	// channel open. This allows to more eagerly send batcher transactions
	// during times of low L2 transaction volume. Note that the effective
	// L1-block distance between batcher transactions is then MaxChannelDuration
	// + NumConfirmations because the batcher waits for NumConfirmations blocks
	// after sending a batcher tx and only then starts a new channel.
	//
	// If 0, duration checks are disabled.
	MaxChannelDuration uint64

	// The batcher tx submission safety margin (in #L1-blocks) to subtract from
	// a channel's timeout and sequencing window, to guarantee safe inclusion of
	// a channel on L1.
	SubSafetyMargin uint64

	// PollInterval is the delay between querying L2 for more transaction
	// and creating a new batch.
	PollInterval time.Duration

	// NumConfirmations is the number of confirmations which we will wait after
	// appending new batches.
	NumConfirmations uint64

	// SafeAbortNonceTooLowCount is the number of ErrNonceTooLowObservations
	// required to give up on a tx at a particular nonce without receiving
	// confirmation.
	SafeAbortNonceTooLowCount uint64

	// ResubmissionTimeout is time we will wait before resubmitting a
	// transaction.
	ResubmissionTimeout time.Duration

	// Mnemonic is the HD seed used to derive the wallet private keys for both
	// the sequence and proposer. Must be used in conjunction with
	// SequencerHDPath and ProposerHDPath.
	Mnemonic string

	// SequencerHDPath is the derivation path used to obtain the private key for
	// batched submission of sequencer transactions.
	SequencerHDPath string

	// PrivateKey is the private key used to submit sequencer transactions.
	PrivateKey string

	RPCConfig rpc.CLIConfig

	/* Optional Params */

	// MaxL1TxSize is the maximum size of a batch tx submitted to L1.
	MaxL1TxSize uint64

	// TargetL1TxSize is the target size of a batch tx submitted to L1.
	TargetL1TxSize uint64

	// TargetNumFrames is the target number of frames per channel.
	TargetNumFrames int

	// ApproxComprRatio is the approximate compression ratio (<= 1.0) of the used
	// compression algorithm.
	ApproxComprRatio float64

	Stopped bool

	LogConfig oplog.CLIConfig

	MetricsConfig opmetrics.CLIConfig

	PprofConfig oppprof.CLIConfig

	// SignerConfig contains the client config for op-signer service
	SignerConfig opsigner.CLIConfig
}

func (c CLIConfig) Check() error {
	if err := c.RPCConfig.Check(); err != nil {
		return err
	}
	if err := c.LogConfig.Check(); err != nil {
		return err
	}
	if err := c.MetricsConfig.Check(); err != nil {
		return err
	}
	if err := c.PprofConfig.Check(); err != nil {
		return err
	}
	if err := c.SignerConfig.Check(); err != nil {
		return err
	}
	return nil
}

// NewConfig parses the Config from the provided flags or environment variables.
func NewConfig(ctx *cli.Context) CLIConfig {
	return CLIConfig{
		/* Required Flags */
		L1EthRpc:                  ctx.GlobalString(flags.L1EthRpcFlag.Name),
		L2EthRpc:                  ctx.GlobalString(flags.L2EthRpcFlag.Name),
		RollupRpc:                 ctx.GlobalString(flags.RollupRpcFlag.Name),
		SubSafetyMargin:           ctx.GlobalUint64(flags.SubSafetyMarginFlag.Name),
		PollInterval:              ctx.GlobalDuration(flags.PollIntervalFlag.Name),
		NumConfirmations:          ctx.GlobalUint64(flags.NumConfirmationsFlag.Name),
		SafeAbortNonceTooLowCount: ctx.GlobalUint64(flags.SafeAbortNonceTooLowCountFlag.Name),
		ResubmissionTimeout:       ctx.GlobalDuration(flags.ResubmissionTimeoutFlag.Name),

		/* Optional Flags */
		MaxChannelDuration: ctx.GlobalUint64(flags.MaxChannelDurationFlag.Name),
		MaxL1TxSize:        ctx.GlobalUint64(flags.MaxL1TxSizeBytesFlag.Name),
		TargetL1TxSize:     ctx.GlobalUint64(flags.TargetL1TxSizeBytesFlag.Name),
		TargetNumFrames:    ctx.GlobalInt(flags.TargetNumFramesFlag.Name),
		ApproxComprRatio:   ctx.GlobalFloat64(flags.ApproxComprRatioFlag.Name),
		Stopped:            ctx.GlobalBool(flags.StoppedFlag.Name),
		Mnemonic:           ctx.GlobalString(flags.MnemonicFlag.Name),
		SequencerHDPath:    ctx.GlobalString(flags.SequencerHDPathFlag.Name),
		PrivateKey:         ctx.GlobalString(flags.PrivateKeyFlag.Name),
		RPCConfig:          rpc.ReadCLIConfig(ctx),
		LogConfig:          oplog.ReadCLIConfig(ctx),
		MetricsConfig:      opmetrics.ReadCLIConfig(ctx),
		PprofConfig:        oppprof.ReadCLIConfig(ctx),
		SignerConfig:       opsigner.ReadCLIConfig(ctx),
	}
}
