package pipeline

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum-optimism/optimism/op-chain-ops/deployer/broadcaster"
	"github.com/ethereum-optimism/optimism/op-chain-ops/genesis"

	"github.com/ethereum-optimism/optimism/op-chain-ops/deployer/opcm"
	"github.com/ethereum-optimism/optimism/op-chain-ops/deployer/state"
	"github.com/ethereum-optimism/optimism/op-chain-ops/foundry"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func DeployOPChain(ctx context.Context, env *Env, artifactsFS foundry.StatDirFs, intent *state.Intent, st *state.State, chainID common.Hash) error {
	lgr := env.Logger.New("stage", "deploy-opchain")

	if !shouldDeployOPChain(intent, st, chainID) {
		lgr.Info("opchain deployment not needed")
		return nil
	}

	lgr.Info("deploying OP chain", "id", chainID.Hex())

	thisIntent, err := intent.Chain(chainID)
	if err != nil {
		return fmt.Errorf("failed to get chain intent: %w", err)
	}

	input := opcm.DeployOPChainInput{
		OpChainProxyAdminOwner:  thisIntent.Roles.ProxyAdminOwner,
		SystemConfigOwner:       thisIntent.Roles.SystemConfigOwner,
		Batcher:                 thisIntent.Roles.Batcher,
		UnsafeBlockSigner:       thisIntent.Roles.UnsafeBlockSigner,
		Proposer:                thisIntent.Roles.Proposer,
		Challenger:              thisIntent.Roles.Challenger,
		BasefeeScalar:           1368,
		BlobBaseFeeScalar:       801949,
		L2ChainId:               chainID.Big(),
		OpcmProxy:               st.ImplementationsDeployment.OpcmProxyAddress,
		SaltMixer:               st.Create2Salt.String(), // passing through salt generated at state initialization
		GasLimit:                30_000_000,
		DisputeGameType:         1, // PERMISSIONED_CANNON Game Type
		DisputeAbsolutePrestate: common.HexToHash("0x038512e02c4c3f7bdaec27d00edf55b7155e0905301e1a88083e4e0a6764d54c"),
		DisputeMaxGameDepth:     73,
		DisputeSplitDepth:       30,
		DisputeClockExtension:   10800,  // 3 hours (input in seconds)
		DisputeMaxClockDuration: 302400, // 3.5 days (input in seconds)
	}

	var dco opcm.DeployOPChainOutput
	lgr.Info("deploying using existing OPCM", "address", st.ImplementationsDeployment.OpcmProxyAddress.Hex())
	bcaster, err := broadcaster.NewKeyedBroadcaster(broadcaster.KeyedBroadcasterOpts{
		Logger:  lgr,
		ChainID: big.NewInt(int64(intent.L1ChainID)),
		Client:  env.L1Client,
		Signer:  env.Signer,
		From:    env.Deployer,
	})
	if err != nil {
		return fmt.Errorf("failed to create broadcaster: %w", err)
	}
	dco, err = opcm.DeployOPChainRaw(
		ctx,
		env.L1Client,
		bcaster,
		env.Deployer,
		artifactsFS,
		input,
	)
	if err != nil {
		return fmt.Errorf("error deploying OP chain: %w", err)
	}

	st.Chains = append(st.Chains, &state.ChainState{
		ID:                                        chainID,
		ProxyAdminAddress:                         dco.OpChainProxyAdmin,
		AddressManagerAddress:                     dco.AddressManager,
		L1ERC721BridgeProxyAddress:                dco.L1ERC721BridgeProxy,
		SystemConfigProxyAddress:                  dco.SystemConfigProxy,
		OptimismMintableERC20FactoryProxyAddress:  dco.OptimismMintableERC20FactoryProxy,
		L1StandardBridgeProxyAddress:              dco.L1StandardBridgeProxy,
		L1CrossDomainMessengerProxyAddress:        dco.L1CrossDomainMessengerProxy,
		OptimismPortalProxyAddress:                dco.OptimismPortalProxy,
		DisputeGameFactoryProxyAddress:            dco.DisputeGameFactoryProxy,
		AnchorStateRegistryProxyAddress:           dco.AnchorStateRegistryProxy,
		AnchorStateRegistryImplAddress:            dco.AnchorStateRegistryImpl,
		FaultDisputeGameAddress:                   dco.FaultDisputeGame,
		PermissionedDisputeGameAddress:            dco.PermissionedDisputeGame,
		DelayedWETHPermissionedGameProxyAddress:   dco.DelayedWETHPermissionedGameProxy,
		DelayedWETHPermissionlessGameProxyAddress: dco.DelayedWETHPermissionlessGameProxy,
	})

	currentBlock, _ := env.L1Client.BlockNumber(ctx)
	block, _ := env.L1Client.BlockByNumber(ctx, big.NewInt(int64(currentBlock)))
	currentBlockHash := block.Hash()

	// If any of the implementations addresses (excluding OpcmProxy) are empty,
	// we need to set them using the addresses of the corresponding proxies.
	// The reason these might be empty is because we're only invoking DeployOPChain.s.sol as part of the pipeline.
	// TODO: Need to initialize 'mipsSingletonAddress' and 'preimageOracleSingletonAddress'
	setEIP1967ImplementationAddress(ctx, env.L1Client, dco.DelayedWETHPermissionedGameProxy, currentBlockHash, &st.ImplementationsDeployment.DelayedWETHImplAddress)
	setEIP1967ImplementationAddress(ctx, env.L1Client, dco.OptimismPortalProxy, currentBlockHash, &st.ImplementationsDeployment.OptimismPortalImplAddress)
	setEIP1967ImplementationAddress(ctx, env.L1Client, dco.SystemConfigProxy, currentBlockHash, &st.ImplementationsDeployment.SystemConfigImplAddress)
	setRDPImplementationAddress(ctx, env.L1Client, dco.AddressManager, &st.ImplementationsDeployment.L1CrossDomainMessengerImplAddress)
	setEIP1967ImplementationAddress(ctx, env.L1Client, dco.L1ERC721BridgeProxy, currentBlockHash, &st.ImplementationsDeployment.L1ERC721BridgeImplAddress)
	setEIP1967ImplementationAddress(ctx, env.L1Client, dco.L1StandardBridgeProxy, currentBlockHash, &st.ImplementationsDeployment.L1StandardBridgeImplAddress)
	setEIP1967ImplementationAddress(ctx, env.L1Client, dco.OptimismMintableERC20FactoryProxy, currentBlockHash, &st.ImplementationsDeployment.OptimismMintableERC20FactoryImplAddress)
	setEIP1967ImplementationAddress(ctx, env.L1Client, dco.DisputeGameFactoryProxy, currentBlockHash, &st.ImplementationsDeployment.DisputeGameFactoryImplAddress)

	if err := env.WriteState(st); err != nil {
		return err
	}

	return nil
}

func setRDPImplementationAddress(ctx context.Context, client *ethclient.Client, addressManager common.Address, implAddress *common.Address) {
	if *implAddress == (common.Address{}) {
		contract := opcm.NewContract(addressManager, client)
		address, _ := contract.GetAddressByName(ctx, "OVM_L1CrossDomainMessenger")
		*implAddress = address
	}
}

func setEIP1967ImplementationAddress(ctx context.Context, client *ethclient.Client, proxy common.Address, currentBlockHash common.Hash, implAddress *common.Address) {
	if *implAddress == (common.Address{}) {
		storageValue, _ := client.StorageAtHash(ctx, proxy, genesis.ImplementationSlot, currentBlockHash)
		*implAddress = common.HexToAddress(hex.EncodeToString(storageValue))
	}
}

func shouldDeployOPChain(intent *state.Intent, st *state.State, chainID common.Hash) bool {
	for _, chain := range st.Chains {
		if chain.ID == chainID {
			return false
		}
	}

	return true
}
