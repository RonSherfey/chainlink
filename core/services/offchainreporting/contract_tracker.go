package offchainreporting

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/eth"
	"github.com/smartcontractkit/chainlink/core/services/log"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/smartcontractkit/chainlink/core/utils"
	"github.com/smartcontractkit/libocr/gethwrappers/offchainaggregator"
	"github.com/smartcontractkit/libocr/offchainreporting/confighelper"
	"github.com/smartcontractkit/libocr/offchainreporting/types"
	ocrtypes "github.com/smartcontractkit/libocr/offchainreporting/types"
)

// configMailboxSanityLimit is the maximum number of configs that can be held
// in the mailbox. Under normal operation there should never be more than 0 or
// 1 configs in the mailbox, this limit is here merely to prevent unbounded usage
// in some kind of unforeseen insane situation.
const configMailboxSanityLimit = 100

var (
	_ ocrtypes.ContractConfigTracker = &OCRContractTracker{}
	_ log.Listener                   = &OCRContractTracker{}

	OCRContractConfigSet            = getEventTopic("ConfigSet")
	OCRContractLatestRoundRequested = getEventTopic("RoundRequested")
)

type (
	// OCRContractTracker complies with ContractConfigTracker interface and
	// handles log events related to the contract more generally
	OCRContractTracker struct {
		utils.StartStopOnce

		ethClient        eth.Client
		contractFilterer *offchainaggregator.OffchainAggregatorFilterer
		contractCaller   *offchainaggregator.OffchainAggregatorCaller
		contractAddress  gethCommon.Address
		logBroadcaster   log.Broadcaster
		jobID            int32
		logger           logger.Logger

		// processLogs worker
		wg     sync.WaitGroup
		chStop chan struct{}

		// LatestRoundRequested
		latestRoundRequested offchainaggregator.OffchainAggregatorRoundRequested
		lrrMu                sync.RWMutex

		// ContractConfig
		configsMB utils.Mailbox
		chConfigs chan ocrtypes.ContractConfig
	}
)

// NewOCRContractTracker makes a new OCRContractTracker
func NewOCRContractTracker(
	address gethCommon.Address,
	contractFilterer *offchainaggregator.OffchainAggregatorFilterer,
	contractCaller *offchainaggregator.OffchainAggregatorCaller,
	ethClient eth.Client,
	logBroadcaster log.Broadcaster,
	jobID int32,
	logger logger.Logger,
) (o *OCRContractTracker, err error) {
	return &OCRContractTracker{
		utils.StartStopOnce{},
		ethClient,
		contractFilterer,
		contractCaller,
		address,
		logBroadcaster,
		jobID,
		logger,
		sync.WaitGroup{},
		make(chan struct{}),
		offchainaggregator.OffchainAggregatorRoundRequested{},
		sync.RWMutex{},
		*utils.NewMailbox(configMailboxSanityLimit),
		make(chan ocrtypes.ContractConfig),
	}, nil
}

// Start must be called before logs can be delivered
func (t *OCRContractTracker) Start() (err error) {
	if !t.OkayToStart() {
		return errors.New("OCRContractTracker: already started")
	}
	connected := t.logBroadcaster.Register(t.contractAddress, t)
	if !connected {
		t.logger.Warnw("OCRContractTracker#Start: log broadcaster is not connected", "jobID", t.jobID, "address", t.contractAddress)
	}
	t.wg.Add(1)
	go t.processLogs()
	return nil
}

// Close should be called after teardown of the OCR job relying on this tracker
func (t *OCRContractTracker) Close() error {
	if !t.OkayToStop() {
		return errors.New("OCRContractTracker already stopped")
	}
	close(t.chStop)
	t.wg.Wait()
	t.logBroadcaster.Unregister(t.contractAddress, t)
	close(t.chConfigs)
	return nil
}

func (t *OCRContractTracker) processLogs() {
	defer t.wg.Done()
	for {
		select {
		case <-t.configsMB.Notify():
			// NOTE: libocr could take an arbitrary amount of time to process a
			// new config. To avoid blocking the log broadcaster, we use this
			// background thread to deliver them and a mailbox as the buffer.
			for {
				x := t.configsMB.Retrieve()
				if x == nil {
					break
				}
				cc := x.(types.ContractConfig)
				select {
				case t.chConfigs <- cc:
				case <-t.chStop:
					return
				}
			}
		case <-t.chStop:
			return
		}
	}
}

// OnConnect complies with LogListener interface
func (t *OCRContractTracker) OnConnect() {}

// OnDisconnect complies with LogListener interface
func (t *OCRContractTracker) OnDisconnect() {}

// HandleLog complies with LogListener interface
func (t *OCRContractTracker) HandleLog(lb log.Broadcast, err error) {
	if err != nil {
		t.logger.Errorw("OCRContract: error in previous LogListener", "err", err)
		return
	}

	// TODO: Transactional?
	was, err := lb.WasAlreadyConsumed()
	if err != nil {
		t.logger.Errorw("OCRContract: could not determine if log was already consumed", "error", err)
		return
	} else if was {
		return
	}

	topics := lb.RawLog().Topics
	if len(topics) == 0 {
		return
	}
	switch topics[0] {
	case OCRContractConfigSet:
		raw := lb.RawLog()
		if raw.Address != t.contractAddress {
			t.logger.Errorf("log address of 0x%x does not match configured contract address of 0x%x", raw.Address, t.contractAddress)
			return
		}
		var configSet *offchainaggregator.OffchainAggregatorConfigSet
		configSet, err = t.contractFilterer.ParseConfigSet(raw)
		if err != nil {
			t.logger.Errorw("could not parse config set", "err", err)
			return
		}
		configSet.Raw = lb.RawLog()
		cc := confighelper.ContractConfigFromConfigSetEvent(*configSet)

		// TODO: Use queue? Only necessary because libocr is opaque
		t.configsMB.Deliver(cc)
	case OCRContractLatestRoundRequested:
		// TODO: Needs tests
		raw := lb.RawLog()
		if raw.Address != t.contractAddress {
			t.logger.Errorf("log address of 0x%x does not match configured contract address of 0x%x", raw.Address, t.contractAddress)
			return
		}
		var rr *offchainaggregator.OffchainAggregatorRoundRequested
		rr, err = t.contractFilterer.ParseRoundRequested(raw)
		if err != nil {
			t.logger.Errorw("could not parse round requested", "err", err)
			return
		}
		t.lrrMu.Lock()
		if rr.Round >= t.latestRoundRequested.Round && rr.Epoch >= t.latestRoundRequested.Epoch {
			t.latestRoundRequested = *rr
		} else {
			t.logger.Warn("OCRContractTracker: ignoring out of date RoundRequested event", "latestRoundRequested", t.latestRoundRequested, "roundRequested", rr)
		}
		t.lrrMu.Unlock()
	default:
	}

	// TODO: Defer this? What if log parsing errors?
	err = lb.MarkConsumed()
	if err != nil {
		t.logger.Errorw("OCRContract: could not mark log consumed", "error", err)
		return
	}
}

// IsV2Job complies with LogListener interface
func (t *OCRContractTracker) IsV2Job() bool {
	return true
}

// JobIDV2 complies with LogListener interface
func (t *OCRContractTracker) JobIDV2() int32 {
	return t.jobID
}

// JobID complies with LogListener interface
func (t *OCRContractTracker) JobID() models.JobID {
	return models.NilJobID
}

// SubscribeToNewConfigs returns the tracker aliased as a ContractConfigSubscription
func (t *OCRContractTracker) SubscribeToNewConfigs(context.Context) (ocrtypes.ContractConfigSubscription, error) {
	return (*OCRContractConfigSubscription)(t), nil
}

// LatestConfigDetails queries the eth node
func (t *OCRContractTracker) LatestConfigDetails(ctx context.Context) (changedInBlock uint64, configDigest ocrtypes.ConfigDigest, err error) {
	opts := bind.CallOpts{Context: ctx, Pending: false}
	result, err := t.contractCaller.LatestConfigDetails(&opts)
	if err != nil {
		return 0, configDigest, errors.Wrap(err, "error getting LatestConfigDetails")
	}
	configDigest, err = ocrtypes.BytesToConfigDigest(result.ConfigDigest[:])
	if err != nil {
		return 0, configDigest, errors.Wrap(err, "error getting config digest")
	}
	return uint64(result.BlockNumber), configDigest, err
}

// ConfigFromLogs queries the eth node for logs for this contract
func (t *OCRContractTracker) ConfigFromLogs(ctx context.Context, changedInBlock uint64) (c ocrtypes.ContractConfig, err error) {
	q := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(changedInBlock)),
		ToBlock:   big.NewInt(int64(changedInBlock)),
		Addresses: []gethCommon.Address{t.contractAddress},
		Topics: [][]gethCommon.Hash{
			{OCRContractConfigSet},
		},
	}

	logs, err := t.ethClient.FilterLogs(ctx, q)
	if err != nil {
		return c, err
	}
	if len(logs) == 0 {
		return c, errors.Errorf("ConfigFromLogs: OCRContract with address 0x%x has no logs", t.contractAddress)
	}

	latest, err := t.contractFilterer.ParseConfigSet(logs[len(logs)-1])
	if err != nil {
		return c, errors.Wrap(err, "ConfigFromLogs failed to ParseConfigSet")
	}
	latest.Raw = logs[len(logs)-1]
	if latest.Raw.Address != t.contractAddress {
		return c, errors.Errorf("log address of 0x%x does not match configured contract address of 0x%x", latest.Raw.Address, t.contractAddress)
	}
	return confighelper.ContractConfigFromConfigSetEvent(*latest), err
}

// LatestBlockHeight queries the eth node for the most recent header
// TODO(sam): This could (should?) be optimised to use the head tracker
// https://www.pivotaltracker.com/story/show/177006717
func (t *OCRContractTracker) LatestBlockHeight(ctx context.Context) (blockheight uint64, err error) {
	h, err := t.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, err
	}
	if h == nil {
		return 0, errors.New("got nil head")
	}

	return uint64(h.Number), nil
}

// LatestRoundRequested returns the configDigest, epoch, and round from the latest
// RoundRequested event emitted by the contract. LatestRoundRequested may or may not
// return a result if the latest such event was emitted in a block b such that
// b.timestamp < tip.timestamp - lookback.
//
// If no event is found, LatestRoundRequested should return zero values, not an error.
// An error should only be returned if an actual error occurred during execution,
// e.g. because there was an error querying the blockchain or the database.
//
// As an optimization, this function may also return zero values, if no
// RoundRequested event has been emitted after the latest NewTransmission event.
func (t *OCRContractTracker) LatestRoundRequested(ctx context.Context, lookback time.Duration) (configDigest ocrtypes.ConfigDigest, epoch uint32, round uint8, err error) {
	// TODO: Use lookback
	// TODO: Optimise
	t.lrrMu.RLock()
	defer t.lrrMu.RUnlock()
	return t.latestRoundRequested.ConfigDigest, t.latestRoundRequested.Epoch, t.latestRoundRequested.Round, nil
}

func getEventTopic(name string) gethCommon.Hash {
	abi, err := abi.JSON(strings.NewReader(offchainaggregator.OffchainAggregatorABI))
	if err != nil {
		panic("could not parse OffchainAggregator ABI: " + err.Error())
	}
	event, exists := abi.Events[name]
	if !exists {
		panic(fmt.Sprintf("abi.Events was missing %s", name))
	}
	return event.ID
}
