package processors

import (
	"encoding/hex"
	"fmt"
	"github.com/guregu/null"
	"io"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/services/horizon/internal/toid"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type LedgersProcessor struct {
	reader         *ingest.LedgerTransactionReader
	ledger         xdr.LedgerHeaderHistoryEntry
	successTxCount int
	failedTxCount  int
	opCount        int
	txSetOpCount   int
}

func NewLedgerProcessor(
	lcm xdr.LedgerCloseMeta,
) (*LedgersProcessor, error) {
	txnReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		"Public Global Stellar Network ; September 2015", lcm)
	if err != nil {
		err = errors.Wrap(err, "Error creating ledger reader")
		return nil, err
	}
	return &LedgersProcessor{
		ledger: txnReader.GetHeader(), //lcm.V0.LedgerHeader,
		reader: txnReader,
	}, nil
}

func (p *LedgersProcessor) GetLedger() (map[string]interface{}, error) {
	err := p.process()
	if err != nil {
		return nil, err
	}
	return ledgerMap(p.ledger, p.successTxCount, p.failedTxCount, p.opCount, p.txSetOpCount)
}


func (p *LedgersProcessor) process() error {
	for {
		tx, err := p.reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "could not read transaction")
		}
		if err = p.processTransaction(tx); err != nil {
			return errors.Wrapf(
				err,
				"could not process transaction %v",
				tx.Index,
			)
		}
	}
}

func (p *LedgersProcessor) processTransaction(transaction ingest.LedgerTransaction) error {
	opCount := len(transaction.Envelope.Operations())
	p.txSetOpCount += opCount
	if transaction.Result.Successful() {
		p.successTxCount++
		p.opCount += opCount
	} else {
		p.failedTxCount++
	}

	return nil
}

func toStroops(i xdr.Int64) string{
	return fmt.Sprintf("%d.%d", i/1e7, i %1e7)
}

func ledgerMap(ledger xdr.LedgerHeaderHistoryEntry,
	successTxsCount int,
	failedTxsCount int,
	opCount int,
	txSetOpCount int,
	//importerVersion int,
) (map[string]interface{}, error) {
	ledgerHeaderBase64, err := xdr.MarshalBase64(ledger.Header)
	if err != nil {
		return nil, err
	}
	closeTime := time.Unix(int64(ledger.Header.ScpValue.CloseTime), 0).UTC()
	return map[string]interface{}{
		//"importer_version":             importerVersion,
		"id":                           toid.New(int32(ledger.Header.LedgerSeq), 0, 0).ToInt64(),
		"sequence":                     ledger.Header.LedgerSeq,
		"hash":                  hex.EncodeToString(ledger.Hash[:]),
		"previous_ledger_hash":         null.NewString(hex.EncodeToString(ledger.Header.PreviousLedgerHash[:]), ledger.Header.LedgerSeq > 1),
		"total_coins":                  toStroops(ledger.Header.TotalCoins),
		"fee_pool":                     toStroops(ledger.Header.FeePool),
		"base_fee_in_stroops":                     ledger.Header.BaseFee,
		"base_reserve_in_stroops":                 ledger.Header.BaseReserve,
		"max_tx_set_size":              ledger.Header.MaxTxSetSize,
		"closed_at":                    closeTime,
		"created_at":                   time.Now().UTC(),
		"updated_at":                   time.Now().UTC(),
		"successful_transaction_count": successTxsCount,
		"failed_transaction_count":     failedTxsCount,
		"operation_count":              opCount,
		"tx_set_operation_count":       txSetOpCount,
		"protocol_version":             ledger.Header.LedgerVersion,
		"header_xdr":                ledgerHeaderBase64,
	}, nil
}

