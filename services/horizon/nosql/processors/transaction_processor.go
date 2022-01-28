package processors

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/guregu/null"
	"github.com/lib/pq"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/utf8"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/services/horizon/internal/toid"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type TransactionProcessor struct {
	reader       *ingest.LedgerTransactionReader
	transactions []history.Transaction
}

func NewTransactionProcessor(
	lcm xdr.LedgerCloseMeta,
) (*TransactionProcessor, error) {
	txnReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		"Public Global Stellar Network ; September 2015", lcm)
	if err != nil {
		err = errors.Wrap(err, "Error creating ledger reader")
		return nil, err
	}
	return &TransactionProcessor{
		transactions: []history.Transaction{},
		reader:       txnReader,
	}, nil
}

func (p *TransactionProcessor) GetTransaction() (map[string]interface{}, error) {
	err := Process(p.reader, p)
	if err != nil {
		return nil, err
	}
	return txnMap(p.transactions[0]), nil
}

func (p *TransactionProcessor) GetAllTxnsForLedger() ([]history.Transaction, error) {
	err := Process(p.reader, p)
	if err != nil {
		return nil, err
	}
	return p.transactions, nil
}

func (p *TransactionProcessor) GetTransactionsForLedger(limit int) (map[string]interface{}, error) {
	err := Process(p.reader, p)
	if err != nil {
		return nil, err
	}
	records := []map[string]interface{}{}
	for _, t := range p.transactions {
		records = append(records, txnMap(t))
	}
	/*if limit > 0 {
		num := int(math.Min(float64(len(records)), float64(limit+1)))
		records = records[:num]
	}*/
	r := map[string]interface{}{"records": records}
	return r, nil
}

func (p *TransactionProcessor) ProcessTransaction(transaction ingest.LedgerTransaction, sequence uint32) error {

	encodingBuffer := xdr.NewEncodingBuffer()

	envelopeBase64, err := encodingBuffer.MarshalBase64(transaction.Envelope)
	if err != nil {
		return err
	}
	resultBase64, err := encodingBuffer.MarshalBase64(&transaction.Result.Result)
	if err != nil {
		return err
	}
	metaBase64, err := encodingBuffer.MarshalBase64(transaction.UnsafeMeta)
	if err != nil {
		return err
	}
	feeMetaBase64, err := encodingBuffer.MarshalBase64(transaction.FeeChanges)
	if err != nil {
		return err
	}

	source := transaction.Envelope.SourceAccount()
	account := source.ToAccountId()
	var accountMuxed null.String
	if source.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		accountMuxed = null.StringFrom(source.Address())
	}
	t := history.TransactionWithoutLedger{
		TransactionHash:  hex.EncodeToString(transaction.Result.TransactionHash[:]),
		LedgerSequence:   int32(sequence),
		ApplicationOrder: int32(transaction.Index),
		Account:          account.Address(),
		AccountMuxed:     accountMuxed,
		AccountSequence:  strconv.FormatInt(transaction.Envelope.SeqNum(), 10),
		MaxFee:           int64(transaction.Envelope.Fee()),
		FeeCharged:       int64(transaction.Result.Result.FeeCharged),
		OperationCount:   int32(len(transaction.Envelope.Operations())),
		TxEnvelope:       envelopeBase64,
		TxResult:         resultBase64,
		TxMeta:           metaBase64,
		TxFeeMeta:        feeMetaBase64,
		TimeBounds:       formatTimeBounds(transaction),
		MemoType:         memoType(transaction),
		Memo:             memo(transaction),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
		Successful:       transaction.Result.Successful(),
	}
	t.TotalOrderID.ID = toid.New(int32(sequence), int32(transaction.Index), 0).ToInt64()

	if transaction.Envelope.IsFeeBump() {
		innerHash := transaction.Result.InnerHash()
		t.InnerTransactionHash = null.StringFrom(hex.EncodeToString(innerHash[:]))
		feeBumpAccount := transaction.Envelope.FeeBumpAccount()
		feeAccount := feeBumpAccount.ToAccountId()
		var feeAccountMuxed null.String
		if feeBumpAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
			feeAccountMuxed = null.StringFrom(feeBumpAccount.Address())
		}
		t.FeeAccount = null.StringFrom(feeAccount.Address())
		t.FeeAccountMuxed = feeAccountMuxed
		t.NewMaxFee = null.IntFrom(transaction.Envelope.FeeBumpFee())
		t.InnerSignatures = signatures(transaction.Envelope.Signatures())
		t.Signatures = signatures(transaction.Envelope.FeeBumpSignatures())
	} else {
		t.InnerTransactionHash = null.StringFromPtr(nil)
		t.FeeAccount = null.StringFromPtr(nil)
		t.FeeAccountMuxed = null.StringFromPtr(nil)
		t.NewMaxFee = null.IntFromPtr(nil)
		t.InnerSignatures = nil
		t.Signatures = signatures(transaction.Envelope.Signatures())
	}
	p.transactions = append(p.transactions, history.Transaction{TransactionWithoutLedger: t})

	return nil
}

func txnMap(txn history.Transaction) map[string]interface{} {

	t := txn.TransactionWithoutLedger
	return map[string]interface{}{
		"id":         t.TransactionHash,
		"hash":       t.TransactionHash,
		"successful": t.Successful,
		"ledger":     t.LedgerSequence,
		//"created_at": "2022-01-11T18:00:05Z",
		"source_account":          t.Account,
		"source_account_sequence": t.AccountSequence,
		"fee_account":             t.Account,
		"fee_charged":             fmt.Sprintf("%d", t.FeeCharged),
		"max_fee":                 fmt.Sprintf("%d", t.MaxFee),
		"operation_count":         t.OperationCount,
		"envelope_xdr":            t.TxEnvelope,
		"result_xdr":              t.TxResult,
		"result_meta_xdr":         t.TxMeta,
		"fee_meta_xdr":            t.TxFeeMeta,
		"memo_type":               t.MemoType,
		"signatures":              t.Signatures,
	}
}

func formatTimeBounds(transaction ingest.LedgerTransaction) history.TimeBounds {
	timeBounds := transaction.Envelope.TimeBounds()
	if timeBounds == nil {
		return history.TimeBounds{Null: true}
	}

	if timeBounds.MaxTime == 0 {
		return history.TimeBounds{
			Lower: null.IntFrom(int64(timeBounds.MinTime)),
		}
	}

	maxTime := timeBounds.MaxTime
	if maxTime > math.MaxInt64 {
		maxTime = math.MaxInt64
	}

	return history.TimeBounds{
		Lower: null.IntFrom(int64(timeBounds.MinTime)),
		Upper: null.IntFrom(int64(maxTime)),
	}
}

func signatures(xdrSignatures []xdr.DecoratedSignature) pq.StringArray {
	signatures := make([]string, len(xdrSignatures))
	for i, sig := range xdrSignatures {
		signatures[i] = base64.StdEncoding.EncodeToString(sig.Signature)
	}
	return signatures
}

func memoType(transaction ingest.LedgerTransaction) string {
	switch transaction.Envelope.Memo().Type {
	case xdr.MemoTypeMemoNone:
		return "none"
	case xdr.MemoTypeMemoText:
		return "text"
	case xdr.MemoTypeMemoId:
		return "id"
	case xdr.MemoTypeMemoHash:
		return "hash"
	case xdr.MemoTypeMemoReturn:
		return "return"
	default:
		panic(fmt.Errorf("invalid memo type: %v", transaction.Envelope.Memo().Type))
	}
}

func memo(transaction ingest.LedgerTransaction) null.String {
	var (
		value string
		valid bool
	)
	memo := transaction.Envelope.Memo()
	switch memo.Type {
	case xdr.MemoTypeMemoNone:
		value, valid = "", false
	case xdr.MemoTypeMemoText:
		scrubbed := utf8.Scrub(memo.MustText())
		notnull := strings.Join(strings.Split(scrubbed, "\x00"), "")
		value, valid = notnull, true
	case xdr.MemoTypeMemoId:
		value, valid = fmt.Sprintf("%d", memo.MustId()), true
	case xdr.MemoTypeMemoHash:
		hash := memo.MustHash()
		value, valid =
			base64.StdEncoding.EncodeToString(hash[:]),
			true
	case xdr.MemoTypeMemoReturn:
		hash := memo.MustRetHash()
		value, valid =
			base64.StdEncoding.EncodeToString(hash[:]),
			true
	default:
		panic(fmt.Errorf("invalid memo type: %v", memo.Type))
	}

	return null.NewString(value, valid)
}
