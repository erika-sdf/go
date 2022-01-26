package nosql

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

const (
	KeyValueBucketName   = "keyValue"
	LedgerMetaBucketName = "ledgerMeta"

	LastIngestedLedgerKey = "lastIngestedLedger"
)

type BoltStore struct {
	filename string
	db       *bolt.DB
}

func NewBoltStore(filename string) *BoltStore {
	return &BoltStore{
		filename: filename,
	}
}

func (b *BoltStore) Open() {
	var err error
	b.db, err = bolt.Open(b.filename, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (b *BoltStore) Close() {
	b.db.Close()
}

func iToBa(i interface{}, numBits int) []byte {
	ba := make([]byte, numBits/8)
	switch i.(type) {
	case int32:
		idx := uint32(i.(int32))
		binary.LittleEndian.PutUint32(ba, idx)
	case uint32:
		binary.LittleEndian.PutUint32(ba, i.(uint32))
	case int64:
		v := i.(int64)
		binary.LittleEndian.PutUint64(ba, uint64(v))
	default:
		log.Errorf("type %T not found", i)
	}
	return ba
}

func baToI(ba []byte) uint32 {
	if len(ba) == 0 {
		ba = []byte{0, 0, 0, 0}
	}
	return binary.LittleEndian.Uint32(ba)
}

func (b *BoltStore) CreateBucketIfNotExists(bucketName string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("error creating bucket %s", err)
		}
		return nil
	})
}

func (b *BoltStore) WriteLedger(l xdr.LedgerCloseMeta) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LedgerMetaBucketName))
		lBytes, err := l.MarshalBinary()
		if err != nil {
			return err
		}
		log.Errorf("Writing ledger %v", l.LedgerSequence())
		log.Errorf("%v: %v", l.LedgerSequence(), iToBa(l.LedgerSequence(), 32))
		err = b.Put(iToBa(l.LedgerSequence(), 32), lBytes)
		return err
	})
}

func (b *BoltStore) GetLedger(id uint32) (xdr.LedgerCloseMeta, error) {
	l := xdr.LedgerCloseMeta{}
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LedgerMetaBucketName))
		log.Errorf("%v: %v", id, iToBa(id, 32))
		g := b.Get(iToBa(id, 32))
		err := l.UnmarshalBinary(g)
		return err
	})
	return l, err
}

func (b *BoltStore) DeleteRangeAll(start, end int64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LedgerMetaBucketName))
		for l := start; l <= end; l++ {
			err := b.Delete(iToBa(l, 64))
			return err
		}
		return nil
	})
}

func (b *BoltStore) GetLastLedgerIngestNonBlocking() (uint32, error) {
	lastIngestedLedgerSeq := int32(0)
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(KeyValueBucketName))
		lastIngestedLedger := b.Get([]byte(LastIngestedLedgerKey))
		lastIngestedLedgerSeq = int32(baToI(lastIngestedLedger))
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint32(lastIngestedLedgerSeq), nil
}


//
//func (b *BoltStore) LatestLedgerSequenceClosedAt() (int32, time.Time, error) {
//	//l := &xdr.LedgerCloseMeta{}
//	l := history.Ledger{}
//	err := b.db.View(func(tx *bolt.Tx) error {
//		b := tx.Bucket([]byte(LedgerMetaBucketName))
//		lastIngestedLedger := b.Get([]byte())
//		l = int32(baToI(lastIngestedLedger))
//		return nil
//	})
//	if err != nil {
//		return 0, err
//	}
//	return l.Sequence, l.ClosedAt, nil
//}

//err := q.GetRaw(ctx, &ledger, `SELECT sequence, closed_at FROM history_ledgers ORDER BY sequence DESC LIMIT 1`)
