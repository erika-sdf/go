package nosql

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"io"
)

const (
	KeyValueBucketName    = "keyValue"
	LedgerMetaBucketName  = "ledgerMeta"
	TxnToLedgerBucketName = "txnToLedger"

	CompressedLedgerMetaBucketName = "ledgerMetaCompressed"

	LastIngestedLedgerKey = "lastIngestedLedger"

	AccountChangeBucketName = "accountChange"
)

func GetBuckets() []string {
	return []string{KeyValueBucketName, LedgerMetaBucketName, TxnToLedgerBucketName, CompressedLedgerMetaBucketName, AccountChangeBucketName}
}

type BoltStore struct {
	filename    string
	db          *bolt.DB
	Compression string
}

func NewBoltStore(filename string, compression string) *BoltStore {
	return &BoltStore{
		filename:    filename,
		Compression: compression,
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

func IToBa(i interface{}, numBits int) []byte {
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

func BaToI32(ba []byte) uint32 {
	if len(ba) == 0 {
		ba = []byte{0, 0, 0, 0}
	}
	return binary.LittleEndian.Uint32(ba)
}

func baToI64(ba []byte) uint64 {
	if len(ba) == 0 {
		ba = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	}
	return binary.LittleEndian.Uint64(ba)
}

func (b *BoltStore) CreateAllBuckets() error {
	for _, bucket := range GetBuckets() {
		err := b.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
	}
	return nil
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

func (b *BoltStore) getBucket(bucketName string) (*bolt.Bucket, error) {
	var bucket *bolt.Bucket
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket = tx.Bucket([]byte(bucketName))
		return nil
	})
	return bucket, err
}

func (b *BoltStore) WriteAccountChange(accountId string, accChange ingest.Change) error {
	bucket, err := b.getBucket(AccountChangeBucketName)
	if err != nil {
		return err
	}
	return b.writeChange(bucket, []byte(accountId), accChange)
}

func GetCompressionReader(g []byte, compression string) (io.ReadCloser, error) {
	var r io.ReadCloser
	var err error
	switch compression {
	case "lzw":
		r = lzw.NewReader(bytes.NewBuffer(g), lzw.LSB, 8)
	case "zlib":
		r, err = zlib.NewReader(bytes.NewBuffer(g))
	case "gzip":
		r, err = gzip.NewReader(bytes.NewBuffer(g))
	case "flate":
		r = flate.NewReader(bytes.NewBuffer(g))
	case "none":
		//log.Error("no compression")
	}
	return r, err
}

func GetCompressionWriter(g *bytes.Buffer, compression string) (io.WriteCloser, error) {
	var w io.WriteCloser
	var err error
	switch compression {
	case "lzw":
		w = lzw.NewWriter(g, lzw.LSB, 8)
	case "zlib":
		w = zlib.NewWriter(g)
	case "gzip":
		w = gzip.NewWriter(g)
	case "flate":
		w, err = flate.NewWriter(g, flate.DefaultCompression)
	case "none":
		//log.Error("no compression")
	}
	return w, err
}

func (b *BoltStore) GetAccount(id string) (*ingest.Change, error) {
	var a ingest.Change
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(AccountChangeBucketName))
		g := bucket.Get([]byte(id))
		if g == nil {
			return nil
		}
		decompressed, err := Decompress(g, b.Compression)
        /*r, err := GetCompressionReader(g, b.Compression)
		if r != nil {
			defer r.Close()
		}
		compress := make([]byte, 0, 10000000)
		if b.Compression == "none" || b.Compression == "" {
			compress = g
		} else {
			buf := make([]byte, 4096)
			i := 0
			for {
				n, err := r.Read(buf)
				copy(compress[i:i+n], buf)
				i = i + n
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Errorf("err: %v", err)
					return err
				}
			}
			compress = compress[:i]
		}*/
		log.Errorf("account id %+v", id)
		log.Errorf("account id %+v", []byte(id))
		log.Errorf("%+v", decompressed)
		dec := gob.NewDecoder(bytes.NewBuffer(decompressed))
		a = ingest.Change{}
		err = dec.Decode(&a)
		return err
	})
	return &a, err
}

func (b *BoltStore) writeChange(bucket *bolt.Bucket, key []byte, change ingest.Change) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		var cBytes bytes.Buffer
		enc := gob.NewEncoder(&cBytes)
		err := enc.Encode(change)
		log.Errorf("encoded: %+v %+v", change, cBytes.Bytes())
		if err != nil {
			return err
		}
		err = bucket.Put(key, cBytes.Bytes())
		return err
	})
}

func (b *BoltStore) WriteLedger(l xdr.LedgerCloseMeta) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(LedgerMetaBucketName))
		lBytes, err := l.MarshalBinary()
		if err != nil {
			return err
		}
		compressed, err := Compress(lBytes, b.Compression)
		if err != nil {
			return err
		}
		
		//log.Errorf("Writing ledger %v", l.LedgerSequence())
		//log.Errorf("%v: %v", l.LedgerSequence(), IToBa(l.LedgerSequence(), 32))
		err = bucket.Put(IToBa(l.LedgerSequence(), 32), compressed)
		return err
	})
}

func (b *BoltStore) GetLedger(id uint32) (xdr.LedgerCloseMeta, error) {
	l := xdr.LedgerCloseMeta{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(CompressedLedgerMetaBucketName))
		if b.Compression == "none" {
			bucket = tx.Bucket([]byte(LedgerMetaBucketName))
		}
		g := bucket.Get(IToBa(id, 32))
		if g == nil {
			return nil
		}
		compress, err := Decompress(g, b.Compression)
		/*r, err := GetCompressionReader(g, b.Compression)
		if r != nil {
			defer r.Close()
		}
		compress := make([]byte, 0, 10000000)
		if b.Compression == "none" {
			//log.Error("no compression - raw data")
			compress = g
		} else {

			buf := make([]byte, 4096)
			i := 0
			for {
				n, err := r.Read(buf)
				copy(compress[i:i+n], buf)

				//log.Errorf("%v", compress[i:i+n])
				i = i + n
				//log.Errorf("%d bytes read", i)
				if err == io.EOF {
					//log.Info("EOF")
					break
				}
				if err != nil {
					log.Errorf("err: %v", err)
					return err
				}

			}
			compress = compress[:i]
		}*/

		err = l.UnmarshalBinary(compress)
		return err
	})
	return l, err
}

func (b *BoltStore) GetLedgerFromTransaction(id int64) (uint32, error) {
	ledgerId := uint32(0)
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(TxnToLedgerBucketName))
		ledgerIdBa := b.Get(IToBa(id, 64))
		ledgerId = BaToI32(ledgerIdBa)
		return nil
	})

	return ledgerId, err
}

func (b *BoltStore) DeleteRangeAll(start, end int64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LedgerMetaBucketName))
		for l := start; l <= end; l++ {
			err := b.Delete(IToBa(l, 64))
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
		lastIngestedLedgerSeq = int32(BaToI32(lastIngestedLedger))
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint32(lastIngestedLedgerSeq), nil
}

func Compress(v []byte, cs string) ([]byte, error) {
	compressed := bytes.NewBuffer(make([]byte, 0, 10000000))
	w, err := GetCompressionWriter(compressed, cs)
	n, err := w.Write(v)
	w.Close()
	c := compressed.Bytes()[:n]
	return c, err
}

func Decompress(v []byte, cs string) ([]byte, error) {
	if cs == "none" || cs == "" {
		return v, nil
	}
	compress := make([]byte, 0, 10000000)
	var r io.ReadCloser
	var err error
	r, err = GetCompressionReader(v, cs)
	defer r.Close()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4096)
	i := 0
	for {
		n, err := r.Read(buf)
		//log.Println(n, "bytes read")
		copy(compress[i:i+n], buf)
		i = i + n
		if err == io.EOF {
			log.Infof("EOF; %d bytes written", i)
			break
		}
		if err != nil {
			log.Errorf("%d bytes written: %v", i, err)
			return nil, err
		}

	}
	return compress[:i], nil
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
