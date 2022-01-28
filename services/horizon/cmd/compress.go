package cmd

import (
	"bytes"
	"compress/lzw"
	"compress/zlib"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/spf13/cobra"
	nosql2 "github.com/stellar/go/services/horizon/nosql"
	"io"
	"log"
	"strconv"
	"time"
)


func writeToDB(db *bolt.DB, k, v []byte ) error {
	err := db.Update(func(tx *bolt.Tx) error{
		ledgerCompressBucket := tx.Bucket([]byte(nosql2.CompressedLedgerMetaBucketName))
		err := ledgerCompressBucket.Put(k, v)
		return err
	})
	return err
}

var compressCmd = &cobra.Command{
	Use:   "compress",
	Short: "compress",
	Long:  "",
	RunE: func(cmd *cobra.Command, args []string) error {

		boltDb, err := bolt.Open("my.db", 0600, nil)
		defer boltDb.Close()

		compressDb, err := bolt.Open("my.db-zlib", 0600, nil)
		defer compressDb.Close()

		compressDb.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(nosql2.CompressedLedgerMetaBucketName))
			if err != nil {
				return fmt.Errorf("error creating bucket %s", err)
			}
			return nil
		})
		//return nil


		//bb := new(bytes.Buffer)
		//w := lzw.NewWriter(bb, lzw.MSB, 8)
		//r := lzw.NewReader(bb, lzw.LSB, 8)
		//_, err := r.Read(bytes.NewBuffer(v))
		num := 0
		next := []byte{}
		k := []byte{}
		v := []byte{}

		start := time.Now()
		for num < 15500 {
			err = boltDb.Update(func(tx *bolt.Tx) error {
				ledgerBucket := tx.Bucket([]byte(nosql2.LedgerMetaBucketName))

				c := ledgerBucket.Cursor()
				k, v = c.First()
				if len(next) != 0 {
					k, v = c.Seek(next)
					log.Println("seeking to ", nosql2.BaToI32(k))
				}

				for {
					compressed := new(bytes.Buffer)
					w := zlib.NewWriter(compressed)
					//w := lzw.NewWriter(compressed, lzw.LSB, 8)
					_, err := w.Write(v)
					w.Close()
					if err !=  nil {
						log.Println(err)
					}
					//log.Println(compressed.Bytes())
					if err != nil {
						return err
					}
					err = writeToDB(compressDb, k, compressed.Bytes())
					if err != nil {
						return err
					}
					k, v = c.Next()
					num += 1
					if num % 100 == 0 {
						next = k
						break
					}
				}
				return err
		     })

			log.Println("compressed ", num, " ledgers")
		}

		end := time.Now()
		log.Println("time elapsed ", end.Sub(start))
		return err
	},
}

var cmpCmd = &cobra.Command{
	Use:   "cmp",
	Short: "cmp",
	Long:  "",
	RunE: func(cmd *cobra.Command, args []string) error {

		argsUInt32 := make([]uint32, 1)
		for i, arg := range args {
			if seq, err := strconv.ParseUint(arg, 10, 32); err != nil {
				continue
			} else {
				argsUInt32[i] = uint32(seq)
			}
		}

		k := nosql2.IToBa(argsUInt32[0], 32)

		boltDb, err := bolt.Open("my.db", 0600, nil)
		if err != nil {
			return err
		}
		defer boltDb.Close()

		compressDb, err := bolt.Open("my.db-lzw-lsb", 0600, nil)
		defer compressDb.Close()

		var orig []byte//:= make([]byte, 1000000)
		compress := make([]byte, 0, 10000000)
		compressNew := bytes.NewBuffer(make([]byte, 0, 10000000))
        var c []byte
		err = boltDb.View(func(tx *bolt.Tx) error {
			ledgerBucket := tx.Bucket([]byte(nosql2.LedgerMetaBucketName))

			orig = ledgerBucket.Get(k)

			w := lzw.NewWriter(compressNew, lzw.LSB, 8)
			//w := zlib.NewWriter(compressNew)
			_, err := w.Write(orig)
			w.Close()
			if err !=  nil {
				log.Println(err)
			}
			c = compressNew.Bytes()
			//log.Println(compressNew.Bytes()[4200:4300])
			log.Println("compressed ", len(orig)," to ", len(c))

			return nil

		})
		err = compressDb.Update(func(tx *bolt.Tx) error {
			ledgerBucket := tx.Bucket([]byte(nosql2.CompressedLedgerMetaBucketName))
			//log.Println("updating ", len(compressNew.Bytes()), " bytes:", compressNew.Bytes()[4200:4300])
			err := ledgerBucket.Put(k, c)
			if err != nil {
				return err
			}
			return nil

		})
		err = compressDb.View(func(tx *bolt.Tx) error {
			ledgerBucket := tx.Bucket([]byte(nosql2.CompressedLedgerMetaBucketName))

			v := ledgerBucket.Get(k)
			r := lzw.NewReader(bytes.NewBuffer(v), lzw.LSB, 8)
			defer r.Close()
			//r, err := zlib.NewReader(bytes.NewBuffer(v))
			if err !=nil {
				return err
			}
			buf := make([]byte, 4096)
			i := 0
			for {
				n, err := r.Read(buf)
				//log.Println(n, "bytes read")
				copy(compress[i:i+n], buf)
				i = i+n
				if err == io.EOF {
					log.Println("EOF")
					break
				}
				if err != nil {
					log.Println(i, " bytes written")
					log.Println(err)
					return err
				}

			}
			return nil

		})

        SIZE := 100
		for i:=0; i < len(orig)-SIZE; i+=SIZE {
			res := bytes.Compare(orig[i:i+SIZE], compress[i:i+SIZE])
			if res != 0 {
				log.Println(orig[i:i+SIZE])
				log.Println(compress[i:i+SIZE])
				log.Println(i, "*********")
				return nil
			}
		}
		return nil
	},
}


func init() {
	RootCmd.AddCommand(compressCmd)
	RootCmd.AddCommand(cmpCmd)
}