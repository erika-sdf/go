package cmd

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/spf13/cobra"
	horizon "github.com/stellar/go/services/horizon/internal"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/ingest"
	nosql2 "github.com/stellar/go/services/horizon/nosql"
	"github.com/stellar/go/services/horizon/nosql/processors"
	"github.com/stellar/go/support/errors"
	hlog "github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"log"
	"strconv"
)

var nodbCmd = &cobra.Command{
	Use:   "nodb",
	Short: "run in no-db mode",
	Long:  "",
	RunE: func(cmd *cobra.Command, args []string) error {

		argsUInt32 := make([]uint32, 2)
		for i, arg := range args {
			if seq, err := strconv.ParseUint(arg, 10, 32); err != nil {
				continue
			} else {
				argsUInt32[i] = uint32(seq)
			}
		}

		boltDb := nosql2.NewBoltStore("my.db")
		boltDb.Open()
		for _, b := range nosql2.GetBuckets() {
			boltDb.CreateBucketIfNotExists(b)
		}
		defer boltDb.Close()

		if argsUInt32[0] != 0 && argsUInt32[1] != 0 {
			ledgerRanges := []history.LedgerRange{{StartSequence: argsUInt32[0], EndSequence: argsUInt32[1]}}
			log.Println("%+v", ledgerRanges)

			err := horizon.ApplyFlags(config, flags, horizon.ApplyOptions{RequireCaptiveCoreConfig: false, AlwaysIngest: true})
			if err != nil {
				return err
			}
			log.Println("%+v", config.CaptiveCoreBinaryPath)

			ingestConfig := ingest.Config{
				NetworkPassphrase:      config.NetworkPassphrase,
				HistoryArchiveURL:      config.HistoryArchiveURLs[0],
				CheckpointFrequency:    config.CheckpointFrequency,
				EnableCaptiveCore:      config.EnableCaptiveCoreIngestion,
				CaptiveCoreBinaryPath:  config.CaptiveCoreBinaryPath,
				RemoteCaptiveCoreURL:   config.RemoteCaptiveCoreURL,
				CaptiveCoreToml:        config.CaptiveCoreToml,
				CaptiveCoreStoragePath: config.CaptiveCoreStoragePath,
				StellarCoreCursor:      config.CursorName,
				StellarCoreURL:         config.StellarCoreURL,
				BoltStore:              boltDb,
			}

			system, systemErr := ingest.NewSystem(ingestConfig)
			if systemErr != nil {
				return systemErr
			}

			err = system.ReingestRange(ledgerRanges, reingestForce)
			if err != nil {
				if _, ok := errors.Cause(err).(ingest.ErrReingestRangeConflict); ok {
					return fmt.Errorf(`The range you have provided overlaps with Horizon's most recently ingested ledger.
It is not possible to run the reingest command on this range in parallel with
Horizon's ingestion system.
Either reduce the range so that it doesn't overlap with Horizon's ingestion system,
or, use the force flag to ensure that Horizon's ingestion system is blocked until
the reingest command completes.`)
				}

				return err
			}
			hlog.Info("Range run successfully!")
		}
		log.Println("%+v", config)
		config.BoltStore = boltDb

		/*app, err := horizon.NewNoSqlAppFromFlags(config, flags)
		if err != nil {
			return err
		}
		return app.ServeNoSql()*/

		app := nosql2.NewNoSqlApp(boltDb)
		return app.Run()

	},
}


var addIdxCmd = &cobra.Command{
	Use:   "addidx",
	Short: "add index",
	Long:  "",
	RunE: func(cmd *cobra.Command, args []string) error {

		boltDb, err := bolt.Open("my.db", 0600, nil)

		defer boltDb.Close()

		num := 0
        next := []byte{}
		k := []byte{}
		v := []byte{}
		for {
			err = boltDb.Update(func(tx *bolt.Tx) error {
				ledgerBucket := tx.Bucket([]byte(nosql2.LedgerMetaBucketName))
				txnBucket := tx.Bucket([]byte(nosql2.TxnToLedgerBucketName))

				c := ledgerBucket.Cursor()
				k, v = c.First()
				if len(next) != 0 {
					k, v = c.Seek(next)
					log.Println("seeking to ", nosql2.BaToI32(k))
				}
				//err := ledgerBucket.ForEach(func(k, v []byte) error {
				for k != nil {
					l := xdr.LedgerCloseMeta{}
					err := l.UnmarshalBinary(v)
					//log.Println(num, l.LedgerSequence())
					txnPro, err := processors.NewTransactionProcessor(l)
					if err != nil {
						return err
					}
					txns, err := txnPro.GetAllTxnsForLedger()
					if err != nil {
						return err
					}
					for _, t := range txns {
						log.Println(l.LedgerSequence(), t.TransactionWithoutLedger.ID, nosql2.IToBa(t.TransactionWithoutLedger.ID, 64))
						err = txnBucket.Put(nosql2.IToBa(t.TransactionWithoutLedger.ID, 64), k)
						if err != nil {
							return err
						}
					}
					k, v = c.Next()
					num += 1
					if num % 100 == 0 {
						next = k
						break
					}
				} //)
				return err
			})
			log.Println("wrote ", num, "index")
		}

        return err
	},
}

func init() {
	RootCmd.AddCommand(nodbCmd)
	RootCmd.AddCommand(addIdxCmd)
}
