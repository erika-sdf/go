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

var nodbIngestCmd = &cobra.Command{
	Use:   "nodb-ingest",
	Short: "ingest",
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

		boltDb := nosql2.NewBoltStore("my.db-gzip", "gzip")
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
		return nil
	},
}

var nodbCmd = &cobra.Command{
	Use:   "nodb",
	Short: "serve in no-db mode",
	Long:  "",
	RunE: func(cmd *cobra.Command, args []string) error {

		compressionStrategy := args[0]
		dbName := fmt.Sprintf("my.db-%s", compressionStrategy)
		if compressionStrategy == "none" {
			dbName = "my.db"
		}
		boltDb := nosql2.NewBoltStore(dbName, compressionStrategy)
		boltDb.Open()
		boltDb.CreateAllBuckets()
		defer boltDb.Close()

		config.BoltStore = boltDb

		app := nosql2.NewNoSqlApp(boltDb)
		return app.Run()

	},
}

var addIdxCmd = &cobra.Command{
	Use:   "addidx",
	Short: "add index",
	Long:  "",
	RunE: func(cmd *cobra.Command, args []string) error {
		compressionStrategy := args[0]
		dbName := fmt.Sprintf("my.db-%s", compressionStrategy)
		boltDb, err := bolt.Open(dbName, 0600, nil)
		if err != nil {
			return nil
		}

		defer boltDb.Close()

		num := 0
		next := []byte{}
		k := []byte{}
		v := []byte{}
		for num < 15500 {
			err = boltDb.Update(func(tx *bolt.Tx) error {
				ledgerBucket := tx.Bucket([]byte(nosql2.CompressedLedgerMetaBucketName)) //LedgerMetaBucketName))
				txnBucket := tx.Bucket([]byte(nosql2.TxnToLedgerBucketName))

				c := ledgerBucket.Cursor()
				k, v = c.First()
				if len(next) != 0 {
					k, v = c.Seek(next)
					log.Println("seeking to ", nosql2.BaToI32(k))
				}
				for k != nil {

					dcmp, err := nosql2.Decompress(v, compressionStrategy)
					if err != nil {
						return err
					}
					l := xdr.LedgerCloseMeta{}
					err = l.UnmarshalBinary(dcmp)
					if err != nil {
						return err
					}

					txnPro, err := processors.NewTransactionProcessor(l)
					if err != nil {
						return err
					}
					txns, err := txnPro.GetAllTxnsForLedger()
					if err != nil {
						return err
					}
					for _, t := range txns {
						//err = txnBucket.Put(nosql2.IToBa(t.TransactionWithoutLedger.ID, 64), k)
						err = txnBucket.Put([]byte(t.TransactionWithoutLedger.TransactionHash), k)
						if err != nil {
							return err
						}
					}
					//log.Println("processed ", len(txns), " txns in ledger ",  l.LedgerSequence())

					k, v = c.Next()
					num += 1
					if num%100 == 0 {
						next = k
						log.Println("processed ", num, " ledger indexes")
						break
					}
				}
				return err
			})
			log.Println("wrote ", num, "index")
		}
		log.Fatal(err)
		return err
	},
}

var ingestStateCmd = &cobra.Command{
	Use:   "ingst",
	Short: "ingst",
	Long:  "current state at ledger arg[0]",
	RunE: func(cmd *cobra.Command, args []string) error {
		dbName := "my.db"
		compressionStrategy := "none"
		if len(args) > 1 {
			compressionStrategy = args[1]
			if len(compressionStrategy) > 0 {
				dbName = fmt.Sprintf("my.db-%s", compressionStrategy)
			}
		}
		ledgerArg := args[0]
		ledgerNumber, err := strconv.Atoi(ledgerArg)
		if err != nil {
			return err
		}
		boltDb := nosql2.NewBoltStore(dbName, compressionStrategy)
		boltDb.Open()
		for _, b := range nosql2.GetBuckets() {
			boltDb.CreateBucketIfNotExists(b)
		}
		defer boltDb.Close()

		err = horizon.ApplyFlags(config, flags, horizon.ApplyOptions{RequireCaptiveCoreConfig: false, AlwaysIngest: true})
		if err != nil {
			return err
		}

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
		err = system.BuildStateAtLedger(uint32(ledgerNumber))
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	RootCmd.AddCommand(nodbIngestCmd)
	RootCmd.AddCommand(nodbCmd)
	RootCmd.AddCommand(addIdxCmd)
	RootCmd.AddCommand(ingestStateCmd)
}
