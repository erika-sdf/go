package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	horizon "github.com/stellar/go/services/horizon/internal"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/ingest"
	nosql2 "github.com/stellar/go/services/horizon/nosql"
	"github.com/stellar/go/support/errors"
	hlog "github.com/stellar/go/support/log"
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

		bolt := nosql2.NewBoltStore("my.db")
		bolt.Open()
		bolt.CreateBucketIfNotExists(nosql2.KeyValueBucketName)
		bolt.CreateBucketIfNotExists(nosql2.LedgerMetaBucketName)
		defer bolt.Close()

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
				BoltStore:              bolt,
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
		config.BoltStore = bolt

		/*app, err := horizon.NewNoSqlAppFromFlags(config, flags)
		if err != nil {
			return err
		}
		return app.ServeNoSql()*/

		app := nosql2.NewNoSqlApp(bolt)
		return app.Run()

	},
}

func init() {
	RootCmd.AddCommand(nodbCmd)
}
