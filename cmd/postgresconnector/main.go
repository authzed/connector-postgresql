package main

import (
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/connector-postgres/pkg/cmd/config"
	"github.com/authzed/connector-postgres/pkg/cmd/importer"
	"github.com/authzed/connector-postgres/pkg/cmd/run"
	"github.com/authzed/connector-postgres/pkg/signals"
	"github.com/authzed/connector-postgres/pkg/streams"
)

func main() {
	s := streams.NewStdIO()
	ctx := signals.Context()
	rootCmd := &cobra.Command{
		Use:               "postgresconnector",
		Short:             "Sync relationships from an external postgres into SpiceDB",
		PersistentPreRunE: cobrautil.SyncViperPreRunE("postgresconnector"),
	}

	rootCmd.AddCommand(run.NewRunCmd(ctx, s))
	rootCmd.AddCommand(config.NewConfigCmd(ctx, s))
	rootCmd.AddCommand(importer.NewImportCmd(ctx, s))
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal().Err(err)
	}
}
