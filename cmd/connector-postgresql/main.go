package main

import (
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/connector-postgresql/pkg/cmd/importer"
	"github.com/authzed/connector-postgresql/pkg/cmd/run"
	"github.com/authzed/connector-postgresql/pkg/signals"
	"github.com/authzed/connector-postgresql/pkg/streams"
)

func main() {
	s := streams.NewStdIO()
	ctx := signals.Context()
	rootCmd := &cobra.Command{
		Use:               "connector-postgresql",
		Short:             "Sync relationships from an external postgres into SpiceDB",
		PersistentPreRunE: cobrautil.SyncViperPreRunE("connector-postgresql"),
	}

	rootCmd.AddCommand(run.NewRunCmd(ctx, s))
	rootCmd.AddCommand(importer.NewImportCmd(ctx, s))
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal().Err(err)
	}
}
