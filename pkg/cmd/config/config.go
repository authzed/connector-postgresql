package config

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/connector-postgres/pkg/pgschema"
	"github.com/authzed/connector-postgres/pkg/streams"
	"github.com/authzed/connector-postgres/pkg/util"
)

// NewConfigCmd configures a new cobra command for generating configs based on
// an existing postgres instance.
func NewConfigCmd(ctx context.Context, streams streams.IO) *cobra.Command {
	o := NewOptions(streams)
	cmd := &cobra.Command{
		Use:   "config",
		Short: "generate a new mapping config based on a connected pg instance.",
		// logs to stderr so that stdout only contains the generated schema
		PreRunE: util.ZeroLogPreRunEFunc(o.IO.ErrOut),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			return o.Run(ctx)
		},
	}
	cmd.Flags().StringVar(&o.PostgresURI, "postgres", "", "address for the postgres endpoint")
	cobrautil.RegisterZeroLogFlags(cmd.Flags())

	return cmd
}

// Options holds options for the config generator
type Options struct {
	streams.IO

	PostgresURI string

	replogConfig *pgxpool.Config
}

// NewOptions returns initialized Options
func NewOptions(ioStreams streams.IO) *Options {
	return &Options{
		IO: ioStreams,
	}
}

// Complete fills out default values before running
func (o *Options) Complete() error {
	if o.PostgresURI == "" {
		return fmt.Errorf("must provide postgres uri or dsn")
	}

	// configure a (limited) connection for watching the replication log
	repcfg, err := pgxpool.ParseConfig(o.PostgresURI + "&replication=database")
	if err != nil {
		return err
	}
	// replication connections don't support extended query protocol
	repcfg.ConnConfig.PreferSimpleProtocol = true
	o.replogConfig = repcfg
	return nil
}

// Run runs the command configured by Options.
func (o *Options) Run(ctx context.Context) error {
	log.Info().EmbedObject(util.LoggedConnConfig{ConnConfig: o.replogConfig.ConnConfig}).Msg("connecting to postgres")

	replogConn, err := pgxpool.ConnectConfig(ctx, o.replogConfig)
	if err != nil {
		return err
	}
	defer replogConn.Close()

	log.Info().Msg("syncing schema")
	schema, err := pgschema.SyncSchema(ctx, replogConn)
	if err != nil {
		return err
	}

	for name, t := range schema.Tables {
		log.Debug().Stringer("XLogPos", schema.XLogPos).Str("table", name).Msgf("%#v", *t)
	}

	tableMap := schema.ToTableMapping()
	mapping, err := json.MarshalIndent(tableMap, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(o.Out, string(mapping))
	return err
}
