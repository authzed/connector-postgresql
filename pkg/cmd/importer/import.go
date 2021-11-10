package importer

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/connector-postgresql/pkg/importer"
	"github.com/authzed/connector-postgresql/pkg/options"
	"github.com/authzed/connector-postgresql/pkg/streams"
	"github.com/authzed/connector-postgresql/pkg/util"
	"github.com/authzed/connector-postgresql/pkg/write"
)

// NewImportCmd configures a new cobra command that imports data from postgres
func NewImportCmd(ctx context.Context, streams streams.IO) *cobra.Command {
	o := NewOptions(streams)
	cmd := &cobra.Command{
		Use:     "import <postgres uri>",
		Short:   "import data from postgres into spicedb",
		Example: "  connector-postgresql import --spicedb-endpoint=localhost:50051 --spicedb-token=somesecretkeyhere --spicedb-insecure=true \"postgres://postgres:secret@localhost:5432/mydb?sslmode=disable\" ",
		PreRunE: util.ZeroLogPreRunEFunc(o.IO.ErrOut),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(ctx, args); err != nil {
				return err
			}
			return o.Run(ctx)
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.Flags().StringVar(&o.SpiceDBEndpoint, "spicedb-endpoint", "localhost:50051", "address for the SpiceDB endpoint")
	cmd.Flags().StringVar(&o.SpiceDBToken, "spicedb-token", "", "token for reading and writing to SpiceDB")
	cmd.Flags().BoolVar(&o.SpiceDBInsecure, "spicedb-insecure", false, "connect to SpiceDB without TLS")
	cmd.Flags().BoolVar(&o.DryRun, "dry-run", true, "print relationships that would be written to SpiceDB")
	cmd.Flags().StringVar(&o.MappingFile, "config", "", "path to a file containing the config that maps between pg tables and spicedb relationships")
	cmd.Flags().BoolVar(&o.AppendSchema, "append-schema", true, "append the config's (zed) schema to the schema in spicedb")
	cobrautil.RegisterZeroLogFlags(cmd.Flags(), "log")

	return cmd
}

// Options holds options for the import command
type Options struct {
	streams.IO
	options.PostgresOptions
	options.SpiceDBOptions
	options.ConfigOptions

	DryRun       bool
	AppendSchema bool

	AppendSchemaWriter write.AppendSchemaWriter
	RelationshipWriter write.RelationshipWriter
}

// NewOptions returns initialized Options
func NewOptions(ioStreams streams.IO) *Options {
	return &Options{
		IO: ioStreams,
	}
}

// Complete fills out default values before running
func (o *Options) Complete(ctx context.Context, args []string) error {
	if len(args) == 1 {
		o.PostgresURI = args[0]
	}

	if err := o.PostgresOptions.Complete(); err != nil {
		return err
	}

	if err := o.SpiceDBOptions.Complete(o.DryRun); err != nil {
		return err
	}

	if err := o.ConfigOptions.Complete(ctx, o.ReplogConfig, o.IO); err != nil {
		return err
	}

	if o.DryRun {
		log.Warn().Msg("Running in dry-run mode. No schema or relationships will be written to SpiceDB.")
		o.RelationshipWriter = write.NewDryRunRelationshipWriter()
		o.AppendSchemaWriter = write.NewDryRunSchemaAppendWriter(o.Client)
		return nil
	}

	o.AppendSchemaWriter = write.NewSchemaAppendWriter(o.Client, !o.AppendSchema)
	o.RelationshipWriter = write.NewRelationshipWriter(o.Client)
	return nil
}

// Run runs the command configured by Options.
func (o *Options) Run(ctx context.Context) error {
	log.Info().EmbedObject(util.LoggedConnConfig{ConnConfig: o.PoolConfig.ConnConfig}).Msg("connecting to postgres")

	conn, err := pgxpool.ConnectConfig(ctx, o.PoolConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := o.AppendSchemaWriter.Write(ctx, o.Config.Schema); err != nil {
		return err
	}

	if err := importer.NewPostgresImporter(conn, o.RelationshipWriter, o.Config.Tables).Import(ctx); err != nil {
		return err
	}

	return o.ConfigPrinter(o.Config)
}
