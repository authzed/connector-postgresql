package run

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/connector-postgresql/pkg/cache"
	importercmd "github.com/authzed/connector-postgresql/pkg/cmd/importer"
	"github.com/authzed/connector-postgresql/pkg/follow"
	"github.com/authzed/connector-postgresql/pkg/importer"
	"github.com/authzed/connector-postgresql/pkg/pgschema"
	"github.com/authzed/connector-postgresql/pkg/streams"
	"github.com/authzed/connector-postgresql/pkg/util"
)

// NewRunCmd configures a new cobra command that both imports (backfills) data
// from a postgres instance and watches the WAL to sync data continuously
func NewRunCmd(ctx context.Context, streams streams.IO) *cobra.Command {
	o := NewOptions(streams)
	cmd := &cobra.Command{
		Use:     "run <postgres uri>",
		Short:   "import data from postgres into spicedb and continue to sync changes by following the replication log",
		Example: "  connector-postgresqlsql run --spicedb-endpoint=localhost:50051 --spicedb-token=somesecretkeyhere --spicedb-insecure=true \"postgres://postgres:secret@localhost:5432/mydb?sslmode=disable\" ",
		PreRunE: util.ZeroLogPreRunEFunc(o.IO.Out),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(ctx, args); err != nil {
				return err
			}
			return o.Run(ctx)
		},
	}
	cmd.Flags().StringVar(&o.SpiceDBEndpoint, "spicedb-endpoint", "localhost:50051", "address for the SpiceDB endpoint")
	cmd.Flags().StringVar(&o.SpiceDBToken, "spicedb-token", "", "token for reading and writing to SpiceDB")
	cmd.Flags().BoolVar(&o.SpiceDBInsecure, "spicedb-insecure", false, "connect to SpiceDB without TLS")
	cmd.Flags().BoolVar(&o.DryRun, "dry-run", true, "print relationships that would be written to SpiceDB")
	cmd.Flags().StringVar(&o.MappingFile, "config", "", "path to a file containing the config that maps between pg tables and spicedb relationships")
	cmd.Flags().BoolVar(&o.AppendSchema, "append-schema", true, "append the config's (zed) schema to the schema in spicedb")
	cmd.Flags().StringVar(&o.MetricsAddr, "metrics-addr", ":9090", "address that will serve prometheus data (default: :9090")
	cobrautil.RegisterZeroLogFlags(cmd.Flags(), "log")

	return cmd
}

// Options holds options for the postgres connector
type Options struct {
	importercmd.Options

	MetricsAddr string
}

// NewOptions returns initialized Options
func NewOptions(ioStreams streams.IO) *Options {
	return &Options{
		Options: importercmd.Options{
			IO: ioStreams,
		},
	}
}

// Complete fills out default values before running
func (o *Options) Complete(ctx context.Context, args []string) error {
	if err := o.Options.Complete(ctx, args); err != nil {
		return err
	}

	// TODO: metrics server
	return nil
}

// Run does a backfill and then watches for changes
func (o *Options) Run(ctx context.Context) error {
	log.Info().EmbedObject(util.LoggedConnConfig{ConnConfig: o.PoolConfig.ConnConfig}).Msg("connecting to postgres")

	conn, err := pgxpool.ConnectConfig(ctx, o.PoolConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	pgImport := importer.NewPostgresImporter(conn, o.RelationshipWriter, o.Config.Tables)
	if err := pgImport.Import(ctx); err != nil {
		return err
	}

	replogConn, err := pgxpool.ConnectConfig(ctx, o.ReplogConfig)
	if err != nil {
		return err
	}
	defer replogConn.Close()

	repCache := cache.NewCache(ctx)
	log.Info().Msg("syncing schema")
	schema, err := pgschema.SyncSchema(ctx, replogConn)
	if err != nil {
		return err
	}

	for _, t := range schema.Tables {
		log.Debug().Stringer("XLogPos", schema.XLogPos).Str("table", t.Name).Msgf("%#v", *t)
	}

	repconn, err := replogConn.Acquire(ctx)
	if err != nil {
		return err
	}
	defer repconn.Release()

	follower := follow.NewWalFollower(repconn.Conn().PgConn(), schema.InternalMapping(o.Config.Tables), repCache)

	go func() {
		// TODO: separate contexts - killing the existing ctx will kill the connection
		// before the slots can be dropped
		if err := follower.Follow(ctx, schema.XLogPos); err != nil {
			log.Info().Err(err).Msg("stopped")
		}
	}()

	for r := repCache.Next(); r != nil; r = repCache.Next() {
		log.Trace().Stringer("operation", r.OpType).Msg(util.RelString(r.Rel))
		err := o.RelationshipWriter.Write(ctx, []*v1.RelationshipUpdate{
			{
				Operation:    r.OpType.RelationshipUpdateOpType(),
				Relationship: r.Rel,
			},
		})
		if err == nil {
			continue
		}
		log.Warn().Err(err).Str("rel", util.RelString(r.Rel)).Stringer("op", r.OpType).Msg("requeueing")

		repCache.Requeue(r.OpType, r.Rel)
	}

	return nil
}
