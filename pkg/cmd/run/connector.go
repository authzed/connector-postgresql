package run

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/authzed/connector-postgres/pkg/cache"
	"github.com/authzed/connector-postgres/pkg/config"
	"github.com/authzed/connector-postgres/pkg/follow"
	"github.com/authzed/connector-postgres/pkg/importer"
	"github.com/authzed/connector-postgres/pkg/pgschema"
	"github.com/authzed/connector-postgres/pkg/streams"
	"github.com/authzed/connector-postgres/pkg/util"
	"github.com/authzed/connector-postgres/pkg/write"
)

// NewRunCmd configures a new cobra command that both imports (backfills) data
// from a postgres instance and watches the WAL to sync data continuously
func NewRunCmd(ctx context.Context, streams streams.IO) *cobra.Command {
	o := NewOptions(streams)
	cmd := &cobra.Command{
		Use:     "run",
		Short:   "Runs the full connector. Starts with a backfill, and then syncs all changes from the WAL.",
		PreRunE: util.ZeroLogPreRunEFunc(o.IO.Out),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			return o.Run(ctx)
		},
	}
	cmd.Flags().StringVar(&o.SpiceDBEndpoint, "spicedb-endpoint", "localhost:50051", "address for the SpiceDB endpoint")
	cmd.Flags().StringVar(&o.SpiceDBToken, "spicedb-token", "", "token for reading and writing to SpiceDB")
	cmd.Flags().BoolVar(&o.SpiceDBInsecure, "spicedb-insecure", false, "connect to SpiceDB without TLS")
	cmd.Flags().StringVar(&o.PostgresURI, "postgres", "", "address for the postgres endpoint")
	cmd.Flags().StringVar(&o.MetricsAddr, "internal-metrics-addr", ":9090", "address that will serve prometheus data (default: :9090")
	cmd.Flags().BoolVar(&o.DryRun, "dry-run", false, "log tuples that would be written without calling spicedb")
	cmd.Flags().StringVar(&o.MappingFile, "config", "", "path to a file containing the config that maps between pg tables and spicedb relationships")
	cobrautil.RegisterZeroLogFlags(cmd.Flags())

	return cmd
}

// Options holds options for the postgres connector
type Options struct {
	streams.IO

	PostgresURI     string
	SpiceDBEndpoint string
	SpiceDBToken    string
	SpiceDBInsecure bool
	MetricsAddr     string
	DryRun          bool
	MappingFile     string

	Mapping      config.TableMapping
	replogConfig *pgxpool.Config
	poolConfig   *pgxpool.Config
	writer       write.RelationshipWriter
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

	// configure a pool for bulk sync operations
	cfg, err := pgxpool.ParseConfig(o.PostgresURI)
	if err != nil {
		return err
	}
	o.poolConfig = cfg

	// configure a (limited) connection for watching the replication log
	repcfg, err := pgxpool.ParseConfig(o.PostgresURI + "&replication=database")
	if err != nil {
		return err
	}
	// replication connections don't support extended query protocol
	repcfg.ConnConfig.PreferSimpleProtocol = true
	o.replogConfig = repcfg

	if len(o.MappingFile) == 0 && o.Mapping == nil {
		return fmt.Errorf("no mapping config file set")
	}

	if o.Mapping == nil {
		mapFileContents, err := os.ReadFile(o.MappingFile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(mapFileContents, &o.Mapping); err != nil {
			return err
		}
	}

	if o.DryRun {
		o.writer = write.NewDryRunRelationshipWriter()
		return nil
	}

	if o.SpiceDBEndpoint == "" {
		return fmt.Errorf("must provide spicedb uri")
	}
	grpcOpts := make([]grpc.DialOption, 0)
	if o.SpiceDBInsecure == true {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}
	if o.SpiceDBToken != "" && o.SpiceDBInsecure {
		grpcOpts = append(grpcOpts, grpcutil.WithInsecureBearerToken(o.SpiceDBToken))
	}
	if o.SpiceDBToken != "" && !o.SpiceDBInsecure {
		grpcOpts = append(grpcOpts, grpcutil.WithBearerToken(o.SpiceDBToken))
	}
	client, err := authzed.NewClient(o.SpiceDBEndpoint, grpcOpts...)
	if err != nil {
		return err
	}
	o.writer = write.NewRelationshipWriter(client)
	return nil
}

// Run does a backfill and then watches for changes
func (o *Options) Run(ctx context.Context) error {
	log.Info().EmbedObject(util.LoggedConnConfig{ConnConfig: o.poolConfig.ConnConfig}).Msg("connecting to postgres")

	conn, err := pgxpool.ConnectConfig(ctx, o.poolConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	pgImport := importer.NewPostgresImporter(conn, o.writer, o.Mapping)
	if err := pgImport.Import(ctx); err != nil {
		return err
	}

	replogConn, err := pgxpool.ConnectConfig(ctx, o.replogConfig)
	if err != nil {
		return err
	}
	defer replogConn.Close()

	cache := cache.NewCache(ctx)
	log.Info().Msg("syncing schema")
	schema, err := pgschema.SyncSchema(ctx, replogConn)
	if err != nil {
		return err
	}

	for name, t := range schema.Tables {
		log.Debug().Stringer("XLogPos", schema.XLogPos).Str("table", name).Msgf("%#v", *t)
	}

	repconn, err := replogConn.Acquire(ctx)
	if err != nil {
		return err
	}
	defer repconn.Release()

	follower := follow.NewWalFollower(repconn.Conn().PgConn(), schema.InternalMapping(o.Mapping), cache)

	go func() {
		// TODO: separate contexts - killing the existing ctx will kill the connection
		// before the slots can be dropped
		if err := follower.Follow(ctx, schema.XLogPos); err != nil {
			log.Info().Err(err).Msg("stopped")
		}
	}()

	for r := cache.Next(); r != nil; r = cache.Next() {
		log.Trace().Stringer("operation", r.OpType).Msg(util.RelString(r.Rel))
		err := o.writer.Write(ctx, []*v1.RelationshipUpdate{
			{
				Operation:    r.OpType.RelationshipUpdateOpType(),
				Relationship: r.Rel,
			},
		})
		if err == nil {
			continue
		}
		log.Warn().Err(err).Str("rel", util.RelString(r.Rel)).Stringer("op", r.OpType).Msg("requeueing")

		cache.Requeue(r.OpType, r.Rel)
	}

	return nil
}
