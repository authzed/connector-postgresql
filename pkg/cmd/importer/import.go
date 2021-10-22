package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/connector-postgres/pkg/config"
	"github.com/authzed/connector-postgres/pkg/importer"
	"github.com/authzed/connector-postgres/pkg/streams"
	"github.com/authzed/connector-postgres/pkg/util"
	"github.com/authzed/connector-postgres/pkg/write"
)

// NewImportCmd configures a new cobra command that imports data from postgres
func NewImportCmd(ctx context.Context, streams streams.IO) *cobra.Command {
	o := NewOptions(streams)
	cmd := &cobra.Command{
		Use:     "import",
		Short:   "import data from postgres into spicedb",
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
	cmd.Flags().BoolVar(&o.DryRun, "dry-run", false, "log tuples that would be written without calling spicedb")
	cmd.Flags().StringVar(&o.MappingFile, "config", "", "path to a file containing the config that maps between pg tables and spicedb relationships")
	cobrautil.RegisterZeroLogFlags(cmd.Flags())

	return cmd
}

// Options holds options for the import command
type Options struct {
	streams.IO

	PostgresURI     string
	DryRun          bool
	MappingFile     string
	SpiceDBEndpoint string
	SpiceDBToken    string
	SpiceDBInsecure bool

	poolConfig *pgxpool.Config
	config     config.TableMapping
	writer     write.RelationshipWriter
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

	cfg, err := pgxpool.ParseConfig(o.PostgresURI)
	if err != nil {
		return err
	}
	o.poolConfig = cfg

	if len(o.MappingFile) == 0 && o.config == nil {
		return fmt.Errorf("no mapping config file set")
	}

	if o.config == nil {
		mapFileContents, err := os.ReadFile(o.MappingFile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(mapFileContents, &o.config); err != nil {
			return err
		}
	}

	if o.DryRun {
		o.writer = write.NewDryRunRelationshipWriter()
		return nil
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

// Run runs the command configured by Options.
func (o *Options) Run(ctx context.Context) error {
	log.Info().EmbedObject(util.LoggedConnConfig{ConnConfig: o.poolConfig.ConnConfig}).Msg("connecting to postgres")

	conn, err := pgxpool.ConnectConfig(ctx, o.poolConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	return importer.NewPostgresImporter(conn, o.writer, o.config).Import(ctx)
}
