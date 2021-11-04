package e2e

import (
	"context"
	_ "embed"
	"fmt"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	"github.com/authzed/connector-postgresql/pkg/cmd/importer"
	"github.com/authzed/connector-postgresql/pkg/cmd/run"
	"github.com/authzed/connector-postgresql/pkg/streams"
)

//go:embed fixtures/generated_config.yaml
var generatedConfig string

//go:embed fixtures/user_config.yaml
var exampleUserConfig []byte

func TestSchemaReflection(t *testing.T) {
	require := require.New(t)
	pg, port := postgres(t, "postgres:secret", 5432)
	connString := newTestDB(t, pg, "postgres:secret", port)

	o := run.NewOptions(streams.NewStdIO())
	o.PostgresURI = connString
	o.DryRun = true
	require.NoError(yaml.Unmarshal(exampleUserConfig, &o.Config))

	ctx, cancel := context.WithCancel(context.Background())
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: o.Out})
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	go func() {
		testpool, err := pgxpool.Connect(context.Background(), connString)
		require.NoError(err)
		for i := 0; i < 3; i++ {
			time.Sleep(1 * time.Second)
			_, err = testpool.Exec(context.Background(), fmt.Sprintf(`
				 INSERT INTO contacts(customer_id, customer_name, contact_name, email)
				 VALUES (2,'SmallFry','Jeshk Doe %d','jeshk.doe@smallfry.dev');
			`, i))
			require.NoError(err)

			_, err = testpool.Exec(context.Background(), fmt.Sprintf(`
				INSERT INTO tags(tag_value)
				VALUES('new_genre_%d');
			`, i))
			require.NoError(err)

			_, err = testpool.Exec(context.Background(), fmt.Sprintf(`
				INSERT INTO article_tag(article_id,tag_id)
				VALUES(1,%d), (2,%d);
			`, 3+i, 3+i))
			require.NoError(err)
		}
		cancel()
	}()
	require.NoError(o.Complete(ctx, []string{connString}))
	require.NoError(o.Run(ctx))
}

func TestImportAuto(t *testing.T) {
	// connector-postgresql import --dry-run=false <spicedb config> psql://whatever
	require := require.New(t)
	pg, port := postgres(t, "postgres:secret", 5432)
	connString := newTestDB(t, pg, "postgres:secret", port)
	spiceClient := spicedb(t)

	testIO, _, ioout, _ := streams.NewTestIO()
	o := importer.NewOptions(testIO)
	o.PostgresURI = connString
	o.AppendSchema = true
	o.Client = spiceClient

	require.NoError(o.Complete(context.Background(), []string{connString}))
	require.NoError(o.Run(context.Background()))
	require.Equal(generatedConfig, ioout.String())
}

func TestImportDryRun(t *testing.T) {
	// connector-postgresql import <spicedb config> psql://whatever
	require := require.New(t)
	pg, port := postgres(t, "postgres:secret", 5432)
	connString := newTestDB(t, pg, "postgres:secret", port)
	spiceClient := spicedb(t)

	testIO, _, ioout, _ := streams.NewTestIO()
	o := importer.NewOptions(testIO)
	o.PostgresURI = connString
	o.AppendSchema = true
	o.DryRun = true
	o.Client = spiceClient

	require.NoError(o.Complete(context.Background(), []string{connString}))
	require.NoError(o.Run(context.Background()))
	require.Equal(generatedConfig, ioout.String())

	// Dry-Run, no schema or relationships written
	_, err := spiceClient.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.Error(err)
	require.Equal(status.Code(err), codes.NotFound)

	// TODO: test with pre-existing schema, assert no relationships written
}

func TestImportUserProvidedConfig(t *testing.T) {
	// connector-postgresql import <spicedb config> --config-path=path/to/config.json psql://whatever
	require := require.New(t)
	pg, port := postgres(t, "postgres:secret", 5432)
	connString := newTestDB(t, pg, "postgres:secret", port)
	spiceClient := spicedb(t)

	testIO, _, ioout, _ := streams.NewTestIO()
	o := importer.NewOptions(testIO)
	o.PostgresURI = connString
	o.AppendSchema = true
	o.DryRun = false
	require.NoError(yaml.Unmarshal(exampleUserConfig, &o.Config))

	o.Client = spiceClient
	require.NoError(o.Complete(context.Background(), []string{connString}))
	require.NoError(o.Run(context.Background()))
	fmt.Println(ioout.String())
}
