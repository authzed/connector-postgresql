package e2e

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

//go:embed fixtures/test_schema.sql
var testSchema string

func spicedb(t testing.TB) *authzed.Client {
	t.Log("starting spicedb")
	defer t.Log("spicedb started")
	require := require.New(t)
	pool, err := dockertest.NewPool("")
	require.NoError(err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Cmd:          strings.Split(`serve --grpc-preshared-key somerandomkeyhere --grpc-no-tls --http-no-tls`, " "),
		Repository:   "quay.io/authzed/spicedb",
		Tag:          "v1.1.0",
		ExposedPorts: []string{"50051"},
	})
	require.NoError(err)

	var client *authzed.Client
	port := resource.GetPort("50051/tcp")
	require.NoError(pool.Retry(func() error {
		var err error
		client, err = authzed.NewClient(fmt.Sprintf("localhost:%s", port), grpcutil.WithInsecureBearerToken("somerandomkeyhere"), grpc.WithInsecure())
		if err != nil {
			return err
		}
		return nil
	}))

	t.Cleanup(func() {
		require.NoError(pool.Purge(resource))
	})
	return client
}

func postgres(t testing.TB, creds string, portNum uint16) (*pgxpool.Pool, string) {
	t.Log("starting postgres")
	defer t.Log("postgres started")
	require := require.New(t)
	pool, err := dockertest.NewPool("")
	require.NoError(err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Cmd:        strings.Split("postgres -c wal_level=logical -c max_wal_senders=5 -c max_replication_slots=5", " "),
		Repository: "postgres",
		Tag:        "11.13",
		Env:        []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=defaultdb"},
	})
	require.NoError(err)

	var dbpool *pgxpool.Pool
	port := resource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	require.NoError(pool.Retry(func() error {
		var err error
		dbpool, err = pgxpool.Connect(context.Background(), fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", creds, port))
		if err != nil {
			return err
		}
		return nil
	}))

	t.Cleanup(func() {
		require.NoError(pool.Purge(resource))
	})

	return dbpool, port
}

func newTestDB(t testing.TB, pool *pgxpool.Pool, creds string, port string) string {
	require := require.New(t)
	newDBName := "db" + tokenHex(require, 4)
	_, err := pool.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	require.NoError(err)

	connectStr := fmt.Sprintf(
		"postgres://%s@localhost:%s/%s?sslmode=disable",
		creds,
		port,
		newDBName,
	)

	testpool, err := pgxpool.Connect(context.Background(), connectStr)
	require.NoError(err)

	_, err = testpool.Exec(context.Background(), testSchema)
	require.NoError(err)

	t.Log(connectStr)

	return connectStr
}

func tokenHex(require *require.Assertions, nbytes uint8) string {
	token := make([]byte, nbytes)
	_, err := rand.Read(token)
	require.NoError(err)
	return hex.EncodeToString(token)
}
