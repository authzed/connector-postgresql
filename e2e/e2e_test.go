package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/authzed/connector-postgres/pkg/cmd/run"
	"github.com/authzed/connector-postgres/pkg/streams"
)

func TestSchemaReflection(t *testing.T) {
	require := require.New(t)
	pg, port, cleanup := postgres(require, "postgres:secret", 5432)
	defer cleanup()
	connString := newTestDB(require, pg, "postgres:secret", port)
	t.Log(connString)
	o := run.NewOptions(streams.NewStdIO())
	o.PostgresURI = connString
	o.DryRun = true
	require.NoError(json.Unmarshal(testMapping, &o.Mapping))
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
				INSERT INTO tag(tag_value)
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
	require.NoError(o.Complete())
	require.NoError(o.Run(ctx))
}

func postgres(require *require.Assertions, creds string, portNum uint16) (*pgxpool.Pool, string, func()) {
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

	cleanup := func() {
		require.NoError(pool.Purge(resource))
	}

	return dbpool, port, cleanup
}

func newTestDB(require *require.Assertions, pool *pgxpool.Pool, creds string, port string) string {
	newDBName := "db" + tokenHex(require, 4)
	_, err := pool.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	require.NoError(err)

	connectStr := fmt.Sprintf(
		"postgres://%s@localhost:%s/%s?sslmode=disable",
		creds,
		port,
		newDBName,
	)
	// fill with test data
	schema := `
CREATE TABLE customers(
   customer_id INT GENERATED ALWAYS AS IDENTITY,
   customer_name VARCHAR(255) NOT NULL,
   PRIMARY KEY(customer_id, customer_name)
);

CREATE TABLE contacts(
   contact_id INT GENERATED ALWAYS AS IDENTITY,
   customer_id INT,
   customer_name VARCHAR(255) NOT NULL,
   contact_name VARCHAR(255) NOT NULL,
   phone VARCHAR(15),
   email VARCHAR(100),
   PRIMARY KEY(contact_id),
   CONSTRAINT fk_customer
      FOREIGN KEY(customer_id,customer_name) 
	  REFERENCES customers(customer_id,customer_name)
	  ON DELETE CASCADE
);

INSERT INTO customers(customer_name)
VALUES('BigCo'),
      ('SmallFry');	   
	   
INSERT INTO contacts(customer_id, customer_name, contact_name, phone, email)
VALUES(1,'BigCo', 'John Doe','(408)-111-1234','john.doe@bigco.dev'),
      (1,'BigCo','Jane Doe','(408)-111-1235','jane.doe@bigco.dev'),
      (2,'SmallFry','Jeshk Doe','(408)-222-1234','jeshk.doe@smallfry.dev');

CREATE TABLE article (
  id SERIAL PRIMARY KEY,
  title TEXT
);

CREATE TABLE tag (
  id SERIAL PRIMARY KEY,
  tag_value TEXT
);

CREATE TABLE article_tag (
  article_id INT,
  tag_id INT,
  PRIMARY KEY (article_id, tag_id),
  CONSTRAINT fk_article FOREIGN KEY(article_id) REFERENCES article(id),
  CONSTRAINT fk_tag FOREIGN KEY(tag_id) REFERENCES tag(id)
);

INSERT INTO article(title) 
VALUES('Dune'), ('Lies of Lock Lamora');

INSERT INTO tag(tag_value)
VALUES('scifi'), ('fantasy');

INSERT INTO article_tag(article_id,tag_id)
VALUES(1,1), (1,2), (2,2);
`

	testpool, err := pgxpool.Connect(context.Background(), connectStr)
	require.NoError(err)
	_, err = testpool.Exec(context.Background(), schema)
	require.NoError(err)

	return connectStr
}

func tokenHex(require *require.Assertions, nbytes uint8) string {
	token := make([]byte, nbytes)
	_, err := rand.Read(token)
	require.NoError(err)
	return hex.EncodeToString(token)
}

var testMapping = []byte(`{
  "article": [],
  "article_tag": [
    {
      "resource_type": "article",
      "subject_type": "tags",
      "relation": "tags",
      "resource_id_cols": [
        "article_id"
      ],
      "subject_id_cols": [
        "tag_id"
      ]
    },
    {
      "resource_type": "tags",
      "subject_type": "article",
      "relation": "article",
      "resource_id_cols": [
        "tag_id"
      ],
      "subject_id_cols": [
        "article_id"
      ]
    }
  ],
  "contacts": [
    {
      "resource_type": "contacts",
      "subject_type": "customers",
      "relation": "fk_customer",
      "resource_id_cols": [
        "contact_id"
      ],
      "subject_id_cols": [
        "customer_id",
        "customer_name"
      ]
    }
  ],
  "customers": [],
  "tag": []
}`)
