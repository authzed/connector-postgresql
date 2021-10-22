module github.com/authzed/connector-postgres/e2e

go 1.16

require (
	github.com/authzed/connector-postgres v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v4 v4.13.0
	github.com/ory/dockertest/v3 v3.8.0
	github.com/rs/zerolog v1.25.0
	github.com/stretchr/testify v1.7.0
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/containerd/continuity v0.2.1 // indirect
)

replace github.com/authzed/connector-postgres => ../
