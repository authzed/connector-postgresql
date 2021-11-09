module github.com/authzed/connector-postgresql/e2e

go 1.16

require (
	github.com/authzed/connector-postgresql v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v4 v4.13.0
	github.com/ory/dockertest/v3 v3.8.0
	github.com/rs/zerolog v1.25.0
	github.com/stretchr/testify v1.7.0
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/authzed/authzed-go v0.2.0
	github.com/authzed/grpcutil v0.0.0-20210914195113-c0d8369e7e1f
	github.com/containerd/continuity v0.2.1 // indirect
	google.golang.org/grpc v1.41.0
	sigs.k8s.io/yaml v1.3.0
)

replace github.com/authzed/connector-postgresql => ../
