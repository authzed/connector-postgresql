module github.com/authzed/connector-postgresql

go 1.16

require (
	github.com/authzed/authzed-go v0.2.0
	github.com/authzed/grpcutil v0.0.0-20210914195113-c0d8369e7e1f
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pglogrepl v0.0.0-20210731151948-9f1effd582c4
	github.com/jackc/pgproto3/v2 v2.1.1
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jzelinskie/cobrautil v0.0.7
	github.com/rs/zerolog v1.26.0
	github.com/spf13/cobra v1.2.1
	google.golang.org/grpc v1.42.0
)

require (
	github.com/lib/pq v1.10.3 // indirect
	github.com/mattn/go-isatty v0.0.14
	github.com/spf13/viper v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.1.0 // indirect
	golang.org/x/net v0.0.0-20211109214657-ef0fda0de508 // indirect
	golang.org/x/sys v0.0.0-20211109184856-51b60fd695b3 // indirect
	google.golang.org/genproto v0.0.0-20211104193956-4c6863e31247 // indirect
	sigs.k8s.io/yaml v1.3.0
)
