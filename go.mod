module github.com/authzed/connector-postgres

go 1.16

require (
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jzelinskie/cobrautil v0.0.5
	github.com/rs/zerolog v1.25.0
	github.com/spf13/cobra v1.2.1
)

require (
	github.com/authzed/authzed-go v0.2.0
	github.com/authzed/grpcutil v0.0.0-20210914195113-c0d8369e7e1f
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pglogrepl v0.0.0-20210731151948-9f1effd582c4
	github.com/jackc/pgproto3/v2 v2.1.1
	github.com/jzelinskie/stringz v0.0.1 // indirect
	github.com/lib/pq v1.10.3 // indirect
	github.com/mattn/go-isatty v0.0.14
	github.com/prometheus/client_golang v1.11.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.0.0 // indirect
	golang.org/x/sys v0.0.0-20210921065528-437939a70204 // indirect
	google.golang.org/grpc v1.41.0
)
