package write

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/authzed-go/v1"
)

// AppendSchemaWriter appends a schema fragment to a spicedb schema
type AppendSchemaWriter interface {
	Write(context.Context, string) error
}

func NewSchemaAppendWriter(client *authzed.Client, discard bool) AppendSchemaWriter {
	if discard {
		return &DiscardingSchemaAppendWriter{}
	}
	return NewStdSchemaAppendWriter(client)
}

// StdSchemaAppendWriter writes via an authzed client, no-frills.
type StdSchemaAppendWriter struct {
	client *authzed.Client
}

func (w *StdSchemaAppendWriter) Write(ctx context.Context, schema string) error {
	schemaResp, err := w.client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
	if status.Code(err) == codes.NotFound {
		schemaResp = &v1.ReadSchemaResponse{SchemaText: ""}
	} else if err != nil {
		return err
	}
	fullSchema := appendSchema(schemaResp.SchemaText, schema)
	log.Info().Msg("writing schema")
	log.Debug().Str("schema", fullSchema).Send()
	_, err = w.client.WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: fullSchema,
	})
	return err
}

// NewStdSchemaAppendWriter constructs a new schema append writer that logs
// but doesn't write.
func NewStdSchemaAppendWriter(client *authzed.Client) *StdSchemaAppendWriter {
	return &StdSchemaAppendWriter{client: client}
}

// DryRunSchemaAppendWriter prints what the schema would have been.
type DryRunSchemaAppendWriter struct {
	client *authzed.Client
}

func (w *DryRunSchemaAppendWriter) Write(ctx context.Context, schema string) error {
	var schemaResp *v1.ReadSchemaResponse
	if w.client == nil {
		schemaResp = &v1.ReadSchemaResponse{SchemaText: ""}
	} else {
		var err error
		schemaResp, err = w.client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
		if status.Code(err) == codes.NotFound {
			schemaResp = &v1.ReadSchemaResponse{SchemaText: ""}
		} else if err != nil {
			return err
		}
	}
	log.Info().Msg("schema write skipped")
	log.Debug().Str("schema", appendSchema(schemaResp.SchemaText, schema)).Send()
	return nil
}

// NewDryRunSchemaAppendWriter constructs a new schema append writer that logs
// but doesn't write. If client is non-nil, it will attempt to read the existing
// schema from spicedb; otherwise it will assume the schema is empty.
func NewDryRunSchemaAppendWriter(client *authzed.Client) *DryRunSchemaAppendWriter {
	return &DryRunSchemaAppendWriter{
		client: client,
	}
}

// DiscardingSchemaAppendWriter does nothing but satisfy SchemaAppendWriter
type DiscardingSchemaAppendWriter struct{}

func (w DiscardingSchemaAppendWriter) Write(ctx context.Context, schema string) error {
	return nil
}

// TODO: smarter appending
func appendSchema(initial, fragment string) string {
	return initial + fragment
}
