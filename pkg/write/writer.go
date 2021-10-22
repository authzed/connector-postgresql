package write

import (
	"context"

	"github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/connector-postgres/pkg/util"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// RelationshipWriter writes v1 relationships
type RelationshipWriter interface {
	Write(context.Context, []*v1.RelationshipUpdate) error
}

// NewBatchingRelationshipWriter will write relationships in batches of size
// batchSize
func NewBatchingRelationshipWriter(writer RelationshipWriter, batchSize int) RelationshipWriter {
	if batchSize == 0 {
		return writer
	}
	return &BatchingRelationshipWriter{
		writer:    writer,
		batchSize: batchSize,
	}
}

// NewRelationshipWriter returns a relationship writer based on the current
// config. It will configure trace logging if the current log level is trace,
// and will dry-run if no client is passed.
func NewRelationshipWriter(client *authzed.Client) RelationshipWriter {
	if client == nil {
		return NewDryRunRelationshipWriter()
	}
	w := StdRelationshipWriter{client: client}
	if zerolog.GlobalLevel() == zerolog.TraceLevel {
		return LoggingRelationshipWriter{writer: w, level: zerolog.TraceLevel}
	}
	return w
}

// StdRelationshipWriter writes via an authzed client, no-frills.
type StdRelationshipWriter struct {
	client *authzed.Client
}

func (w StdRelationshipWriter) Write(ctx context.Context, updates []*v1.RelationshipUpdate) error {
	_, err := w.client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{Updates: updates})
	return err
}

// BatchingRelationshipWriter writes in batches of batchSize
type BatchingRelationshipWriter struct {
	writer    RelationshipWriter
	batchSize int
}

func (w BatchingRelationshipWriter) Write(ctx context.Context, updates []*v1.RelationshipUpdate) error {
	for i, start, end := 0, 0, w.batchSize; end <= len(updates)-1; i, start, end = i+1, (i+1)*w.batchSize, (i+2)*w.batchSize {
		if end > len(updates)-1 {
			end = len(updates) - 1
		}
		if err := w.writer.Write(ctx, updates[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// LoggingRelationshipWriter will log each write before delegating to an
// underlying RelationshipWriter
type LoggingRelationshipWriter struct {
	writer RelationshipWriter
	level  zerolog.Level
}

func (w LoggingRelationshipWriter) Write(ctx context.Context, updates []*v1.RelationshipUpdate) error {
	err := w.writer.Write(ctx, updates)
	for _, u := range updates {
		log.WithLevel(w.level).Str("rel", util.RelString(u.Relationship)).Msg(u.Operation.String())
	}
	return err
}

// NewDryRunRelationshipWriter constructs a new relationship writer that logs
// but doesn't write.
func NewDryRunRelationshipWriter() RelationshipWriter {
	return LoggingRelationshipWriter{
		writer: DiscardingRelationshipWriter{},
		level:  zerolog.InfoLevel,
	}
}

// DiscardingRelationshipWriter does nothing but satisfy RelationshipWriter
type DiscardingRelationshipWriter struct{}

func (w DiscardingRelationshipWriter) Write(ctx context.Context, updates []*v1.RelationshipUpdate) error {
	return nil
}
