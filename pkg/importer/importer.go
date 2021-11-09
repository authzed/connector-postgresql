package importer

import (
	"context"
	"fmt"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/authzed/connector-postgresql/pkg/config"
	"github.com/authzed/connector-postgresql/pkg/write"
)

// Importer is an interface satisfied by anything that can import data into
// SpiceDB
type Importer interface {
	Import(ctx context.Context) error
}

// PostgresImporter will import data from postgres into SpiceDB. It knows how
// to convert data into relationships with its TableMapping.
type PostgresImporter struct {
	conn    *pgxpool.Pool
	writer  write.RelationshipWriter
	mapping []config.TableMapping
}

var _ Importer = &PostgresImporter{}

// NewPostgresImporter returns a new instance of a postgres importer
func NewPostgresImporter(conn *pgxpool.Pool, writer write.RelationshipWriter, mapping []config.TableMapping) *PostgresImporter {
	return &PostgresImporter{
		conn:    conn,
		writer:  writer,
		mapping: mapping,
	}
}

// Import walks through each table in the config and writes relationships
func (i *PostgresImporter) Import(ctx context.Context) error {
	for _, m := range i.mapping {
		log.Info().Str("table", m.Name).Msg("writing relationships")
		if err := i.importTable(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (i *PostgresImporter) importTable(ctx context.Context, tableMap config.TableMapping) error {
	for _, rm := range tableMap.Relationships {
		if err := i.importRelationships(ctx, tableMap.Name, rm); err != nil {
			return err
		}
	}
	return nil
}

func (i *PostgresImporter) importRelationships(ctx context.Context, table string, rm config.RowMapping) error {
	relupdates, err := i.relationshipsFor(ctx, table, rm)
	if err != nil {
		return err
	}
	return i.writer.Write(ctx, relupdates)
}

func (i *PostgresImporter) relationshipsFor(ctx context.Context, table string, rm config.RowMapping) ([]*v1.RelationshipUpdate, error) {
	relupdates := make([]*v1.RelationshipUpdate, 0)
	rcols := make([]string, 0)
	scols := make([]string, 0)
	for _, r := range rm.ResourceIDCols {
		rcols = append(rcols, r+"::text")
	}
	for _, s := range rm.SubjectIDCols {
		scols = append(scols, s+"::text")
	}
	query := fmt.Sprintf("SELECT CONCAT_WS('_', %s), CONCAT_WS('_', %s) FROM %s;", strings.Join(rcols, ","), strings.Join(scols, ","), table)
	rows, err := i.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var resid, subid string
		if err := rows.Scan(&resid, &subid); err != nil {
			return nil, err
		}
		rel := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: rm.ResourceType,
				ObjectId:   resid,
			},
			Relation: rm.Relation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: rm.SubjectType,
					ObjectId:   subid,
				},
			},
		}
		relupdates = append(relupdates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: rel,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return relupdates, nil
}
