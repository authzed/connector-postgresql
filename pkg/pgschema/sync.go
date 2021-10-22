package pgschema

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// SyncSchema generates a simplified representation of the postgres schema
// It assumes that conn has the `replication` flag set so that it can
// fetch the current tx log sequence number, and will error if not.
func SyncSchema(ctx context.Context, conn *pgxpool.Pool, includedTables ...string) (*Schema, error) {
	if !strings.Contains(conn.Config().ConnString(), "replication") {
		return nil, fmt.Errorf("SyncSchema called on a non-replication connection")
	}
	if !conn.Config().ConnConfig.PreferSimpleProtocol {
		return nil, fmt.Errorf("SyncSchema can't be called without simple protocol preferred")
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	defer tx.Rollback(ctx)

	tables, err := syncTables(ctx, tx, includedTables)
	if err != nil {
		return nil, err
	}

	for name, t := range tables {
		pks, err := syncPrimaryKeys(ctx, tx, name)
		if err != nil {
			return nil, err
		}
		t.PrimaryKeys = *pks

		cols, err := syncColIds(ctx, tx, name)
		if err != nil {
			return nil, err
		}
		t.Cols = cols
	}

	fks, err := syncForeignKeys(ctx, tx)
	if err != nil {
		return nil, err
	}
	for name, t := range tables {
		keys, ok := fks[name]
		if !ok {
			continue
		}
		t.ForeignKeys = keys
	}

	// Exec on the underlying pgconn will re-use the current transaction
	id, err := pglogrepl.ParseIdentifySystem(tx.Conn().PgConn().Exec(ctx, "IDENTIFY_SYSTEM"))
	if err != nil {
		return nil, err
	}
	return &Schema{
		Tables:  tables,
		XLogPos: id.XLogPos,
	}, nil
}

func syncTables(ctx context.Context, tx pgx.Tx, includedTables []string) (map[string]*Table, error) {
	tables := make(map[string]*Table, 0)

	expected := make(map[string]struct{}, len(includedTables))
	for _, t := range includedTables {
		expected[t] = struct{}{}
	}

	includes := func(tableName string) bool {
		if len(expected) == 0 {
			return true
		}
		_, ok := expected[tableName]
		return ok
	}

	rows, err := tx.Query(ctx, querySelectTables)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var oid string
		var name string
		if err := rows.Scan(&oid, &name); err != nil {
			return nil, err
		}
		if !includes(name) {
			continue
		}
		id, err := strconv.Atoi(oid)
		if err != nil {
			return nil, err
		}
		tables[name] = &Table{
			ID:          uint32(id),
			Name:        name,
			PrimaryKeys: PrimaryKey{cols: make([]string, 0)},
			ForeignKeys: make([]ForeignKey, 0),
		}
		delete(expected, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(expected) > 0 {
		return nil, fmt.Errorf("not all expected tables found in remote schema. missing: %v", expected)
	}
	return tables, nil
}

func syncColIds(ctx context.Context, tx pgx.Tx, name string) ([]Col, error) {
	cols := make([]Col, 0)
	rows, err := tx.Query(ctx, querySelectColIds, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var colnum int
		var col string
		if err := rows.Scan(&col, &colnum); err != nil {
			return nil, err
		}
		cols = append(cols, Col{
			name: col,
			id:   colnum,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func syncPrimaryKeys(ctx context.Context, tx pgx.Tx, name string) (*PrimaryKey, error) {
	pknums := make([]int, 0)
	pks := make([]string, 0)
	rows, err := tx.Query(ctx, querySelectPrimaryKeys, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var colnum int
		var col string
		if err := rows.Scan(&colnum, &col); err != nil {
			return nil, err
		}
		pks = append(pks, col)
		pknums = append(pknums, colnum)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &PrimaryKey{colids: pknums, cols: pks}, nil
}

func syncForeignKeys(ctx context.Context, tx pgx.Tx) (map[string][]ForeignKey, error) {
	fks := make(map[string][]ForeignKey, 0)
	rows, err := tx.Query(ctx, querySelectForeignKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var fk ForeignKey
		// TODO: should be able to scan into a []int (from array_agg), but didn't work
		var colnums string
		var col string
		if err := rows.Scan(&col, &colnums, &fk.name, &fk.foreignTable, &fk.primaryTable); err != nil {
			return nil, err
		}
		fk.cols = strings.Split(col, ",")
		for _, c := range strings.Split(colnums, ",") {
			n, err := strconv.Atoi(c)
			if err != nil {
				return nil, err
			}
			fk.colids = append(fk.colids, n)
		}
		if _, ok := fks[fk.foreignTable]; !ok {
			fks[fk.foreignTable] = make([]ForeignKey, 0)
		}
		fks[fk.foreignTable] = append(fks[fk.foreignTable], fk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return fks, nil
}
