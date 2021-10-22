package pgschema

import (
	"github.com/authzed/connector-postgres/pkg/config"
	"github.com/jackc/pglogrepl"
)

// Schema represents a set of Tables and the tx log sequence number (XLogPos)
// at which they were fetched
type Schema struct {
	Tables  map[string]*Table
	XLogPos pglogrepl.LSN
}

// ToTableMapping generates a provisional TableMapping config based on an
// existing schema. This can be a good starting point for developing a
// postgres import config.
func (s *Schema) ToTableMapping() config.TableMapping {
	mapping := make(map[string][]config.RowMapping, 0)
	for _, t := range s.Tables {
		mapping[t.Name] = make([]config.RowMapping, 0)
		for _, fk := range t.ForeignKeys {
			mapping[t.Name] = append(mapping[t.Name], config.RowMapping{
				ResourceType:   fk.foreignTable,
				SubjectType:    fk.primaryTable,
				Relation:       fk.name,
				ResourceIDCols: t.PrimaryKeys.cols,
				SubjectIDCols:  fk.cols,
			})
		}
	}
	return mapping
}

// InternalMapping converts a TableMapping config to an InternalTableMapping by
// using the information on the Schema.
func (s *Schema) InternalMapping(external config.TableMapping) config.InternalTableMapping {
	mapping := make(map[uint32][]config.InternalRowMapping, 0)
	for t, rms := range external {
		irms := make([]config.InternalRowMapping, 0)
		for _, rm := range rms {
			resids := make([]int, 0)
			subids := make([]int, 0)
			// TODO: better
			for _, colname := range rm.ResourceIDCols {
				for _, col := range s.Tables[t].Cols {
					if col.name == colname {
						resids = append(resids, col.id)
						break
					}
				}
			}
			for _, colname := range rm.SubjectIDCols {
				for _, col := range s.Tables[t].Cols {
					if col.name == colname {
						subids = append(subids, col.id)
						break
					}
				}
			}
			irms = append(irms, config.InternalRowMapping{
				ResourceType:   rm.ResourceType,
				SubjectType:    rm.SubjectType,
				Relation:       rm.Relation,
				ResourceIDCols: resids,
				SubjectIDCols:  subids,
			})
		}
		mapping[s.Tables[t].ID] = irms
	}
	return mapping
}

// Table is associated with a set of PrimaryKeys and a set of ForeignKeys
type Table struct {
	// ID is the int table identifier in postgres
	ID          uint32
	Name        string
	PrimaryKeys PrimaryKey
	ForeignKeys []ForeignKey
	Cols        []Col
}

// PrimaryKey is the name of a primary key field in a table
type PrimaryKey struct {
	colids []int
	cols   []string
}

// Col is the name and index of a column in a table
type Col struct {
	name string
	id   int
}

// ForeignKey represents a foreign key relationship
// the ForeignKey named "name" indicates that the columns "cols" on Table "foreignTable"
// references the primary key columns "cols" of "primaryTable"
type ForeignKey struct {
	name         string
	cols         []string
	colids       []int
	foreignTable string
	primaryTable string
}
