package pgschema

import (
	"github.com/authzed/connector-postgresql/pkg/config"
	"github.com/jackc/pglogrepl"
)

// Schema represents a set of Tables and the tx log sequence number (XLogPos)
// at which they were fetched
type Schema struct {
	Tables  []*Table
	XLogPos pglogrepl.LSN
}

func (s *Schema) ToConfig() *config.Config {
	return &config.Config{
		Schema: s.ToZedSchema(),
		Tables: s.ToTableMapping(),
	}
}

// ToTableMapping generates a provisional TableMapping config based on an
// existing schema. This can be a good starting point for developing a
// postgres import config.
func (s *Schema) ToTableMapping() []config.TableMapping {
	mapping := make([]config.TableMapping, 0, len(s.Tables))
	for _, t := range s.Tables {
		relationshipConfig := make([]config.RowMapping, 0, len(t.ForeignKeys))
		for _, fk := range t.ForeignKeys {
			relationshipConfig = append(relationshipConfig, config.RowMapping{
				ResourceType:   fk.foreignTable,
				SubjectType:    fk.primaryTable,
				Relation:       fk.name,
				ResourceIDCols: t.PrimaryKeys.cols,
				SubjectIDCols:  fk.cols,
			})
		}
		mapping = append(mapping, config.TableMapping{
			Name:          t.Name,
			Relationships: relationshipConfig,
		})
	}
	return mapping
}

// ToZedSchema generates an (example) zed schema for the postgres schema
func (s *Schema) ToZedSchema() (zedSchema string) {
	for _, t := range s.Tables {
		zedSchema += "\n"
		zedSchema += "definition " + t.Name
		if len(t.ForeignKeys) == 0 {
			zedSchema += " {}\n"
			continue
		}
		zedSchema += " {\n"
		for _, fk := range t.ForeignKeys {
			zedSchema += "    relation " + fk.name + ": " + fk.primaryTable + "\n"
		}
		zedSchema += "}\n"
	}
	return
}

// InternalMapping converts a TableMapping config to an InternalTableMapping by
// using the information on the Schema.
func (s *Schema) InternalMapping(external []config.TableMapping) []config.InternalTableMapping {
	tmap := make(map[string]*Table, 0)
	for _, t := range s.Tables {
		tmap[t.Name] = t
	}
	mapping := make([]config.InternalTableMapping, 0, len(external))
	for _, extMap := range external {
		internalRelMap := make([]config.InternalRowMapping, 0, len(extMap.Relationships))
		for _, rm := range extMap.Relationships {
			resids := make([]int, 0)
			subids := make([]int, 0)
			// TODO: better
			for _, colname := range rm.ResourceIDCols {
				for _, col := range tmap[extMap.Name].Cols {
					if col.name == colname {
						resids = append(resids, col.id)
						break
					}
				}
			}
			for _, colname := range rm.SubjectIDCols {
				for _, col := range tmap[extMap.Name].Cols {
					if col.name == colname {
						subids = append(subids, col.id)
						break
					}
				}
			}
			internalRelMap = append(internalRelMap, config.InternalRowMapping{
				ResourceType:   rm.ResourceType,
				SubjectType:    rm.SubjectType,
				Relation:       rm.Relation,
				ResourceIDCols: resids,
				SubjectIDCols:  subids,
			})
		}
		mapping = append(mapping, config.InternalTableMapping{
			TableID:              tmap[extMap.Name].ID,
			RelationshipsByColID: internalRelMap,
		})
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
