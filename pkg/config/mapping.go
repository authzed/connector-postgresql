package config

// Config holds a zed schema and a tablemapping that generates that schema
type Config struct {
	Tables []TableMapping `json:"tables"`
	Schema string         `json:"schema"`
}

// TableMapping maps the name of a table to a set of configs for transforming
// rows into relationships
type TableMapping struct {
	Name          string       `json:"name"`
	Relationships []RowMapping `json:"relationships,omitempty"`
}

// RowMapping configures how to transform a row into a relationship
type RowMapping struct {
	ResourceType   string   `json:"resource_type"`
	SubjectType    string   `json:"subject_type"`
	Relation       string   `json:"relation"`
	ResourceIDCols []string `json:"resource_id_cols"`
	SubjectIDCols  []string `json:"subject_id_cols"`
}

// InternalTableMapping is a TableMapping with table names converted into
// internal postgres ids, so that it can be used to parse the replication log
type InternalTableMapping struct {
	TableID              uint32
	RelationshipsByColID []InternalRowMapping
}

// InternalRowMapping is a RowMapping with column names converted into column
// indexes, so that it can be used to parse the replication log
type InternalRowMapping struct {
	ResourceType   string
	SubjectType    string
	Relation       string
	ResourceIDCols []int
	SubjectIDCols  []int
}
