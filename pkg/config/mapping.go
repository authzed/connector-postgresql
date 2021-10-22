package config

// TableMapping maps the name of a table to a set of configs for transforming
// rows into relationships
type TableMapping map[string][]RowMapping

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
type InternalTableMapping map[uint32][]InternalRowMapping

// InternalRowMapping is a RowMapping with column names converted into column
// indexes, so that it can be used to parse the replication log
type InternalRowMapping struct {
	ResourceType   string
	SubjectType    string
	Relation       string
	ResourceIDCols []int
	SubjectIDCols  []int
}
