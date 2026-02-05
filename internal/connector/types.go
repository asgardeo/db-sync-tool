package connector

// ReaderType identifies a CDC reader implementation.
type ReaderType string

// WriterType identifies a SQL writer implementation.
type WriterType string

// Supported reader types.
const (
	ReaderTypeMSSQL ReaderType = "mssql"
)

// Supported writer types.
const (
	WriterTypePostgres WriterType = "postgres"
)

// ReaderConfig contains configuration for creating a CDC reader.
type ReaderConfig struct {
	Type             ReaderType
	ConnectionString string
}

// WriterConfig contains configuration for creating a SQL writer.
type WriterConfig struct {
	Type             WriterType
	ConnectionString string
	SchemaMappings   []SchemaMapping
}

// SchemaMapping represents a source to target schema mapping.
type SchemaMapping struct {
	SourceSchema string
	TargetSchema string
}
