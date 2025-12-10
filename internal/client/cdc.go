package client

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/internal/common"
	"github.com/wso2/db-sync-tool/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CdcReader reads CDC changes from MSSQL.
type CdcReader struct {
	db          *sql.DB
	schemaCache map[string][]*proto.ColumnMetadata
	cacheMu     sync.RWMutex
}

// NewCdcReader creates a new CDC reader with the given connection string.
func NewCdcReader(connectionString string) (*CdcReader, error) {
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &CdcReader{
		db:          db,
		schemaCache: make(map[string][]*proto.ColumnMetadata),
	}, nil
}

// Close closes the database connection.
func (r *CdcReader) Close() error {
	return r.db.Close()
}

// PollChanges polls for CDC changes starting from the given LSN.
func (r *CdcReader) PollChanges(ctx context.Context, fromLsn *common.Lsn, batchSize uint32) ([]*proto.ChangeRequest, error) {
	// Get the valid LSN range
	minLsn, maxLsn, err := r.getLsnRange(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get LSN range: %w", err)
	}

	// If maxLsn is nil or empty, there are no CDC changes available yet
	if len(maxLsn) == 0 {
		log.Debug().Msg("No CDC changes available yet (max_lsn is NULL)")
		return nil, nil
	}

	// Determine starting LSN
	var startLsn []byte
	if fromLsn != nil {
		nextLsn, err := r.getNextLsn(ctx, fromLsn.Bytes())
		if err != nil {
			return nil, fmt.Errorf("failed to get next LSN: %w", err)
		}
		if nextLsn != nil {
			startLsn = nextLsn
		} else {
			startLsn = minLsn
		}
	} else {
		startLsn = minLsn
	}

	// If startLsn is still nil or empty, use minLsn from a valid capture instance
	if len(startLsn) == 0 {
		log.Debug().Msg("No valid start LSN available")
		return nil, nil
	}

	log.Debug().
		Str("start_lsn", common.NewLsn(startLsn).ToHexString()).
		Str("max_lsn", common.NewLsn(maxLsn).ToHexString()).
		Msg("Polling changes in LSN range")

	// Get all tracked CDC tables
	cdcTables, err := r.getCdcTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get CDC tables: %w", err)
	}

	var allChanges []*proto.ChangeRequest

	for _, table := range cdcTables {
		changes, err := r.readTableChanges(ctx, table.CaptureInstance, table.Schema, table.Table, startLsn, batchSize)
		if err != nil {
			log.Warn().Err(err).
				Str("table", fmt.Sprintf("%s.%s", table.Schema, table.Table)).
				Msg("Failed to read CDC changes")
			continue
		}
		allChanges = append(allChanges, changes...)

		// Stop if we've hit batch size
		if len(allChanges) >= int(batchSize) {
			break
		}
	}

	// Sort by LSN and sequence number for consistent ordering
	// (Go's sort is stable, so we can sort twice)
	// For simplicity, just return as-is since CDC returns ordered

	return allChanges, nil
}

type cdcTable struct {
	Schema          string
	Table           string
	CaptureInstance string
}

func (r *CdcReader) getLsnRange(ctx context.Context) ([]byte, []byte, error) {
	var maxLsn []byte

	// Get max LSN (global across all capture instances)
	maxQuery := "SELECT sys.fn_cdc_get_max_lsn()"
	if err := r.db.QueryRowContext(ctx, maxQuery).Scan(&maxLsn); err != nil {
		return nil, nil, err
	}

	// If no max LSN, return early - no changes available
	if len(maxLsn) == 0 {
		return nil, nil, nil
	}

	// Get the minimum LSN across all capture instances
	minQuery := `
		SELECT MIN(min_lsn)
		FROM (
			SELECT sys.fn_cdc_get_min_lsn(capture_instance) as min_lsn
			FROM cdc.change_tables
		) as mins
		WHERE min_lsn IS NOT NULL
	`
	var minLsn []byte
	if err := r.db.QueryRowContext(ctx, minQuery).Scan(&minLsn); err != nil {
		if err == sql.ErrNoRows {
			return nil, maxLsn, nil
		}
		return nil, nil, err
	}

	return minLsn, maxLsn, nil
}

func (r *CdcReader) getNextLsn(ctx context.Context, lsn []byte) ([]byte, error) {
	var nextLsn []byte

	query := fmt.Sprintf("SELECT sys.fn_cdc_increment_lsn(0x%s)", common.NewLsn(lsn).ToHexString())
	row := r.db.QueryRowContext(ctx, query)

	if err := row.Scan(&nextLsn); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return nextLsn, nil
}

func (r *CdcReader) getCdcTables(ctx context.Context) ([]cdcTable, error) {
	query := `
		SELECT
			OBJECT_SCHEMA_NAME(source_object_id) as schema_name,
			OBJECT_NAME(source_object_id) as table_name,
			capture_instance
		FROM cdc.change_tables
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []cdcTable
	for rows.Next() {
		var schema, table, capture sql.NullString
		if err := rows.Scan(&schema, &table, &capture); err != nil {
			return nil, err
		}
		tables = append(tables, cdcTable{
			Schema:          schema.String,
			Table:           table.String,
			CaptureInstance: capture.String,
		})
	}

	return tables, rows.Err()
}

func (r *CdcReader) readTableChanges(ctx context.Context, captureInstance, schema, table string, fromLsn []byte, batchSize uint32) ([]*proto.ChangeRequest, error) {
	// Get column metadata if not cached
	tableKey := fmt.Sprintf("%s.%s", schema, table)

	r.cacheMu.RLock()
	columns, ok := r.schemaCache[tableKey]
	r.cacheMu.RUnlock()

	if !ok {
		var err error
		columns, err = r.getColumnMetadata(ctx, schema, table)
		if err != nil {
			return nil, err
		}
		r.cacheMu.Lock()
		r.schemaCache[tableKey] = columns
		r.cacheMu.Unlock()
	}

	// Build column list for the query (to avoid ambiguous column names with SELECT *)
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf("[%s]", col.Name))
	}
	columnList := strings.Join(columnNames, ", ")

	// Build the CDC query
	fromLsnHex := common.NewLsn(fromLsn).ToHexString()
	query := fmt.Sprintf(`
		SELECT TOP (%d)
			__$start_lsn,
			__$seqval,
			__$operation,
			__$update_mask,
			%s
		FROM cdc.fn_cdc_get_all_changes_%s(
			0x%s,
			sys.fn_cdc_get_max_lsn(),
			N'all update old'
		)
		ORDER BY __$start_lsn, __$seqval
	`, batchSize, columnList, captureInstance, fromLsnHex)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		// Handle case where CDC function doesn't exist or no changes
		log.Warn().Err(err).Msg("CDC query failed, returning empty changes")
		return nil, nil
	}
	defer rows.Close()

	var changes []*proto.ChangeRequest
	var seq uint64

	// Get column types for scanning
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		// Create scan destinations
		values := make([]interface{}, len(colTypes))
		valuePtrs := make([]interface{}, len(colTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		change, err := r.rowToChangeRequest(values, colTypes, schema, table, columns, seq)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
		seq++
	}

	return changes, rows.Err()
}

func (r *CdcReader) rowToChangeRequest(values []interface{}, colTypes []*sql.ColumnType, schema, table string, columns []*proto.ColumnMetadata, sequence uint64) (*proto.ChangeRequest, error) {
	// Extract CDC metadata columns (first 4)
	var lsn []byte
	if v, ok := values[0].([]byte); ok {
		lsn = v
	}

	var operation int32
	switch v := values[2].(type) {
	case int64:
		operation = int32(v)
	case int32:
		operation = v
	case int:
		operation = int32(v)
	}

	// Map MSSQL CDC operation to our enum
	var opType proto.OperationType
	switch operation {
	case 1:
		opType = proto.OperationType_OPERATION_TYPE_DELETE
	case 2:
		opType = proto.OperationType_OPERATION_TYPE_INSERT
	case 3:
		opType = proto.OperationType_OPERATION_TYPE_UPDATE_OLD
	case 4:
		opType = proto.OperationType_OPERATION_TYPE_UPDATE_NEW
	default:
		opType = proto.OperationType_OPERATION_TYPE_UNSPECIFIED
	}

	// Extract primary key and data columns (skip first 4 CDC columns)
	pkData := make(map[string]interface{})
	rowData := make(map[string]interface{})

	for i, col := range columns {
		idx := 4 + i // Skip CDC system columns
		if idx >= len(values) {
			break
		}

		val := convertValue(values[idx], colTypes[idx])
		rowData[col.Name] = val
		if col.IsPrimaryKey {
			pkData[col.Name] = val
		}
	}

	pkJSON, err := json.Marshal(pkData)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize primary key: %w", err)
	}

	rowJSON, err := json.Marshal(rowData)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize row data: %w", err)
	}

	return &proto.ChangeRequest{
		BatchId:         "", // Will be set by the orchestrator
		Lsn:             lsn,
		SequenceNumber:  sequence,
		Operation:       opType,
		SchemaName:      schema,
		TableName:       table,
		PrimaryKeyJson:  string(pkJSON),
		RowDataJson:     string(rowJSON),
		SourceTimestamp: timestamppb.Now(),
		Columns:         columns,
	}, nil
}

func convertValue(val interface{}, colType *sql.ColumnType) interface{} {
	if val == nil {
		return nil
	}

	typeName := strings.ToLower(colType.DatabaseTypeName())

	switch v := val.(type) {
	case []byte:
		// Check if it's a binary type
		if typeName == "varbinary" || typeName == "binary" || typeName == "image" {
			return base64.StdEncoding.EncodeToString(v)
		}
		// Otherwise treat as string
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return v
	}
}

func (r *CdcReader) getColumnMetadata(ctx context.Context, schema, table string) ([]*proto.ColumnMetadata, error) {
	query := fmt.Sprintf(`
		SELECT
			c.COLUMN_NAME,
			c.DATA_TYPE,
			CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as is_nullable,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as is_pk,
			c.CHARACTER_MAXIMUM_LENGTH,
			c.NUMERIC_PRECISION,
			c.NUMERIC_SCALE
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN (
			SELECT ku.COLUMN_NAME
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
				ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
			WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
				AND tc.TABLE_SCHEMA = '%s'
				AND tc.TABLE_NAME = '%s'
		) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
		WHERE c.TABLE_SCHEMA = '%s' AND c.TABLE_NAME = '%s'
		ORDER BY c.ORDINAL_POSITION
	`, schema, table, schema, table)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*proto.ColumnMetadata
	for rows.Next() {
		var name, dataType string
		var isNullable, isPK int
		var maxLength, precision, scale sql.NullInt32

		if err := rows.Scan(&name, &dataType, &isNullable, &isPK, &maxLength, &precision, &scale); err != nil {
			return nil, err
		}

		col := &proto.ColumnMetadata{
			Name:         name,
			DataType:     dataType,
			IsNullable:   isNullable == 1,
			IsPrimaryKey: isPK == 1,
		}
		if maxLength.Valid {
			v := maxLength.Int32
			col.MaxLength = &v
		}
		if precision.Valid {
			v := precision.Int32
			col.Precision = &v
		}
		if scale.Valid {
			v := scale.Int32
			col.Scale = &v
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}
