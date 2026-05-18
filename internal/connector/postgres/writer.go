// Package postgres provides a PostgreSQL SQL writer implementation.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/proto"
)

// SchemaMapping represents a source to target schema mapping.
type SchemaMapping struct {
	SourceSchema string
	TargetSchema string
}

// PostgreSQLWriter writes CDC changes to PostgreSQL.
// Implements the connector.SQLWriter interface.
type PostgreSQLWriter struct {
	pool           *pgxpool.Pool
	pkCache        map[string][]string
	cacheMu        sync.RWMutex
	schemaMappings []SchemaMapping
}

// NewPostgreSQLWriter creates a new PostgreSQL writer.
func NewPostgreSQLWriter(connectionString string, schemaMappings []SchemaMapping) (*PostgreSQLWriter, error) {
	pool, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	return &PostgreSQLWriter{
		pool:           pool,
		pkCache:        make(map[string][]string),
		schemaMappings: schemaMappings,
	}, nil
}

// mapSchema returns the target schema for a given source schema.
func (w *PostgreSQLWriter) mapSchema(sourceSchema string) string {
	for _, m := range w.schemaMappings {
		if m.SourceSchema == sourceSchema {
			return m.TargetSchema
		}
	}
	return sourceSchema
}

// Close closes the connection pool.
func (w *PostgreSQLWriter) Close() {
	w.pool.Close()
}

// IsHealthy checks if the database connection is healthy.
func (w *PostgreSQLWriter) IsHealthy(ctx context.Context) bool {
	return w.pool.Ping(ctx) == nil
}

// ApplyChanges applies a batch of changes within a transaction.
func (w *PostgreSQLWriter) ApplyChanges(ctx context.Context, changes []*proto.ChangeRequest, idempotent bool) (uint64, error) {
	log.Debug().Int("change_count", len(changes)).Msg("Applying changes")

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Defer constraint checking until commit to handle out-of-order inserts
	// (e.g., child rows arriving before parent rows in the same batch)
	if _, err := tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		return 0, fmt.Errorf("failed to defer constraints: %w", err)
	}

	var rowsAffected uint64

	for _, change := range changes {
		op := change.Operation

		// Skip UPDATE_OLD records - we only process the after image
		if op == proto.OperationType_OPERATION_TYPE_UPDATE_OLD {
			continue
		}

		var result int64
		var err error

		switch op {
		case proto.OperationType_OPERATION_TYPE_INSERT, proto.OperationType_OPERATION_TYPE_UPDATE_NEW:
			if idempotent {
				result, err = w.upsert(ctx, tx, change)
			} else {
				result, err = w.insert(ctx, tx, change)
			}
		case proto.OperationType_OPERATION_TYPE_DELETE:
			result, err = w.delete(ctx, tx, change)
		default:
			log.Warn().Int32("operation", int32(op)).Msg("Skipping unrecognized operation")
			continue
		}

		if err != nil {
			return 0, err
		}
		rowsAffected += uint64(result)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Debug().Uint64("rows_affected", rowsAffected).Msg("Transaction committed")
	return rowsAffected, nil
}

func (w *PostgreSQLWriter) upsert(ctx context.Context, tx pgx.Tx, change *proto.ChangeRequest) (int64, error) {
	targetSchema := w.mapSchema(change.SchemaName)
	table := fmt.Sprintf("%s.%s", targetSchema, change.TableName)
	pkColumns, err := w.getPrimaryKeyColumns(ctx, change)
	if err != nil {
		return 0, err
	}

	var rowData map[string]interface{}
	if err := json.Unmarshal([]byte(change.RowDataJson), &rowData); err != nil {
		return 0, fmt.Errorf("failed to parse row data JSON: %w", err)
	}

	if len(rowData) == 0 {
		return 0, nil
	}

	// Build column lists
	columns := make([]string, 0, len(rowData))
	placeholders := make([]string, 0, len(rowData))
	values := make([]interface{}, 0, len(rowData))
	i := 1

	for col, val := range rowData {
		columns = append(columns, col)
		placeholders = append(placeholders, "$"+strconv.Itoa(i))
		values = append(values, val)
		i++
	}

	columnList := strings.Join(columns, ", ")
	placeholderList := strings.Join(placeholders, ", ")
	pkList := strings.Join(pkColumns, ", ")

	// Build update clause (exclude PKs from update)
	pkSet := make(map[string]bool)
	for _, pk := range pkColumns {
		pkSet[pk] = true
	}

	var updateColumns []string
	for _, col := range columns {
		if !pkSet[col] {
			updateColumns = append(updateColumns, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}

	var sql string
	if len(updateColumns) == 0 {
		// All columns are PKs, just do insert ignore
		sql = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			table, columnList, placeholderList, pkList,
		)
	} else {
		updateList := strings.Join(updateColumns, ", ")
		sql = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			table, columnList, placeholderList, pkList, updateList,
		)
	}

	log.Debug().Str("sql", sql).Msg("Executing upsert")

	result, err := tx.Exec(ctx, sql, values...)
	if err != nil {
		return 0, fmt.Errorf("upsert query failed: %w", err)
	}

	return result.RowsAffected(), nil
}

func (w *PostgreSQLWriter) insert(ctx context.Context, tx pgx.Tx, change *proto.ChangeRequest) (int64, error) {
	targetSchema := w.mapSchema(change.SchemaName)
	table := fmt.Sprintf("%s.%s", targetSchema, change.TableName)

	var rowData map[string]interface{}
	if err := json.Unmarshal([]byte(change.RowDataJson), &rowData); err != nil {
		return 0, fmt.Errorf("failed to parse row data JSON: %w", err)
	}

	if len(rowData) == 0 {
		return 0, nil
	}

	columns := make([]string, 0, len(rowData))
	placeholders := make([]string, 0, len(rowData))
	values := make([]interface{}, 0, len(rowData))
	i := 1

	for col, val := range rowData {
		columns = append(columns, col)
		placeholders = append(placeholders, "$"+strconv.Itoa(i))
		values = append(values, val)
		i++
	}

	columnList := strings.Join(columns, ", ")
	placeholderList := strings.Join(placeholders, ", ")

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columnList, placeholderList)

	log.Debug().Str("sql", sql).Msg("Executing insert")

	result, err := tx.Exec(ctx, sql, values...)
	if err != nil {
		return 0, fmt.Errorf("insert query failed: %w", err)
	}

	return result.RowsAffected(), nil
}

func (w *PostgreSQLWriter) delete(ctx context.Context, tx pgx.Tx, change *proto.ChangeRequest) (int64, error) {
	targetSchema := w.mapSchema(change.SchemaName)
	table := fmt.Sprintf("%s.%s", targetSchema, change.TableName)

	var pkData map[string]interface{}
	if err := json.Unmarshal([]byte(change.PrimaryKeyJson), &pkData); err != nil {
		return 0, fmt.Errorf("failed to parse primary key JSON: %w", err)
	}

	if len(pkData) == 0 {
		log.Warn().Msg("Delete with empty primary key, skipping")
		return 0, nil
	}

	// Build WHERE clause
	conditions := make([]string, 0, len(pkData))
	values := make([]interface{}, 0, len(pkData))
	i := 1

	for col, val := range pkData {
		conditions = append(conditions, fmt.Sprintf("%s = $%d", col, i))
		values = append(values, val)
		i++
	}

	whereClause := strings.Join(conditions, " AND ")
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", table, whereClause)

	log.Debug().Str("sql", sql).Msg("Executing delete")

	result, err := tx.Exec(ctx, sql, values...)
	if err != nil {
		return 0, fmt.Errorf("delete query failed: %w", err)
	}

	return result.RowsAffected(), nil
}

func (w *PostgreSQLWriter) getPrimaryKeyColumns(ctx context.Context, change *proto.ChangeRequest) ([]string, error) {
	// Use source schema for cache key (consistent with incoming change requests)
	cacheKey := fmt.Sprintf("%s.%s", change.SchemaName, change.TableName)

	// Check cache first
	w.cacheMu.RLock()
	if pks, ok := w.pkCache[cacheKey]; ok {
		w.cacheMu.RUnlock()
		return pks, nil
	}
	w.cacheMu.RUnlock()

	// Try to get from change metadata
	var pkFromMetadata []string
	for _, col := range change.Columns {
		if col.IsPrimaryKey {
			pkFromMetadata = append(pkFromMetadata, col.Name)
		}
	}

	if len(pkFromMetadata) > 0 {
		w.cacheMu.Lock()
		w.pkCache[cacheKey] = pkFromMetadata
		w.cacheMu.Unlock()
		return pkFromMetadata, nil
	}

	// Fall back to querying PostgreSQL (use target schema)
	targetSchema := w.mapSchema(change.SchemaName)
	targetTable := fmt.Sprintf("%s.%s", targetSchema, change.TableName)
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
	`

	rows, err := w.pool.Query(ctx, query, targetTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Cache the result
	w.cacheMu.Lock()
	w.pkCache[cacheKey] = columns
	w.cacheMu.Unlock()

	return columns, nil
}
