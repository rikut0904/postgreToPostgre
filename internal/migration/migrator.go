package migration

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"postgreToPostgre/internal/database"
)

type Migrator struct {
	sink       Broadcaster
	logger     *log.Logger
	onProgress func(*Progress)
}

type execTarget interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

type tableTask struct {
	schema string
	table  string
}

func NewMigrator(sink Broadcaster, logger *log.Logger) *Migrator {
	return &Migrator{sink: sink, logger: logger}
}

func (m *Migrator) WithProgressHook(fn func(*Progress)) {
	m.onProgress = fn
}

func workerCount(tables int, requested int) int {
	if requested > 0 {
		if requested > tables {
			return tables
		}
		return requested
	}
	n := tables
	if n > 8 {
		n = 8
	}
	if n < 1 {
		n = 1
	}
	return n
}

func (m *Migrator) Run(ctx context.Context, req Request) error {
	if req.Options.BatchSize <= 0 {
		req.Options.BatchSize = 1000
	}
	if req.Options.ExistingTableMode == "" {
		req.Options.ExistingTableMode = "drop"
	}
	if req.Options.ExistingDataMode == "" {
		req.Options.ExistingDataMode = "truncate"
	}

	src, err := database.Connect(ctx, req.Source)
	if err != nil {
		return err
	}
	defer src.Close(ctx)

	dst, err := database.Connect(ctx, req.Destination)
	if err != nil {
		return err
	}
	defer dst.Close(ctx)

	tableStatuses, totalRows, err := buildTableStatuses(ctx, src, req.Selection)
	if err != nil {
		return err
	}

	progress := NewProgress(m.sink, tableStatuses)
	if m.onProgress != nil {
		m.onProgress(progress)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if req.Options.ExistingTableMode == "drop" {
		progress.SetPhase("drop")
		if err := m.dropSelectedSchemas(ctx, dst, req.Selection, progress); err != nil {
			return err
		}
		req.Options.ExistingTableMode = "keep"
	}

	progress.SetPhase("schema")
	if err := m.createSchemas(ctx, dst, req.Selection, progress, req.Options); err != nil {
		return err
	}

	progress.SetPhase("types")
	if err := m.createTypes(ctx, src, dst, req, progress); err != nil {
		return err
	}

	progress.SetPhase("sequences")
	if err := m.createSequences(ctx, src, dst, req, progress); err != nil {
		return err
	}

		if req.Options.DryRun {
			progress.Log("dry run: skipping data migration")
			progress.Finish()
			return nil
		}

	progress.SetPhase("tables")
	if err := m.createTables(ctx, src, dst, req, progress); err != nil {
		return err
	}

	progress.SetPhase("data")
	if err := m.copyDataParallel(ctx, req, progress, totalRows); err != nil {
		return err
	}

	progress.SetPhase("indexes")
	if err := m.createIndexesParallel(ctx, req, progress); err != nil {
		return err
	}

	progress.SetPhase("constraints")
	if err := m.createConstraintsParallel(ctx, req, progress); err != nil {
		return err
	}

	progress.SetPhase("views")
	if err := m.createViews(ctx, src, dst, req, progress); err != nil {
		return err
	}

	progress.SetPhase("functions")
	if err := m.createFunctions(ctx, src, dst, req, progress); err != nil {
		return err
	}

	progress.SetPhase("triggers")
	if err := m.createTriggers(ctx, src, dst, req, progress); err != nil {
		return err
	}

	progress.Finish()
	return nil
}

func (m *Migrator) createSchemas(ctx context.Context, dst execTarget, sel Selection, progress *Progress, opts Options) error {
	for _, schema := range sel.Schemas {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ddl := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, database.QuoteIdent(schema.Name))
		progress.Log(fmt.Sprintf("create schema %s", schema.Name))
		if opts.DryRun {
			continue
		}
		if _, err := dst.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) createTables(ctx context.Context, src *pgx.Conn, dst execTarget, req Request, progress *Progress) error {
	for _, schema := range req.Selection.Schemas {
		for _, table := range schema.Tables {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cols, err := database.TableColumns(ctx, src, schema.Name, table)
			if err != nil {
				return err
			}
			if req.Options.ExistingTableMode == "drop" {
				exists, kind, err := database.RelationKind(ctx, dst, schema.Name, table)
				if err != nil {
					return err
				}
				if exists {
					if err := dropRelationByKind(ctx, dst, progress, schema.Name, table, kind); err != nil {
						return err
					}
				}
			}

			var colDefs []string
			for _, col := range cols {
				if col.DefaultVal != "" {
					if err := ensureSequenceForDefault(ctx, src, dst, progress, schema.Name, col.DefaultVal); err != nil {
						return err
					}
				}
				def := fmt.Sprintf("%s %s", database.QuoteIdent(col.Name), col.DataType)
				if col.DefaultVal != "" {
					def += " DEFAULT " + col.DefaultVal
				}
				if !col.Nullable {
					def += " NOT NULL"
				}
				colDefs = append(colDefs, def)
			}
			ddl := fmt.Sprintf(`CREATE TABLE %s %s (%s)`,
				ifNotExists(req.Options.ExistingTableMode == "keep"),
				fmt.Sprintf("%s.%s", database.QuoteIdent(schema.Name), database.QuoteIdent(table)),
				strings.Join(colDefs, ", "),
			)
			progress.Log(fmt.Sprintf("create table %s.%s", schema.Name, table))
			if _, err := dst.Exec(ctx, ddl); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Migrator) dropSelectedSchemas(ctx context.Context, dst execTarget, sel Selection, progress *Progress) error {
	for _, schema := range sel.Schemas {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		progress.Log(fmt.Sprintf("drop schema %s", schema.Name))
		if _, err := dst.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, database.QuoteIdent(schema.Name))); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) createTypes(ctx context.Context, src *pgx.Conn, dst execTarget, req Request, progress *Progress) error {
	for _, schema := range req.Selection.Schemas {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		enums, err := database.EnumTypes(ctx, src, schema.Name)
		if err != nil {
			return err
		}
		for _, e := range enums {
			labels := make([]string, 0, len(e.Labels))
			for _, l := range e.Labels {
				labels = append(labels, fmt.Sprintf("'%s'", strings.ReplaceAll(l, "'", "''")))
			}
			ddl := fmt.Sprintf(`CREATE TYPE %s.%s AS ENUM (%s)`, database.QuoteIdent(schema.Name), database.QuoteIdent(e.Name), strings.Join(labels, ", "))
			progress.Log(fmt.Sprintf("create enum %s.%s", schema.Name, e.Name))
			if _, err := dst.Exec(ctx, ddl); err != nil {
				m.logger.Printf("enum error: %v", err)
			}
		}

		composites, err := database.CompositeTypes(ctx, src, schema.Name)
		if err != nil {
			return err
		}
		for _, c := range composites {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var attrs []string
			for _, a := range c.Attributes {
				def := fmt.Sprintf("%s %s", database.QuoteIdent(a.Name), a.DataType)
				attrs = append(attrs, def)
			}
			ddl := fmt.Sprintf(`CREATE TYPE %s.%s AS (%s)`, database.QuoteIdent(schema.Name), database.QuoteIdent(c.Name), strings.Join(attrs, ", "))
			progress.Log(fmt.Sprintf("create composite %s.%s", schema.Name, c.Name))
			if _, err := dst.Exec(ctx, ddl); err != nil {
				m.logger.Printf("composite error: %v", err)
			}
		}
	}
	return nil
}

// copyDataParallel copies table data using a worker pool with independent connections.
func (m *Migrator) copyDataParallel(ctx context.Context, req Request, progress *Progress, totalRows int64) error {
	var tasks []tableTask
	for _, schema := range req.Selection.Schemas {
		for _, table := range schema.Tables {
			tasks = append(tasks, tableTask{schema: schema.Name, table: table})
		}
	}
	if len(tasks) == 0 {
		return nil
	}

	workers := workerCount(len(tasks), req.Options.Workers)
	taskCh := make(chan tableTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var migratedTotal int64
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			srcConn, err := database.Connect(ctx, req.Source)
			if err != nil {
				m.logger.Printf("worker src connect error: %v", err)
				return
			}
			defer srcConn.Close(ctx)

			dstConn, err := database.Connect(ctx, req.Destination)
			if err != nil {
				m.logger.Printf("worker dst connect error: %v", err)
				return
			}
			defer dstConn.Close(ctx)

			if req.Options.DisableFK {
				if _, err := dstConn.Exec(ctx, `SET session_replication_role = 'replica'`); err != nil {
					m.logger.Printf("failed to disable FK: %v", err)
				}
				defer dstConn.Exec(ctx, `SET session_replication_role = 'origin'`)
			}

			for task := range taskCh {
				if ctx.Err() != nil {
					return
				}
				if err := m.copyOneTable(ctx, srcConn, dstConn, task.schema, task.table, req.Options, progress, &migratedTotal, totalRows); err != nil {
					m.logger.Printf("table %s.%s failed: %v", task.schema, task.table, err)
					progress.AddFailedTable(task.schema, task.table, err.Error())
					progress.UpdateTable(task.schema, task.table, "failed", 0, 0)
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errMu.Unlock()
					return
				}
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (m *Migrator) copyOneTable(ctx context.Context, src *pgx.Conn, dst *pgx.Conn, schema, table string, opts Options, progress *Progress, migratedTotal *int64, totalRows int64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	exists, kind, err := database.RelationKind(ctx, dst, schema, table)
	if err != nil {
		return err
	}
	if exists && kind != "r" && kind != "p" {
		if opts.ExistingTableMode == "drop" {
			if err := dropRelationByKind(ctx, dst, progress, schema, table, kind); err != nil {
				return err
			}
			cols, err := database.TableColumns(ctx, src, schema, table)
			if err != nil {
				return err
			}
			var colDefs []string
			for _, col := range cols {
				def := fmt.Sprintf("%s %s", database.QuoteIdent(col.Name), col.DataType)
				if col.DefaultVal != "" {
					def += " DEFAULT " + col.DefaultVal
				}
				if !col.Nullable {
					def += " NOT NULL"
				}
				colDefs = append(colDefs, def)
			}
			ddl := fmt.Sprintf(`CREATE TABLE %s.%s (%s)`,
				database.QuoteIdent(schema),
				database.QuoteIdent(table),
				strings.Join(colDefs, ", "),
			)
			progress.Log(fmt.Sprintf("create table %s.%s", schema, table))
			if _, err := dst.Exec(ctx, ddl); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%s.%s is not a table", schema, table)
		}
	}

	tx, err := dst.Begin(ctx)
	if err != nil {
		return err
	}

	dataMode := opts.ExistingDataMode
	cols, err := database.TableColumns(ctx, src, schema, table)
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	colNames := make([]string, 0, len(cols))
	for _, c := range cols {
		colNames = append(colNames, c.Name)
	}

	rowCount, _ := database.TableRowCount(ctx, src, schema, table)
	progress.UpdateTable(schema, table, "in_progress", rowCount, 0)
	if ctx.Err() != nil {
		_ = tx.Rollback(ctx)
		return ctx.Err()
	}

	if dataMode == "truncate" {
		progress.Log(fmt.Sprintf("truncate %s.%s", schema, table))
		if _, err := tx.Exec(ctx, fmt.Sprintf(`TRUNCATE %s.%s`, database.QuoteIdent(schema), database.QuoteIdent(table))); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}

	if dataMode == "upsert" {
		pkCols, _ := database.PrimaryKeyColumns(ctx, src, schema, table)
		if len(pkCols) == 0 {
			progress.Log(fmt.Sprintf("no primary key on %s.%s, falling back to insert", schema, table))
			dataMode = "append"
		} else {
			if err := m.upsertRows(ctx, src, tx, schema, table, colNames, pkCols, opts.BatchSize, progress, migratedTotal, totalRows, rowCount); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			progress.UpdateTable(schema, table, "completed", rowCount, rowCount)
			return tx.Commit(ctx)
		}
	}

	if err := m.copyRows(ctx, src, tx, schema, table, colNames, opts.BatchSize, progress, migratedTotal, totalRows, rowCount); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	progress.UpdateTable(schema, table, "completed", rowCount, rowCount)
	return tx.Commit(ctx)
}

func dropRelationByKind(ctx context.Context, dst execTarget, progress *Progress, schema, name, kind string) error {
	switch kind {
	case "r", "p":
		progress.Log(fmt.Sprintf("drop table %s.%s", schema, name))
		_, err := dst.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, database.QuoteIdent(schema), database.QuoteIdent(name)))
		return err
	case "v":
		progress.Log(fmt.Sprintf("drop view %s.%s", schema, name))
		_, err := dst.Exec(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS %s.%s CASCADE`, database.QuoteIdent(schema), database.QuoteIdent(name)))
		return err
	case "m":
		progress.Log(fmt.Sprintf("drop materialized view %s.%s", schema, name))
		_, err := dst.Exec(ctx, fmt.Sprintf(`DROP MATERIALIZED VIEW IF EXISTS %s.%s CASCADE`, database.QuoteIdent(schema), database.QuoteIdent(name)))
		return err
	case "f":
		progress.Log(fmt.Sprintf("drop foreign table %s.%s", schema, name))
		_, err := dst.Exec(ctx, fmt.Sprintf(`DROP FOREIGN TABLE IF EXISTS %s.%s CASCADE`, database.QuoteIdent(schema), database.QuoteIdent(name)))
		return err
	case "c":
		progress.Log(fmt.Sprintf("drop type %s.%s", schema, name))
		_, err := dst.Exec(ctx, fmt.Sprintf(`DROP TYPE IF EXISTS %s.%s CASCADE`, database.QuoteIdent(schema), database.QuoteIdent(name)))
		return err
	default:
		return fmt.Errorf("%s.%s is not a table (relkind=%s)", schema, name, kind)
	}
}

var nextvalRe = regexp.MustCompile(`(?i)^nextval\('([^']+)'::regclass\)$`)

func ensureSequenceForDefault(ctx context.Context, src *pgx.Conn, dst execTarget, progress *Progress, defaultSchema, defaultVal string) error {
	matches := nextvalRe.FindStringSubmatch(strings.TrimSpace(defaultVal))
	if len(matches) != 2 {
		return nil
	}
	seqSchema, seqName, ok := parseRegclassName(matches[1], defaultSchema)
	if !ok {
		return nil
	}
	exists, kind, err := database.RelationKind(ctx, dst, seqSchema, seqName)
	if err != nil {
		return err
	}
	if exists {
		if kind == "S" {
			return nil
		}
		return fmt.Errorf("%s.%s is not a sequence (relkind=%s)", seqSchema, seqName, kind)
	}
	def, err := database.SequenceDefinition(ctx, src, seqSchema, seqName)
	if err != nil {
		return err
	}
	progress.Log(fmt.Sprintf("create sequence %s.%s", seqSchema, seqName))
	_, err = dst.Exec(ctx, def)
	return err
}

func parseRegclassName(raw, defaultSchema string) (schema, name string, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	parts := strings.Split(raw, ".")
	if len(parts) == 1 {
		return defaultSchema, unquoteIdent(parts[0]), true
	}
	if len(parts) == 2 {
		return unquoteIdent(parts[0]), unquoteIdent(parts[1]), true
	}
	return "", "", false
}

func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
		s = strings.ReplaceAll(s, `""`, `"`)
	}
	return s
}

func (m *Migrator) copyRows(ctx context.Context, src *pgx.Conn, dst execTarget, schema, table string, columns []string, batchSize int, progress *Progress, migratedTotal *int64, totalRows, tableTotal int64) error {
	rows, err := src.Query(ctx, fmt.Sprintf(`SELECT * FROM %s.%s`, database.QuoteIdent(schema), database.QuoteIdent(table)))
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([][]any, 0, batchSize)
	var migrated int64

	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		vals, err := rows.Values()
		if err != nil {
			return err
		}
		batch = append(batch, vals)
		if len(batch) >= batchSize {
			if err := copyBatch(ctx, dst, schema, table, columns, batch); err != nil {
				return err
			}
			migrated += int64(len(batch))
			atomic.AddInt64(migratedTotal, int64(len(batch)))
			progress.UpdateTable(schema, table, "in_progress", tableTotal, migrated)
			progress.UpdateOverall(calcOverall(atomic.LoadInt64(migratedTotal), totalRows))
			batch = batch[:0]
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if len(batch) > 0 {
		if err := copyBatch(ctx, dst, schema, table, columns, batch); err != nil {
			return err
		}
		migrated += int64(len(batch))
		atomic.AddInt64(migratedTotal, int64(len(batch)))
		progress.UpdateTable(schema, table, "in_progress", tableTotal, migrated)
		progress.UpdateOverall(calcOverall(atomic.LoadInt64(migratedTotal), totalRows))
	}
	return nil
}

func (m *Migrator) upsertRows(ctx context.Context, src *pgx.Conn, dst execTarget, schema, table string, columns, pkCols []string, batchSize int, progress *Progress, migratedTotal *int64, totalRows, tableTotal int64) error {
	rows, err := src.Query(ctx, fmt.Sprintf(`SELECT * FROM %s.%s`, database.QuoteIdent(schema), database.QuoteIdent(table)))
	if err != nil {
		return err
	}
	defer rows.Close()

	insertSQL := buildUpsertSQL(schema, table, columns, pkCols)
	var batchValues [][]any
	var migrated int64

	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		vals, err := rows.Values()
		if err != nil {
			return err
		}
		batchValues = append(batchValues, vals)
		if len(batchValues) >= batchSize {
			if err := execBatch(ctx, dst, insertSQL, batchValues); err != nil {
				return err
			}
			migrated += int64(len(batchValues))
			atomic.AddInt64(migratedTotal, int64(len(batchValues)))
			progress.UpdateTable(schema, table, "in_progress", tableTotal, migrated)
			progress.UpdateOverall(calcOverall(atomic.LoadInt64(migratedTotal), totalRows))
			batchValues = batchValues[:0]
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if len(batchValues) > 0 {
		if err := execBatch(ctx, dst, insertSQL, batchValues); err != nil {
			return err
		}
		migrated += int64(len(batchValues))
		atomic.AddInt64(migratedTotal, int64(len(batchValues)))
		progress.UpdateTable(schema, table, "in_progress", tableTotal, migrated)
		progress.UpdateOverall(calcOverall(atomic.LoadInt64(migratedTotal), totalRows))
	}
	return nil
}

// createIndexesParallel creates indexes using a worker pool.
func (m *Migrator) createIndexesParallel(ctx context.Context, req Request, progress *Progress) error {
	type indexTask struct {
		schema string
		table  string
		ddl    string
	}

	// Collect all index DDLs first using a single connection.
	srcConn, err := database.Connect(ctx, req.Source)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	var tasks []indexTask
	for _, schema := range req.Selection.Schemas {
		for _, table := range schema.Tables {
			defs, err := database.IndexDefinitions(ctx, srcConn, schema.Name, table)
			if err != nil {
				return err
			}
			for _, def := range defs {
				tasks = append(tasks, indexTask{schema: schema.Name, table: table, ddl: def})
			}
		}
	}
	srcConn.Close(ctx)

	if len(tasks) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := workerCount(len(tasks), req.Options.Workers)
	taskCh := make(chan indexTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			dstConn, err := database.Connect(ctx, req.Destination)
			if err != nil {
				m.logger.Printf("index worker connect error: %v", err)
				return
			}
			defer dstConn.Close(ctx)

			for task := range taskCh {
				if ctx.Err() != nil {
					return
				}
				progress.Log(fmt.Sprintf("create index: %s", task.ddl))
				if _, err := dstConn.Exec(ctx, task.ddl); err != nil {
					m.logger.Printf("index error: %v", err)
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errMu.Unlock()
					return
				}
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// createConstraintsParallel creates constraints using a worker pool.
func (m *Migrator) createConstraintsParallel(ctx context.Context, req Request, progress *Progress) error {
	type constraintTask struct {
		schema string
		table  string
		ddl    string
	}

	srcConn, err := database.Connect(ctx, req.Source)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	var tasks []constraintTask
	for _, schema := range req.Selection.Schemas {
		for _, table := range schema.Tables {
			defs, err := database.ConstraintDefinitions(ctx, srcConn, schema.Name, table)
			if err != nil {
				return err
			}
			for _, def := range defs {
				ddl := fmt.Sprintf(`ALTER TABLE %s.%s ADD %s`, database.QuoteIdent(schema.Name), database.QuoteIdent(table), def)
				tasks = append(tasks, constraintTask{schema: schema.Name, table: table, ddl: ddl})
			}
		}
	}
	srcConn.Close(ctx)

	if len(tasks) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := workerCount(len(tasks), req.Options.Workers)
	taskCh := make(chan constraintTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			dstConn, err := database.Connect(ctx, req.Destination)
			if err != nil {
				m.logger.Printf("constraint worker connect error: %v", err)
				return
			}
			defer dstConn.Close(ctx)

			for task := range taskCh {
				if ctx.Err() != nil {
					return
				}
				progress.Log(fmt.Sprintf("add constraint: %s.%s", task.schema, task.table))
				if _, err := dstConn.Exec(ctx, task.ddl); err != nil {
					m.logger.Printf("constraint error: %v", err)
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errMu.Unlock()
					return
				}
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (m *Migrator) createViews(ctx context.Context, src *pgx.Conn, dst execTarget, req Request, progress *Progress) error {
	for _, schema := range req.Selection.Schemas {
		for _, view := range schema.Views {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			def, err := database.ViewDefinition(ctx, src, schema.Name, view)
			if err != nil {
				return err
			}
			ddl := fmt.Sprintf(`CREATE OR REPLACE VIEW %s.%s AS %s`, database.QuoteIdent(schema.Name), database.QuoteIdent(view), def)
			progress.Log(fmt.Sprintf("create view %s.%s", schema.Name, view))
			if _, err := dst.Exec(ctx, ddl); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Migrator) createFunctions(ctx context.Context, src *pgx.Conn, dst execTarget, req Request, progress *Progress) error {
	for _, schema := range req.Selection.Schemas {
		for _, fn := range schema.Funcs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			def, err := database.FunctionDefinition(ctx, src, schema.Name, fn)
			if err != nil {
				return err
			}
			progress.Log(fmt.Sprintf("create function %s.%s", schema.Name, fn))
			if _, err := dst.Exec(ctx, def); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Migrator) createSequences(ctx context.Context, src *pgx.Conn, dst execTarget, req Request, progress *Progress) error {
	for _, schema := range req.Selection.Schemas {
		for _, seq := range schema.Seqs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			def, err := database.SequenceDefinition(ctx, src, schema.Name, seq)
			if err != nil {
				m.logger.Printf("sequence def error: %v", err)
				continue
			}
			progress.Log(fmt.Sprintf("create sequence %s.%s", schema.Name, seq))
			if _, err := dst.Exec(ctx, def); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Migrator) createTriggers(ctx context.Context, src *pgx.Conn, dst execTarget, req Request, progress *Progress) error {
	for _, schema := range req.Selection.Schemas {
		for _, table := range schema.Tables {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			triggers, err := database.ListTriggers(ctx, src, schema.Name)
			if err != nil {
				return err
			}
			for _, trg := range triggers {
				if trg.Table != table {
					continue
				}
				def, err := database.TriggerDefinition(ctx, src, schema.Name, trg.Table, trg.Name)
				if err != nil {
					return err
				}
				progress.Log(fmt.Sprintf("create trigger %s on %s.%s", trg.Name, schema.Name, trg.Table))
				if _, err := dst.Exec(ctx, def); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func buildTableStatuses(ctx context.Context, src *pgx.Conn, sel Selection) ([]TableStatus, int64, error) {
	var tables []TableStatus
	var total int64
	for _, schema := range sel.Schemas {
		for _, table := range schema.Tables {
			if ctx.Err() != nil {
				return nil, 0, ctx.Err()
			}
			count, err := database.TableRowCount(ctx, src, schema.Name, table)
			if err != nil {
				return nil, 0, err
			}
			tables = append(tables, TableStatus{
				Schema:       schema.Name,
				Name:         table,
				Status:       "pending",
				TotalRows:    count,
				MigratedRows: 0,
				Percent:      0,
			})
			total += count
		}
	}
	return tables, total, nil
}

func ifNotExists(enable bool) string {
	if enable {
		return "IF NOT EXISTS"
	}
	return ""
}

func calcOverall(done, total int64) int {
	if total == 0 {
		return 0
	}
	return int(float64(done) / float64(total) * 100.0)
}

func copyBatch(ctx context.Context, dst execTarget, schema, table string, columns []string, rows [][]any) error {
	_, err := dst.CopyFrom(ctx, pgx.Identifier{schema, table}, columns, pgx.CopyFromRows(rows))
	return err
}

func buildUpsertSQL(schema, table string, columns, pkCols []string) string {
	colIdents := make([]string, 0, len(columns))
	placeholders := make([]string, 0, len(columns))
	updateCols := make([]string, 0, len(columns))
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, c := range pkCols {
		pkSet[c] = struct{}{}
	}

	for i, col := range columns {
		colIdents = append(colIdents, database.QuoteIdent(col))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		if _, ok := pkSet[col]; !ok {
			updateCols = append(updateCols, fmt.Sprintf("%s = EXCLUDED.%s", database.QuoteIdent(col), database.QuoteIdent(col)))
		}
	}

	conflictCols := make([]string, 0, len(pkCols))
	for _, c := range pkCols {
		conflictCols = append(conflictCols, database.QuoteIdent(c))
	}

	action := "DO UPDATE SET " + strings.Join(updateCols, ", ")
	if len(updateCols) == 0 {
		action = "DO NOTHING"
	}

	return fmt.Sprintf(
		`INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (%s) %s`,
		database.QuoteIdent(schema),
		database.QuoteIdent(table),
		strings.Join(colIdents, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictCols, ", "),
		action,
	)
}

func execBatch(ctx context.Context, dst execTarget, sql string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, values := range rows {
		batch.Queue(sql, values...)
	}
	br := dst.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
	}
	return nil
}
