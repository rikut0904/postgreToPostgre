package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type SchemaObjects struct {
	Schema    string       `json:"schema"`
	Tables    []string     `json:"tables"`
	Views     []string     `json:"views"`
	Functions []string     `json:"functions"`
	Sequences []string     `json:"sequences"`
	Triggers  []TriggerRef `json:"triggers"`
}

type TriggerRef struct {
	Name  string `json:"name"`
	Table string `json:"table"`
}

type ColumnInfo struct {
	Name       string `json:"name"`
	DataType   string `json:"dataType"`
	Nullable   bool   `json:"nullable"`
	DefaultVal string `json:"defaultValue,omitempty"`
}

type EnumType struct {
	Name   string   `json:"name"`
	Labels []string `json:"labels"`
}

type CompositeType struct {
	Name       string       `json:"name"`
	Attributes []ColumnInfo `json:"attributes"`
}

func ListSchemas(ctx context.Context, q Querier) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema')
		  AND nspname NOT LIKE 'pg_toast%'
		ORDER BY nspname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func ListTables(ctx context.Context, q Querier, schema string) ([]string, error) {
	return listNames(ctx, q, `SELECT tablename FROM pg_tables WHERE schemaname=$1 ORDER BY tablename`, schema)
}

func ListViews(ctx context.Context, q Querier, schema string) ([]string, error) {
	return listNames(ctx, q, `SELECT viewname FROM pg_views WHERE schemaname=$1 ORDER BY viewname`, schema)
}

func ListFunctions(ctx context.Context, q Querier, schema string) ([]string, error) {
	return listNames(ctx, q, `
		SELECT p.proname
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname=$1
		ORDER BY p.proname`, schema)
}

func ListSequences(ctx context.Context, q Querier, schema string) ([]string, error) {
	return listNames(ctx, q, `SELECT sequencename FROM pg_sequences WHERE schemaname=$1 ORDER BY sequencename`, schema)
}

func ListTriggers(ctx context.Context, q Querier, schema string) ([]TriggerRef, error) {
	rows, err := q.Query(ctx, `
		SELECT t.tgname, c.relname
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT t.tgisinternal
		  AND n.nspname=$1
		ORDER BY t.tgname`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TriggerRef
	for rows.Next() {
		var ref TriggerRef
		if err := rows.Scan(&ref.Name, &ref.Table); err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

func TableColumns(ctx context.Context, q Querier, schema, table string) ([]ColumnInfo, error) {
	rows, err := q.Query(ctx, `
		SELECT a.attname,
			   pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
			   a.attnotnull,
			   pg_get_expr(ad.adbin, ad.adrelid) AS default_val
		FROM pg_attribute a
		LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		WHERE a.attrelid = $1::regclass
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum`,
		fmt.Sprintf("%s.%s", schema, table),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		var notNull bool
		var def *string
		if err := rows.Scan(&c.Name, &c.DataType, &notNull, &def); err != nil {
			return nil, err
		}
		c.Nullable = !notNull
		if def != nil {
			c.DefaultVal = *def
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func TableRowCount(ctx context.Context, q Querier, schema, table string) (int64, error) {
	var count int64
	err := q.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, QuoteIdent(schema), QuoteIdent(table))).Scan(&count)
	return count, err
}

func RelationKind(ctx context.Context, q Querier, schema, name string) (exists bool, relkind string, err error) {
	err = q.QueryRow(ctx, `
		SELECT c.relkind
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname=$1 AND c.relname=$2
		LIMIT 1`, schema, name).Scan(&relkind)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, "", nil
		}
		return false, "", err
	}
	return true, relkind, nil
}

func PrimaryKeyColumns(ctx context.Context, q Querier, schema, table string) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		  AND i.indisprimary
		ORDER BY a.attnum`,
		fmt.Sprintf("%s.%s", schema, table),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func IndexDefinitions(ctx context.Context, q Querier, schema, table string) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT pg_get_indexdef(i.indexrelid)
		FROM pg_index i
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname=$1 AND t.relname=$2
		  AND NOT i.indisprimary
		ORDER BY i.indexrelid`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var defs []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		defs = append(defs, d)
	}
	return defs, rows.Err()
}

func ConstraintDefinitions(ctx context.Context, q Querier, schema, table string) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT conname, pg_get_constraintdef(oid)
		FROM pg_constraint
		WHERE conrelid = $1::regclass
		ORDER BY conname`, fmt.Sprintf("%s.%s", schema, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var defs []string
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			return nil, err
		}
		defs = append(defs, fmt.Sprintf("CONSTRAINT %s %s", QuoteIdent(name), def))
	}
	return defs, rows.Err()
}

func ConstraintExists(ctx context.Context, q Querier, schema, name string) (bool, error) {
	var exists bool
	err := q.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM pg_constraint c
			JOIN pg_namespace n ON n.oid = c.connamespace
			WHERE n.nspname=$1 AND c.conname=$2
		)`, schema, name).Scan(&exists)
	return exists, err
}

func ViewDefinition(ctx context.Context, q Querier, schema, view string) (string, error) {
	var def string
	err := q.QueryRow(ctx, `SELECT pg_get_viewdef($1::regclass, true)`, fmt.Sprintf("%s.%s", schema, view)).Scan(&def)
	return def, err
}

func FunctionDefinition(ctx context.Context, q Querier, schema, name string) (string, error) {
	var def string
	err := q.QueryRow(ctx, `
		SELECT pg_get_functiondef(p.oid)
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname=$1 AND p.proname=$2
		ORDER BY p.oid
		LIMIT 1`, schema, name).Scan(&def)
	return def, err
}

func TriggerDefinition(ctx context.Context, q Querier, schema, table, name string) (string, error) {
	var def string
	err := q.QueryRow(ctx, `
		SELECT pg_get_triggerdef(t.oid)
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname=$1 AND c.relname=$2 AND t.tgname=$3
		  AND NOT t.tgisinternal
		LIMIT 1`, schema, table, name).Scan(&def)
	return def, err
}

func SequenceDefinition(ctx context.Context, q Querier, schema, name string) (string, error) {
	var def string
	err := q.QueryRow(ctx, `SELECT pg_get_sequence_def($1::regclass)`, fmt.Sprintf("%s.%s", schema, name)).Scan(&def)
	if err == nil {
		return def, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code != "42883" {
		return "", err
	}
	if !errors.As(err, &pgErr) {
		return "", err
	}

	var dataType string
	var startValue, minValue, maxValue, incrementBy, cacheSize int64
	var cycle bool
	err = q.QueryRow(ctx, `
		SELECT data_type, start_value, min_value, max_value, increment_by, cache_size, cycle
		FROM pg_sequences
		WHERE schemaname=$1 AND sequencename=$2`, schema, name).Scan(
		&dataType, &startValue, &minValue, &maxValue, &incrementBy, &cacheSize, &cycle,
	)
	if err == nil {
		return buildSequenceDDL(schema, name, dataType, startValue, minValue, maxValue, incrementBy, cacheSize, cycle), nil
	}

	var lastValue, inc, minV, maxV, cacheV int64
	var isCycled bool
	err = q.QueryRow(ctx, fmt.Sprintf(
		`SELECT last_value, increment_by, min_value, max_value, cache_value, is_cycled FROM %s.%s`,
		QuoteIdent(schema), QuoteIdent(name),
	)).Scan(&lastValue, &inc, &minV, &maxV, &cacheV, &isCycled)
	if err == nil {
		return buildSequenceDDL(schema, name, "", lastValue, minV, maxV, inc, cacheV, isCycled), nil
	}

	return "", err
}

func buildSequenceDDL(schema, name, dataType string, startValue, minValue, maxValue, incrementBy, cacheSize int64, cycle bool) string {
	qualified := fmt.Sprintf("%s.%s", QuoteIdent(schema), QuoteIdent(name))
	ddl := fmt.Sprintf("CREATE SEQUENCE %s", qualified)
	if dataType != "" {
		ddl += " AS " + dataType
	}
	ddl += fmt.Sprintf(" INCREMENT BY %d", incrementBy)
	ddl += fmt.Sprintf(" MINVALUE %d", minValue)
	ddl += fmt.Sprintf(" MAXVALUE %d", maxValue)
	ddl += fmt.Sprintf(" START WITH %d", startValue)
	ddl += fmt.Sprintf(" CACHE %d", cacheSize)
	if cycle {
		ddl += " CYCLE"
	} else {
		ddl += " NO CYCLE"
	}
	return ddl
}

func EnumTypes(ctx context.Context, q Querier, schema string) ([]EnumType, error) {
	rows, err := q.Query(ctx, `
		SELECT t.typname, e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname=$1
		ORDER BY t.typname, e.enumsortorder`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[string][]string)
	for rows.Next() {
		var name, label string
		if err := rows.Scan(&name, &label); err != nil {
			return nil, err
		}
		m[name] = append(m[name], label)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	var out []EnumType
	for name, labels := range m {
		out = append(out, EnumType{Name: name, Labels: labels})
	}
	return out, nil
}

func CompositeTypes(ctx context.Context, q Querier, schema string) ([]CompositeType, error) {
	rows, err := q.Query(ctx, `
		SELECT t.typname,
			   a.attname,
			   pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
			   a.attnotnull
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_class c ON c.oid = t.typrelid
		JOIN pg_attribute a ON a.attrelid = c.oid
		WHERE n.nspname=$1
		  AND t.typtype='c'
		  AND c.relkind='c'
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY t.typname, a.attnum`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	typeMap := map[string][]ColumnInfo{}
	for rows.Next() {
		var typeName, colName, dataType string
		var notNull bool
		if err := rows.Scan(&typeName, &colName, &dataType, &notNull); err != nil {
			return nil, err
		}
		typeMap[typeName] = append(typeMap[typeName], ColumnInfo{
			Name:     colName,
			DataType: dataType,
			Nullable: !notNull,
		})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	var out []CompositeType
	for name, attrs := range typeMap {
		out = append(out, CompositeType{Name: name, Attributes: attrs})
	}
	return out, nil
}

func listNames(ctx context.Context, q Querier, query string, schema string) ([]string, error) {
	rows, err := q.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
