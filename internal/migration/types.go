package migration

import "postgreToPostgre/internal/database"

type Selection struct {
	Schemas []SchemaSelection `json:"schemas"`
}

type SchemaSelection struct {
	Name   string   `json:"name"`
	Tables []string `json:"tables"`
	Views  []string `json:"views"`
	Funcs  []string `json:"functions"`
	Seqs   []string `json:"sequences"`
}

type Options struct {
	ExistingTableMode string `json:"existingTableMode"`
	ExistingDataMode  string `json:"existingDataMode"`
	BatchSize         int    `json:"batchSize"`
	DisableFK         bool   `json:"disableFk"`
	DryRun            bool   `json:"dryRun"`
	Workers           int    `json:"workers"`
}

type Request struct {
	Source      database.ConnInfo `json:"source"`
	Destination database.ConnInfo `json:"destination"`
	Selection   Selection         `json:"selection"`
	Options     Options           `json:"options"`
}

type FailedTable struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Error  string `json:"error"`
}

type Status struct {
	Running       bool          `json:"running"`
	Overall       int           `json:"overallPercent"`
	ElapsedSec    int64         `json:"elapsedSeconds"`
	CurrentPhase  string        `json:"currentPhase"`
	LogMessage    string        `json:"logMessage"`
	TableProgress []TableStatus `json:"tables"`
	FailedTables  []FailedTable `json:"failedTables,omitempty"`
}

type TableStatus struct {
	Schema       string `json:"schema"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	TotalRows    int64  `json:"totalRows"`
	MigratedRows int64  `json:"migratedRows"`
	Percent      int    `json:"percent"`
}
