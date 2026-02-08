package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type ConnInfo struct {
	URL      string `json:"url"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	SSLMode  string `json:"sslMode"`
}

func (c ConnInfo) DSN() string {
	if c.URL != "" {
		return c.URL
	}
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host,
		c.Port,
		c.Database,
		c.User,
		c.Password,
		sslMode,
	)
}

func Connect(ctx context.Context, info ConnInfo) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(info.DSN())
	if err != nil {
		return nil, err
	}
	// Use simple protocol so text decoding works for types like "char".
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, cfg)
}
