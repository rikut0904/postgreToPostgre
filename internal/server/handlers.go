package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"postgreToPostgre/internal/database"
	"postgreToPostgre/internal/migration"
	ws "postgreToPostgre/internal/websocket"
)

type testConnectionRequest struct {
	Conn database.ConnInfo `json:"conn"`
}

type browseRequest struct {
	Conn database.ConnInfo `json:"conn"`
}

type browseResponse struct {
	Schemas []database.SchemaObjects `json:"schemas"`
}

type tableInfoRequest struct {
	Conn   database.ConnInfo `json:"conn"`
	Schema string            `json:"schema"`
	Table  string            `json:"table"`
}

type tableInfoResponse struct {
	Columns  []database.ColumnInfo `json:"columns"`
	RowCount int64                 `json:"rowCount"`
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req testConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	conn, err := database.Connect(ctx, req.Conn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleBrowse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req browseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	conn, err := database.Connect(ctx, req.Conn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close(ctx)

	schemas, err := database.ListSchemas(ctx, conn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var out []database.SchemaObjects
	for _, schema := range schemas {
		tables, err := database.ListTables(ctx, conn, schema)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		views, err := database.ListViews(ctx, conn, schema)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		functions, err := database.ListFunctions(ctx, conn, schema)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sequences, err := database.ListSequences(ctx, conn, schema)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		triggers, err := database.ListTriggers(ctx, conn, schema)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		out = append(out, database.SchemaObjects{
			Schema:    schema,
			Tables:    tables,
			Views:     views,
			Functions: functions,
			Sequences: sequences,
			Triggers:  triggers,
		})
	}

	writeJSON(w, browseResponse{Schemas: out})
}

func (s *Server) handleTableInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req tableInfoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	conn, err := database.Connect(ctx, req.Conn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close(ctx)

	cols, err := database.TableColumns(ctx, conn, req.Schema, req.Table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rowCount, err := database.TableRowCount(ctx, conn, req.Schema, req.Table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, tableInfoResponse{Columns: cols, RowCount: rowCount})
}

func (s *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req migration.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	err := s.manager.Start(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.manager.Cancel()
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.manager.Status())
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	client := ws.NewClient(conn)
	s.hub.Register(client)

	go func() {
		defer func() {
			s.hub.Unregister(client)
			client.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	_ = conn.SetReadDeadline(time.Now().Add(24 * time.Hour))
}

func (s *Server) handleHelp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.ServeFile(w, r, "web/help.html")
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
