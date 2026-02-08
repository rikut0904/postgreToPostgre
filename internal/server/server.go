package server

import (
	"net/http"
	"path/filepath"

	"postgreToPostgre/internal/migration"
	"postgreToPostgre/internal/websocket"
)

type Server struct {
	hub     *websocket.Hub
	manager *migration.Manager
}

func New(hub *websocket.Hub) *Server {
	return &Server{
		hub:     hub,
		manager: migration.NewManager(hub),
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/test-connection", s.handleTestConnection)
	mux.HandleFunc("/api/browse", s.handleBrowse)
	mux.HandleFunc("/api/table-info", s.handleTableInfo)
	mux.HandleFunc("/api/migrate", s.handleMigrate)
	mux.HandleFunc("/api/migrate/cancel", s.handleCancel)
	mux.HandleFunc("/api/migrate/status", s.handleStatus)
	mux.HandleFunc("/ws/progress", s.handleWS)
	mux.HandleFunc("/help", s.handleHelp)

	staticDir := filepath.Join(".", "web")
	fileServer := http.FileServer(http.Dir(staticDir))
	mux.Handle("/", fileServer)

	return mux
}
