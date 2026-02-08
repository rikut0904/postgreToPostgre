package main

import (
	"log"
	"net/http"
	"os"

	"postgreToPostgre/internal/server"
	"postgreToPostgre/internal/websocket"
)

func main() {
	addr := ":8081"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}

	hub := websocket.NewHub()
	go hub.Run()

	srv := server.New(hub)

	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, srv.Routes()); err != nil {
		log.Fatal(err)
	}
}
