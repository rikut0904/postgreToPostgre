package migration

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"
)

var ErrAlreadyRunning = errors.New("migration already running")

type Manager struct {
	mu       sync.Mutex
	running  bool
	cancel   context.CancelFunc
	status   Status
	progress *Progress
	sink     Broadcaster
}

func NewManager(sink Broadcaster) *Manager {
	return &Manager{sink: sink}
}

func (m *Manager) Start(req Request) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return ErrAlreadyRunning
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.running = true
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			m.running = false
			m.cancel = nil
			if m.progress != nil {
				m.status = m.progress.Snapshot()
			}
			m.mu.Unlock()
		}()

		logFile, err := os.OpenFile("migration.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Printf("log file error: %v", err)
		}
		logger := log.New(logFile, "", log.LstdFlags)
		if logFile != nil {
			defer logFile.Close()
		}

		migrator := NewMigrator(m.sink, logger)
		migrator.WithProgressHook(func(p *Progress) {
			m.AttachProgress(p)
		})
		if err := migrator.Run(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				if m.progress != nil {
					m.progress.FinishWithError("canceled")
				}
			} else if m.progress != nil {
				m.progress.FinishWithError(err.Error())
			}
			logger.Printf("migration failed: %v", err)
		}
	}()

	return nil
}

func (m *Manager) Cancel() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		if m.progress != nil {
			m.progress.Log("cancel requested")
		}
		m.cancel()
	}
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.progress != nil {
		m.status = m.progress.Snapshot()
	}
	return m.status
}

func (m *Manager) AttachProgress(p *Progress) {
	m.mu.Lock()
	m.progress = p
	m.status = p.Snapshot()
	m.mu.Unlock()
}

func (m *Manager) WaitForStart(timeout time.Duration) bool {
	start := time.Now()
	for {
		m.mu.Lock()
		running := m.running
		m.mu.Unlock()
		if running {
			return true
		}
		if time.Since(start) > timeout {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
