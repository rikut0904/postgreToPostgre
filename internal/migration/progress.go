package migration

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Broadcaster interface {
	Broadcast([]byte)
}

type Progress struct {
	mu        sync.Mutex
	startedAt time.Time
	status    Status
	sink      Broadcaster
}

type wsMessage struct {
	Type string `json:"type"`
	Data Status `json:"data"`
}

func NewProgress(sink Broadcaster, tables []TableStatus) *Progress {
	return &Progress{
		startedAt: time.Now(),
		status: Status{
			Running:       true,
			Overall:       0,
			ElapsedSec:    0,
			CurrentPhase:  "init",
			LogMessage:    "",
			TableProgress: tables,
		},
		sink: sink,
	}
}

func (p *Progress) Snapshot() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status
}

func (p *Progress) SetPhase(phase string) {
	p.mu.Lock()
	p.status.CurrentPhase = phase
	p.status.ElapsedSec = int64(time.Since(p.startedAt).Seconds())
	p.mu.Unlock()
	p.emit()
}

func (p *Progress) Log(msg string) {
	p.mu.Lock()
	p.status.LogMessage = msg
	p.status.ElapsedSec = int64(time.Since(p.startedAt).Seconds())
	p.mu.Unlock()
	p.emit()
}

func (p *Progress) UpdateOverall(percent int) {
	p.mu.Lock()
	p.status.Overall = percent
	p.status.ElapsedSec = int64(time.Since(p.startedAt).Seconds())
	p.mu.Unlock()
	p.emit()
}

func (p *Progress) UpdateTable(schema, name, status string, total, migrated int64) {
	p.mu.Lock()
	for i := range p.status.TableProgress {
		t := &p.status.TableProgress[i]
		if t.Schema == schema && t.Name == name {
			t.Status = status
			t.TotalRows = total
			t.MigratedRows = migrated
			if total > 0 {
				t.Percent = int(float64(migrated) / float64(total) * 100.0)
			} else {
				t.Percent = 0
			}
			break
		}
	}
	p.status.ElapsedSec = int64(time.Since(p.startedAt).Seconds())
	p.mu.Unlock()
	p.emit()
}

func (p *Progress) AddFailedTable(schema, name, errMsg string) {
	p.mu.Lock()
	p.status.FailedTables = append(p.status.FailedTables, FailedTable{
		Schema: schema,
		Name:   name,
		Error:  errMsg,
	})
	p.status.ElapsedSec = int64(time.Since(p.startedAt).Seconds())
	p.mu.Unlock()
	p.emit()
}

func (p *Progress) FinishWithError(errMsg string) {
	p.mu.Lock()
	p.status.Running = false
	p.status.ElapsedSec = int64(time.Since(p.startedAt).Seconds())
	p.status.LogMessage = fmt.Sprintf("migration failed: %s (elapsed %ds)", errMsg, p.status.ElapsedSec)
	p.mu.Unlock()
	p.emit()
}

func (p *Progress) Finish() {
	p.mu.Lock()
	p.status.Running = false
	p.status.Overall = 100
	p.status.ElapsedSec = int64(time.Since(p.startedAt).Seconds())
	p.status.LogMessage = fmt.Sprintf("migration completed (elapsed %ds)", p.status.ElapsedSec)
	p.mu.Unlock()
	p.emit()
}

func (p *Progress) emit() {
	if p.sink == nil {
		return
	}
	p.mu.Lock()
	status := p.status
	p.mu.Unlock()

	payload, err := json.Marshal(wsMessage{
		Type: "progress",
		Data: status,
	})
	if err != nil {
		return
	}
	p.sink.Broadcast(payload)
}
