package client

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/internal/common"
)

// StateManager manages persistent state for CDC cursor tracking.
type StateManager struct {
	statePath string
	state     *common.CdcCursorState
	mu        sync.RWMutex
}

// NewStateManager creates a new state manager, loading existing state if present.
func NewStateManager(statePath string) (*StateManager, error) {
	sm := &StateManager{
		statePath: statePath,
	}

	// Ensure directory exists
	dir := filepath.Dir(statePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Load existing state if present
	if data, err := os.ReadFile(statePath); err == nil {
		var state common.CdcCursorState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		sm.state = &state
		log.Info().
			Str("last_lsn", state.LastProcessedLsn.ToHexString()).
			Msg("Loaded existing cursor state")
	} else {
		log.Info().Msg("No existing state file, starting fresh")
	}

	return sm, nil
}

// GetCursor returns the current cursor state.
func (sm *StateManager) GetCursor() *common.CdcCursorState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.state == nil {
		return nil
	}

	// Return a copy
	stateCopy := *sm.state
	return &stateCopy
}

// UpdateCursor updates the cursor after a successful acknowledgment.
// This is the critical operation that ensures exactly-once semantics.
func (sm *StateManager) UpdateCursor(batchID string, newLsn common.Lsn) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	log.Debug().
		Str("batch_id", batchID).
		Str("new_lsn", newLsn.ToHexString()).
		Msg("Updating cursor")

	if sm.state == nil {
		sm.state = &common.CdcCursorState{
			TableName:        "all", // Simplified - tracking all tables together
			LastProcessedLsn: newLsn,
			PendingBatches:   []common.PendingBatch{},
			UpdatedAt:        time.Now().UTC(),
		}
	}

	// Remove the acknowledged batch from pending
	newPending := make([]common.PendingBatch, 0, len(sm.state.PendingBatches))
	for _, b := range sm.state.PendingBatches {
		if b.BatchID != batchID {
			newPending = append(newPending, b)
		}
	}
	sm.state.PendingBatches = newPending

	// Update the acknowledged LSN
	sm.state.LastAckedLsn = &newLsn
	sm.state.LastProcessedLsn = newLsn
	sm.state.UpdatedAt = time.Now().UTC()

	// Persist to disk
	if err := sm.persistState(); err != nil {
		return err
	}

	log.Debug().Msg("Cursor updated and persisted")
	return nil
}

// RecordPendingBatch records a batch as pending (sent but not yet acknowledged).
func (sm *StateManager) RecordPendingBatch(batchID string, lsn common.Lsn, sequenceCount uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.state != nil {
		sm.state.PendingBatches = append(sm.state.PendingBatches, common.PendingBatch{
			BatchID:       batchID,
			Lsn:           lsn,
			SequenceCount: sequenceCount,
			SentAt:        time.Now().UTC(),
		})
		return sm.persistState()
	}

	return nil
}

// GetPendingBatches returns batches that were sent but never acknowledged (for recovery).
func (sm *StateManager) GetPendingBatches() []common.PendingBatch {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.state == nil {
		return nil
	}

	// Return a copy
	result := make([]common.PendingBatch, len(sm.state.PendingBatches))
	copy(result, sm.state.PendingBatches)
	return result
}

func (sm *StateManager) persistState() error {
	if sm.state == nil {
		return nil
	}

	data, err := json.MarshalIndent(sm.state, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file first, then rename for atomicity
	tempPath := sm.statePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, sm.statePath)
}
