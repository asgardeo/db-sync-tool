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
	// state is a map of source name -> cursor state
	state map[string]*common.CdcCursorState
	mu    sync.RWMutex
}

// NewStateManager creates a new state manager, loading existing state if present.
func NewStateManager(statePath string) (*StateManager, error) {
	sm := &StateManager{
		statePath: statePath,
		state:     make(map[string]*common.CdcCursorState),
	}

	// Ensure directory exists
	dir := filepath.Dir(statePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Load existing state if present
	if data, err := os.ReadFile(statePath); err == nil {
		// Try to unmarshal as map first
		var multiState map[string]*common.CdcCursorState
		if err := json.Unmarshal(data, &multiState); err == nil && len(multiState) > 0 {
			sm.state = multiState
			log.Info().Int("count", len(multiState)).Msg("Loaded existing multi-cursor state")
		} else {
			// Fallback: try unmarshal as single state (legacy migration)
			var singleState common.CdcCursorState
			if err := json.Unmarshal(data, &singleState); err == nil && !singleState.UpdatedAt.IsZero() {
				// Assign to "default" source
				sm.state["default"] = &singleState
				log.Info().
					Str("last_lsn", singleState.LastProcessedLsn.ToHexString()).
					Msg("Loaded existing legacy cursor state as 'default'")
			} else {
				// If both fail, start fresh (or file is empty/corrupt)
				log.Warn().Msg("Could not parse state file, starting fresh")
			}
		}
	} else {
		log.Info().Msg("No existing state file, starting fresh")
	}

	return sm, nil
}

// GetCursor returns the current cursor state for a given source.
func (sm *StateManager) GetCursor(sourceName string) *common.CdcCursorState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	state, ok := sm.state[sourceName]
	if !ok || state == nil {
		return nil
	}

	// Return a copy
	stateCopy := *state
	return &stateCopy
}

// UpdateCursor updates the cursor for a specific source after a successful acknowledgment.
func (sm *StateManager) UpdateCursor(sourceName string, batchID string, newLsn common.Lsn) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	log.Debug().
		Str("source", sourceName).
		Str("batch_id", batchID).
		Str("new_lsn", newLsn.ToHexString()).
		Msg("Updating cursor")

	state, ok := sm.state[sourceName]
	if !ok || state == nil {
		sm.state[sourceName] = &common.CdcCursorState{
			TableName:        "all", // Simplified - tracking all tables together per source
			LastProcessedLsn: newLsn,
			PendingBatches:   []common.PendingBatch{},
			UpdatedAt:        time.Now().UTC(),
		}
		state = sm.state[sourceName]
	}

	// Remove the acknowledged batch from pending
	newPending := make([]common.PendingBatch, 0, len(state.PendingBatches))
	for _, b := range state.PendingBatches {
		if b.BatchID != batchID {
			newPending = append(newPending, b)
		}
	}
	state.PendingBatches = newPending

	// Update the acknowledged LSN
	state.LastAckedLsn = &newLsn
	state.LastProcessedLsn = newLsn
	state.UpdatedAt = time.Now().UTC()

	// Persist to disk
	if err := sm.persistState(); err != nil {
		return err
	}

	log.Debug().Msg("Cursor updated and persisted")
	return nil
}

// RecordPendingBatch records a batch as pending for a specific source.
func (sm *StateManager) RecordPendingBatch(sourceName string, batchID string, lsn common.Lsn, sequenceCount uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	state, ok := sm.state[sourceName]
	if !ok || state == nil {
		// Initialize state if not exists, though usually GetCursor would be called first
		state = &common.CdcCursorState{
			TableName:        "all",
			LastProcessedLsn: lsn, // Temporarily set
			UpdatedAt:        time.Now().UTC(),
		}
		sm.state[sourceName] = state
	}

	state.PendingBatches = append(state.PendingBatches, common.PendingBatch{
		BatchID:       batchID,
		Lsn:           lsn,
		SequenceCount: sequenceCount,
		SentAt:        time.Now().UTC(),
	})
	return sm.persistState()
}

// GetPendingBatches returns batches that were sent but never acknowledged (for recovery).
// Returns a map of sourceName -> []PendingBatch
func (sm *StateManager) GetPendingBatches() map[string][]common.PendingBatch {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string][]common.PendingBatch)
	for source, state := range sm.state {
		if len(state.PendingBatches) > 0 {
			batches := make([]common.PendingBatch, len(state.PendingBatches))
			copy(batches, state.PendingBatches)
			result[source] = batches
		}
	}
	return result
}

// persistState writes the current state to disk atomically.
// Callers must hold sm.mu (Lock or RLock) — sync.RWMutex is not reentrant.
func (sm *StateManager) persistState() error {
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
