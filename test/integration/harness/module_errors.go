//go:build integration

package harness

import (
	"errors"

	"luas.fast/cluster"
)

// ModuleErrors keeps each error that a member's cluster.Config.ErrorHandler
// receives, keyed by member id.
//
// Concurrency: call from the goroutine that ticks the members. A test that
// runs Runner.Run on another goroutine must receive the Run result before it
// reads the store.
type ModuleErrors struct{ byMember map[int32][]error }

// NewModuleErrors returns an empty store.
func NewModuleErrors() *ModuleErrors {
	return &ModuleErrors{byMember: make(map[int32][]error)}
}

// AmendConfig sets cfg.ErrorHandler to keep errors under cfg.MemberID. Use it
// as ClusterMemberSetConfig.AmendMemberConfig.
//
// Behaviour: kept errors survive a restart that reuses the returned config.
func (r *ModuleErrors) AmendConfig(cfg cluster.Config) cluster.Config {
	memberID := cfg.MemberID
	cfg.ErrorHandler = func(err error) {
		r.byMember[memberID] = append(r.byMember[memberID], err)
	}
	return cfg
}

// Count returns how many kept errors of memberID match target.
func (r *ModuleErrors) Count(memberID int32, target error) int {
	count := 0
	for _, err := range r.byMember[memberID] {
		if errors.Is(err, target) {
			count++
		}
	}
	return count
}

// MatchAt returns the kept error of memberID at index among those that match
// target.
//
// Returns: nil when target has index or fewer matches.
func (r *ModuleErrors) MatchAt(memberID int32, target error, index int) error {
	for _, err := range r.byMember[memberID] {
		if !errors.Is(err, target) {
			continue
		}
		if index == 0 {
			return err
		}
		index--
	}
	return nil
}
