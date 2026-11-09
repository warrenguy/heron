package soakrun

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const defaultDuration = 30 * time.Minute

type Overrides struct {
	Seed     uint64
	HasSeed  bool
	Duration time.Duration
}

// ParseOverrides leaves HasSeed false for an empty seed. Seed 0 is a valid
// replay value. An empty duration selects defaultDuration.
//
// Returns an error when the seed or duration does not parse, or the duration
// is not positive.
func ParseOverrides(seedText, durationText string) (Overrides, error) {
	out := Overrides{Duration: defaultDuration}

	if seedText != "" {
		seed, err := strconv.ParseUint(seedText, 10, 64)
		if err != nil {
			return Overrides{}, fmt.Errorf("soak seed: %w", err)
		}

		out.Seed = seed
		out.HasSeed = true
	}

	if durationText != "" {
		duration, err := time.ParseDuration(durationText)
		if err != nil {
			return Overrides{}, fmt.Errorf("soak duration: %w", err)
		}

		if duration <= 0 {
			return Overrides{}, fmt.Errorf("soak duration %s must be positive", durationText)
		}

		out.Duration = duration
	}

	return out, nil
}

func UnplannedElections(firstTerm, finalTerm int64, leaderFaults int) int64 {
	return max(finalTerm-firstTerm-int64(leaderFaults), 0)
}

// Returned, NonEmptyGets, and Reopens stay nil when the client does not stop. A
// stopped client stores each count, including zero.
type Result struct {
	Seed               uint64            `json:"seed"`
	Duration           string            `json:"duration"`
	Steps              map[string]int    `json:"steps"`
	Returned           *int              `json:"returned,omitempty"`
	NonEmptyGets       *int              `json:"nonEmptyGets,omitempty"`
	Reopens            *int              `json:"reopens,omitempty"`
	UnplannedElections int64             `json:"unplannedElections"`
	LongestRecovery    map[string]string `json:"longestRecovery"`
}

const seedFileName = "seed.txt"

// WriteSeedFile syncs the seed file before it returns. A test timeout skips
// cleanups. The seed must already be on disk.
func WriteSeedFile(dir string, seed uint64) error {
	path := filepath.Join(dir, seedFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("write soak seed: %w", err)
	}

	defer func() { _ = file.Close() }()

	_, err = fmt.Fprintf(file, "%d\n", seed)
	if err != nil {
		return fmt.Errorf("write soak seed: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("write soak seed: %w", err)
	}

	return nil
}
