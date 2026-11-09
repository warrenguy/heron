package soakrun

import (
	"errors"
	"fmt"
)

// TermRow is one TERM record from a recording log.
type TermRow struct {
	Term                int64
	TermBaseLogPosition int64
	LogPosition         int64
}

// CheckTermRows checks the TERM chain in each file and the final row across
// files. Earlier rows may differ between files.
//
// Returns an error when:
//   - files is empty
//   - a file has no rows
//   - a file does not hold one row per term from 0
//   - a LogPosition differs from the next row's base
//   - the final terms differ
//   - the final bases differ
func CheckTermRows(files [][]TermRow) error {
	if len(files) == 0 {
		return errors.New("term rows: no files")
	}

	finalTerms := make([]int64, len(files))
	finalBases := make([]int64, len(files))

	for i, rows := range files {
		if err := checkTermFile(rows); err != nil {
			return fmt.Errorf("term rows: file %d: %w", i, err)
		}

		last := rows[len(rows)-1]
		finalTerms[i] = last.Term
		finalBases[i] = last.TermBaseLogPosition
	}

	for i := 1; i < len(files); i++ {
		if finalTerms[i] != finalTerms[0] {
			return fmt.Errorf("term rows: final terms %v", finalTerms)
		}

		if finalBases[i] != finalBases[0] {
			return fmt.Errorf("term rows: final bases %v", finalBases)
		}
	}
	return nil
}

func checkTermFile(rows []TermRow) error {
	if len(rows) == 0 {
		return errors.New("no rows")
	}

	for i, row := range rows {
		if row.Term != int64(i) {
			return fmt.Errorf("term %d at index %d", row.Term, i)
		}

		if i == 0 {
			continue
		}

		prev := rows[i-1]
		if prev.LogPosition != row.TermBaseLogPosition {
			return fmt.Errorf(
				"term %d log position %d differs from term %d base %d",
				prev.Term,
				prev.LogPosition,
				row.Term,
				row.TermBaseLogPosition,
			)
		}
	}
	return nil
}
