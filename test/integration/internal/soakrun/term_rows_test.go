package soakrun

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckTermRows(t *testing.T) {
	t.Parallel()

	equal := []TermRow{
		{Term: 0, TermBaseLogPosition: 0, LogPosition: 100},
		{Term: 1, TermBaseLogPosition: 100, LogPosition: -1},
	}

	tests := []struct {
		name    string
		files   [][]TermRow
		wantErr string
	}{
		{
			name:  "equal files",
			files: [][]TermRow{equal, equal, equal},
		},
		{
			name: "earlier rows differ",
			files: [][]TermRow{
				{{0, 0, 13344}, {1, 13344, 13344}, {2, 13344, 13536}, {3, 13536, 26304}, {4, 26304, -1}},
				{{0, 0, 13344}, {1, 13344, 13344}, {2, 13344, 26304}, {3, 26304, 26304}, {4, 26304, -1}},
				{{0, 0, 192}, {1, 192, 13344}, {2, 13344, 26304}, {3, 26304, 26304}, {4, 26304, -1}},
			},
		},
		{
			name: "gap",
			files: [][]TermRow{
				{
					{Term: 0, TermBaseLogPosition: 0, LogPosition: 10},
					{Term: 2, TermBaseLogPosition: 10, LogPosition: -1},
				},
				equal,
				equal,
			},
			wantErr: "term rows: file 0: term 2 at index 1",
		},
		{
			name: "log position differs from next base",
			files: [][]TermRow{
				{
					{Term: 0, TermBaseLogPosition: 0, LogPosition: 10},
					{Term: 1, TermBaseLogPosition: 11, LogPosition: -1},
				},
				equal,
				equal,
			},
			wantErr: "term rows: file 0: term 0 log position 10 differs from term 1 base 11",
		},
		{
			name: "different final bases",
			files: [][]TermRow{
				{{Term: 0, TermBaseLogPosition: 0, LogPosition: -1}},
				{{Term: 0, TermBaseLogPosition: 8, LogPosition: -1}},
				{{Term: 0, TermBaseLogPosition: 0, LogPosition: -1}},
			},
			wantErr: "term rows: final bases [0 8 0]",
		},
		{
			name: "different final terms",
			files: [][]TermRow{
				equal,
				{{Term: 0, TermBaseLogPosition: 0, LogPosition: -1}},
				equal,
			},
			wantErr: "term rows: final terms [1 0 1]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := CheckTermRows(test.files)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.EqualError(t, err, test.wantErr)
		})
	}
}
