package protocol

import "testing"

func TestIsInternalTable(t *testing.T) {
	tests := []struct {
		tableName string
		want      bool
	}{
		{"cdc_snapshot_job", true},
		{"cdc_snapshot_chunks", true},
		{"users", false},
		{"orders", false},
		{"cdc_snapshot_", true},
		{"acdc_snapshot_", false},
	}
	for _, tt := range tests {
		t.Run(tt.tableName, func(t *testing.T) {
			if got := IsInternalTable(tt.tableName); got != tt.want {
				t.Errorf("IsInternalTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
