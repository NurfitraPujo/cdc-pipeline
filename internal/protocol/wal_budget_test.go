package protocol

import (
	"testing"
	"time"
)

func TestProjectedTimeToBreach(t *testing.T) {
	tests := []struct {
		name          string
		remaining     int64
		growthPerSec  float64
		wantOK        bool
		wantDuration  time.Duration
		toleranceSecs float64
	}{
		{
			name:          "plan section 5 worked example: 30GB budget at the 4h-ceiling rate breaches at ~4h",
			remaining:     WALBudgetBytes,
			growthPerSec:  float64(WALBudgetBytes) / MaxPauseTTL.Seconds(), // exactly the 2.1MB/s threshold
			wantOK:        true,
			wantDuration:  MaxPauseTTL,
			toleranceSecs: 1,
		},
		{
			name:         "double the threshold rate breaches in half the ceiling",
			remaining:    WALBudgetBytes,
			growthPerSec: 2 * (float64(WALBudgetBytes) / MaxPauseTTL.Seconds()),
			wantOK:       true,
			wantDuration: MaxPauseTTL / 2,
		},
		{
			name:         "zero growth rate never breaches",
			remaining:    WALBudgetBytes,
			growthPerSec: 0,
			wantOK:       false,
		},
		{
			name:         "negative growth rate (slot catching up) never breaches",
			remaining:    WALBudgetBytes,
			growthPerSec: -1000,
			wantOK:       false,
		},
		{
			name:         "budget already exhausted never projects forward",
			remaining:    0,
			growthPerSec: 1000,
			wantOK:       false,
		},
		{
			name:         "negative remaining budget never projects forward",
			remaining:    -1,
			growthPerSec: 1000,
			wantOK:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ProjectedTimeToBreach(tt.remaining, tt.growthPerSec)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			diff := got - tt.wantDuration
			if diff < 0 {
				diff = -diff
			}
			tol := time.Duration(tt.toleranceSecs) * time.Second
			if tol == 0 {
				tol = time.Second
			}
			if diff > tol {
				t.Fatalf("projected = %s, want ~%s (tolerance %s)", got, tt.wantDuration, tol)
			}
		})
	}
}

func TestMaxPauseTTLAndWALBudgetBytesMatchPlanSection5(t *testing.T) {
	// 30 GB / 4 h = 7.5 GB/h ~= 2.1 MB/s, per plan section 5.
	rate := float64(WALBudgetBytes) / MaxPauseTTL.Seconds()
	const wantMBPerSec = 2.1
	gotMBPerSec := rate / (1024 * 1024)
	diff := gotMBPerSec - wantMBPerSec
	if diff < 0 {
		diff = -diff
	}
	if diff > 0.05 {
		t.Fatalf("threshold rate = %.3f MB/s, want ~%.1f MB/s", gotMBPerSec, wantMBPerSec)
	}
}
