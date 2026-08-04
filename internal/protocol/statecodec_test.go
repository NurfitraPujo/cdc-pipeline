package protocol

import (
	"encoding/json"
	"testing"
	"time"
)

// The defect this guards: msgp-encoded state was read back with json.Unmarshal,
// which fails and leaves the struct zero-valued. Assert the failure directly so
// the premise of UnmarshalState's sniffing does not silently stop being true.
func TestJSONCannotDecodeMsgpState(t *testing.T) {
	st := TableStats{Status: "ACTIVE", TotalSynced: 42}
	data, err := st.MarshalMsg(nil)
	if err != nil {
		t.Fatalf("MarshalMsg: %v", err)
	}

	var decoded TableStats
	if err := json.Unmarshal(data, &decoded); err == nil {
		t.Fatal("json.Unmarshal accepted msgp bytes; the encoding split is no longer detectable")
	}
	if decoded.TotalSynced != 0 {
		t.Fatalf("expected zero-valued struct after failed decode, got %d", decoded.TotalSynced)
	}
}

func TestStateRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	t.Run("TableStats", func(t *testing.T) {
		want := TableStats{
			Status:          "ACTIVE",
			RPS:             12.5,
			TotalSynced:     9001,
			ErrorCount:      3,
			LastSourceTS:    now,
			LastProcessedTS: now,
			LagMS:           250,
			UpdatedAt:       now,
		}
		data, err := MarshalState(&want)
		if err != nil {
			t.Fatalf("MarshalState: %v", err)
		}
		var got TableStats
		if err := UnmarshalState(data, &got); err != nil {
			t.Fatalf("UnmarshalState: %v", err)
		}
		if got.TotalSynced != want.TotalSynced || got.Status != want.Status || got.LagMS != want.LagMS {
			t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
		}
	})

	t.Run("Checkpoint", func(t *testing.T) {
		want := Checkpoint{IngressLSN: 7, EgressLSN: 11, LastPK: `{"id":5}`, Status: "ACTIVE", UpdatedAt: now}
		data, err := MarshalState(&want)
		if err != nil {
			t.Fatalf("MarshalState: %v", err)
		}
		var got Checkpoint
		if err := UnmarshalState(data, &got); err != nil {
			t.Fatalf("UnmarshalState: %v", err)
		}
		// Compare UpdatedAt with Equal: msgp restores the instant but not the
		// *time.Location pointer, so == on the struct is spuriously false.
		if !got.UpdatedAt.Equal(want.UpdatedAt) {
			t.Fatalf("UpdatedAt = %v, want %v", got.UpdatedAt, want.UpdatedAt)
		}
		got.UpdatedAt, want.UpdatedAt = time.Time{}, time.Time{}
		if got != want {
			t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
		}
	})
}

// Deployments that ran before the encodings were unified still hold
// JSON-written stats under keys now read as msgp.
func TestUnmarshalStateAcceptsLegacyJSON(t *testing.T) {
	data, err := json.Marshal(TableStats{Status: "ACTIVE", TotalSynced: 77})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var got TableStats
	if err := UnmarshalState(data, &got); err != nil {
		t.Fatalf("UnmarshalState on legacy JSON: %v", err)
	}
	if got.TotalSynced != 77 {
		t.Fatalf("TotalSynced = %d, want 77", got.TotalSynced)
	}
}

func TestUnmarshalStateReportsGarbage(t *testing.T) {
	var got TableStats
	if err := UnmarshalState([]byte("not encoded at all"), &got); err == nil {
		t.Fatal("expected an error for undecodable bytes, got nil")
	}
}
