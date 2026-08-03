package api

import "testing"

func TestMaskDSNEmitsAReadableMask(t *testing.T) {
	got := maskDSN("databend://root:hunter2@localhost:8000/default?sslmode=disable") //nolint:gosec // fixture DSN; this file exists to test password masking

	// net/url percent-encodes "*" in userinfo, which used to surface in the
	// sink list as "root:%2A%2A%2A@..." -- unrecognisable as a mask.
	want := "databend://root:***@localhost:8000/default?sslmode=disable" //nolint:gosec // fixture DSN; this file exists to test password masking
	if got != want {
		t.Errorf("maskDSN =\n %q\nwant %q", got, want)
	}
}

func TestMaskDSNLeavesPasswordlessDSNAlone(t *testing.T) {
	in := "databend://root@localhost:8000/default"
	if got := maskDSN(in); got != in {
		t.Errorf("maskDSN = %q, want it unchanged", got)
	}
}

func TestMaskDSNPassesThroughUnparseableInput(t *testing.T) {
	in := "not a url at all"
	if got := maskDSN(in); got != in {
		t.Errorf("maskDSN = %q, want it unchanged", got)
	}
}

// The round trip an edit-form save actually performs: read (masked), send the
// masked value back untouched, and get the real password restored.
func TestMaskedDSNRoundTripsThroughReconstruct(t *testing.T) {
	original := "databend://root:hunter2@localhost:8000/default?sslmode=disable" //nolint:gosec // fixture DSN; this file exists to test password masking

	masked := maskDSN(original)
	if masked == original {
		t.Fatal("password was not masked")
	}

	got := reconstructDSN(masked, original)
	if got != original {
		t.Errorf("reconstructDSN =\n %q\nwant %q", got, original)
	}
}

// Older responses emitted the percent-encoded mask; a client that echoes one
// back must still be understood, or its sink's password is destroyed.
func TestReconstructAcceptsPercentEncodedMask(t *testing.T) {
	original := "databend://root:hunter2@localhost:8000/default" //nolint:gosec // fixture DSN; this file exists to test password masking
	legacy := "databend://root:%2A%2A%2A@localhost:8000/default" //nolint:gosec // fixture DSN; this file exists to test password masking

	if got := reconstructDSN(legacy, original); got != original {
		t.Errorf("reconstructDSN =\n %q\nwant %q", got, original)
	}
}

// A genuinely new password must be stored, not silently replaced by the old one.
func TestReconstructKeepsARealNewPassword(t *testing.T) {
	original := "databend://root:hunter2@localhost:8000/default"  //nolint:gosec // fixture DSN; this file exists to test password masking
	updated := "databend://root:newsecret@localhost:8000/default" //nolint:gosec // fixture DSN; this file exists to test password masking

	if got := reconstructDSN(updated, original); got != updated {
		t.Errorf("reconstructDSN = %q, want the new DSN %q", got, updated)
	}
}
