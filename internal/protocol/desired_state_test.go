package protocol

import "testing"

// TestEffectiveDesiredState locks in the backward-compat rule from plan
// section 4.1: a PipelineConfig written before desired_state existed has an
// empty field and must still mean "running" -- WS-1's whole point is to add
// this concept without silently idling every pipeline configured before the
// field was introduced.
func TestEffectiveDesiredState(t *testing.T) {
	cases := []struct {
		name string
		in   DesiredState
		want DesiredState
	}{
		{"empty means running", "", DesiredStateRunning},
		{"running stays running", DesiredStateRunning, DesiredStateRunning},
		{"paused stays paused", DesiredStatePaused, DesiredStatePaused},
		{"stopped stays stopped", DesiredStateStopped, DesiredStateStopped},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validPipelineConfig()
			cfg.DesiredState = tc.in
			if got := cfg.EffectiveDesiredState(); got != tc.want {
				t.Errorf("EffectiveDesiredState() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPipelineConfigValidateDesiredState(t *testing.T) {
	cases := []struct {
		name    string
		state   DesiredState
		wantErr bool
	}{
		{"empty is valid (pre-WS-1 configs)", "", false},
		{"running is valid", DesiredStateRunning, false},
		{"paused is valid", DesiredStatePaused, false},
		{"stopped is valid", DesiredStateStopped, false},
		{"garbage is rejected", DesiredState("halted"), true},
		{"case-sensitive: Running is rejected", DesiredState("Running"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validPipelineConfig()
			cfg.DesiredState = tc.state
			err := cfg.Validate()
			if tc.wantErr && err == nil {
				t.Errorf("Validate() with desired_state=%q: expected error, got nil", tc.state)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Validate() with desired_state=%q: unexpected error: %v", tc.state, err)
			}
		})
	}
}

func validPipelineConfig() PipelineConfig {
	return PipelineConfig{
		ID:      "p1",
		Name:    "pipeline one",
		Sources: []string{"s1"},
		Sinks:   []string{"snk1"},
	}
}
