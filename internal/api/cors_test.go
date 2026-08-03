package api

import "testing"

func TestAllowedOriginsDefaults(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "")

	got := allowedOrigins()
	for _, want := range defaultAllowedOrigins {
		if !got[want] {
			t.Errorf("default origin %q missing", want)
		}
	}
	if got["http://localhost:9999"] {
		t.Error("an unlisted origin must not be allowed")
	}
}

func TestAllowedOriginsFromEnvReplacesDefaults(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "http://localhost:3100, https://example.test ")

	got := allowedOrigins()

	if !got["http://localhost:3100"] {
		t.Error("configured origin missing")
	}
	// Surrounding whitespace must be tolerated -- comma-separated env lists
	// are routinely written with spaces.
	if !got["https://example.test"] {
		t.Error("configured origin with surrounding spaces missing")
	}
	// The env var replaces rather than extends, so a caller can lock a
	// deployment down to exactly one origin.
	if got["http://localhost:3000"] {
		t.Error("defaults must not survive an explicit CORS_ALLOWED_ORIGINS")
	}
}

func TestAllowedOriginsIgnoresEmptyEntries(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "http://a.test,,  ,http://b.test")

	got := allowedOrigins()

	if len(got) != 2 {
		t.Errorf("got %d origins, want 2: %v", len(got), got)
	}
	if got[""] {
		t.Error("the empty origin must never be allowed")
	}
}
