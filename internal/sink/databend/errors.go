package databend

import (
	"errors"
	"fmt"
	"strings"
)

// DDLError wraps a DDL (or DDL-adjacent, e.g. database-existence) failure
// from Databend with a permanent/transient classification.
// MULTI_SCHEMA_PLAN.md §7.4 item 9 / §6: permanent failures (unknown
// database, unknown table, syntax, privilege) will never succeed on retry
// and must be dead-lettered rather than redelivered forever; transient
// failures (connection reset, timeout) are safe to retry as-is.
//
// Today only this package inspects DDLError (via IsPermanentDDLError); the
// engine-side wiring that would turn a permanent DDLError into "mark table
// Failed, dead-letter, stop nacking" is out of this stage's file scope
// (internal/sink/** only) -- see the stage report for detail. The type is
// exported so that wiring is a mechanical follow-up rather than a redesign.
type DDLError struct {
	Target    string // TableRef.String() or bare database name, for logging/DLQ context
	Permanent bool
	Err       error
}

func (e *DDLError) Error() string {
	kind := "transient"
	if e.Permanent {
		kind = "permanent"
	}
	return fmt.Sprintf("databend DDL error (%s) for %s: %v", kind, e.Target, e.Err)
}

func (e *DDLError) Unwrap() error { return e.Err }

// IsPermanentDDLError reports whether err (or anything it wraps, including
// errors.Join trees) is a permanent DDL failure that must not be retried.
func IsPermanentDDLError(err error) bool {
	var ddlErr *DDLError
	return errors.As(err, &ddlErr) && ddlErr.Permanent
}

// permanentDDLMarkers are substrings of Databend driver error text that
// indicate the failure will never succeed on retry. Databend error codes
// verified empirically against datafuselabs/databend:latest
// (MULTI_SCHEMA_PLAN.md §6): 1003 unknown database, 1025 unknown table. We
// also match the codes' English text and syntax/privilege errors, since the
// Go driver surfaces both a numeric code and a message and we do not depend
// on one specific rendering.
// A bare "1003"/"1025" substring is NOT safe: those digits occur incidentally
// in byte counts, ports, row counts, LSNs and longer codes ("21003"), which
// would misclassify a retryable failure as permanent and dead-letter it --
// silent data loss. Match only code-shaped renderings.
var permanentDDLMarkers = []string{
	"code: 1003", "code:1003", "[1003]", // unknown database
	"code: 1025", "code:1025", "[1025]", // unknown table
	"unknown database",
	"unknown table",
	"syntax error",
	"access denied",
	"permission denied",
	"privilege",
}

// classifyDDLError inspects a raw driver error and wraps it as a DDLError,
// classified permanent or transient per permanentDDLMarkers. Returns nil for
// a nil input so callers can use it unconditionally on the result of
// ExecContext/QueryContext.
func classifyDDLError(target string, err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	permanent := false
	for _, marker := range permanentDDLMarkers {
		if strings.Contains(msg, marker) {
			permanent = true
			break
		}
	}
	return &DDLError{Target: target, Permanent: permanent, Err: err}
}
