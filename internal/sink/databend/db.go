package databend

import (
	"context"
	"database/sql"
)

// DBExec is the minimal database interface used by the Databend sink. It is
// satisfied by *sql.DB through sqlDBAdapter so unit tests can swap in a
// lightweight in-memory implementation.
type DBExec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (DBRows, error)
	QueryRowScan(ctx context.Context, query string, args []any, dest ...any) error
	Close() error
}

// DBRows is the minimal abstraction over *sql.Rows used by the sink. Tests can
// implement this directly to avoid pulling in a real database driver.
type DBRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// sqlDBAdapter wraps *sql.DB so it satisfies DBExec. We avoid an extra wrapper
// type for normal production wiring because the tests do not need to substitute
// any behaviour for ExecContext / QueryContext / Close; they only need to
// intercept QueryRowScan for SHOW CREATE TABLE.
type sqlDBAdapter struct {
	DB *sql.DB
}

// ExecContext delegates to the underlying *sql.DB.
func (a sqlDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.DB.ExecContext(ctx, query, args...)
}

// QueryContext delegates to the underlying *sql.DB and wraps the returned
// *sql.Rows so the sink does not directly depend on the unexported fields of
// database/sql.Rows in its public surface area.
func (a sqlDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (DBRows, error) {
	rows, err := a.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return emptyDBRows{}, nil
	}
	return sqlRowsAdapter{r: rows}, nil
}

// QueryRowScan wraps QueryRowContext().Scan so tests can stub single-row
// reads (notably SHOW CREATE TABLE) without constructing a fake *sql.Row.
func (a sqlDBAdapter) QueryRowScan(ctx context.Context, query string, args []any, dest ...any) error {
	return a.DB.QueryRowContext(ctx, query, args...).Scan(dest...)
}

// Close delegates to the underlying *sql.DB.
func (a sqlDBAdapter) Close() error {
	return a.DB.Close()
}

type sqlRowsAdapter struct {
	r *sql.Rows
}

func (a sqlRowsAdapter) Next() bool             { return a.r.Next() }
func (a sqlRowsAdapter) Scan(dest ...any) error { return a.r.Scan(dest...) }
func (a sqlRowsAdapter) Close() error           { return a.r.Close() }
func (a sqlRowsAdapter) Err() error             { return a.r.Err() }

// emptyDBRows is returned when the underlying QueryContext yields no rows
// (which can happen for SHOW CREATE TABLE on a missing table on some drivers).
type emptyDBRows struct{}

func (emptyDBRows) Next() bool          { return false }
func (emptyDBRows) Scan(_ ...any) error { return nil }
func (emptyDBRows) Close() error        { return nil }
func (emptyDBRows) Err() error          { return nil }
