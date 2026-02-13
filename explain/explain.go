package explain

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Mode selects between EXPLAIN and EXPLAIN ANALYZE.
type Mode int

const (
	Explain Mode = iota // EXPLAIN (plan only)
	Analyze             // EXPLAIN ANALYZE (plan + actual execution)
)

func (m Mode) String() string {
	switch m {
	case Explain:
		return "EXPLAIN"
	case Analyze:
		return "EXPLAIN ANALYZE"
	}
	return "EXPLAIN"
}

func (m Mode) prefix() string {
	switch m {
	case Explain:
		return "EXPLAIN "
	case Analyze:
		return "EXPLAIN ANALYZE "
	}
	return "EXPLAIN "
}

// Result holds the output of an EXPLAIN query.
type Result struct {
	Plan     string
	Duration time.Duration
}

// Client wraps a database connection for running EXPLAIN queries.
type Client struct {
	db *sql.DB
}

// NewClient creates a new Client from an existing *sql.DB.
func NewClient(db *sql.DB) *Client {
	return &Client{db: db}
}

// Run executes EXPLAIN or EXPLAIN ANALYZE for the given query with optional args.
func (c *Client) Run(ctx context.Context, mode Mode, query string, args []string) (*Result, error) {
	anyArgs := make([]any, len(args))
	for i, a := range args {
		anyArgs[i] = a
	}

	start := time.Now()
	rows, err := c.db.QueryContext(ctx, mode.prefix()+query, anyArgs...)
	if err != nil {
		return nil, fmt.Errorf("explain: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("explain: scan: %w", err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("explain: rows: %w", err)
	}

	return &Result{
		Plan:     strings.Join(lines, "\n"),
		Duration: time.Since(start),
	}, nil
}

// Close closes the underlying database connection.
func (c *Client) Close() error {
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("explain: close: %w", err)
	}
	return nil
}
