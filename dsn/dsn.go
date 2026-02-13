package dsn

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// DetectDriver infers the database driver name from a DSN string.
//
//   - "postgres://" or "postgresql://" prefix -> "pgx"
//   - Contains "@" (MySQL-style user:pass@tcp(...)/db) -> "mysql"
//   - Contains "=" but not "@" (PostgreSQL key=value style) -> "pgx"
//   - Otherwise -> error
func DetectDriver(raw string) (string, error) {
	if raw == "" {
		return "", errors.New("dsn: empty DSN")
	}

	lower := strings.ToLower(raw)
	switch {
	case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
		return "pgx", nil
	case strings.Contains(raw, "@"):
		return "mysql", nil
	case strings.Contains(raw, "="):
		return "pgx", nil
	}

	return "", fmt.Errorf("dsn: cannot detect driver from: %s", raw)
}

// Open detects the driver from the DSN and opens a *sql.DB.
func Open(raw string) (*sql.DB, error) {
	driver, err := DetectDriver(raw)
	if err != nil {
		return nil, err
	}

	openDSN := raw
	if driver == "mysql" {
		openDSN = strings.TrimPrefix(openDSN, "mysql://")
	}

	db, err := sql.Open(driver, openDSN)
	if err != nil {
		return nil, fmt.Errorf("dsn: open: %w", err)
	}
	return db, nil
}
