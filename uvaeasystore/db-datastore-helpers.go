//
//
//

package uvaeasystore

import (
	"database/sql"
	"errors"
	"fmt"
	sqlite3 "github.com/mattn/go-sqlite3"
	"log"
	"strings"
)

func execPreparedBy2(stmt *sql.Stmt, value1 string, value2 string) error {
	_, err := stmt.Exec(value1, value2)
	return errorMapper(err)
}

func execPreparedBy3(stmt *sql.Stmt, value1 string, value2 string, value3 string) error {
	_, err := stmt.Exec(value1, value2, value3)
	return errorMapper(err)
}

func execPreparedBy4(stmt *sql.Stmt, value1 string, value2 string, value3 string, value4 string) error {
	_, err := stmt.Exec(value1, value2, value3, value4)
	return errorMapper(err)
}

func objectQueryResults(rows *sql.Rows, log *log.Logger) (EasyStoreObject, error) {
	results := easyStoreObjectImpl{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.namespace, &results.id, &results.vtag, &results.created, &results.modified)
		if err != nil {
			return nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrNotFound)
	}

	logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return &results, nil
}

func fieldQueryResults(rows *sql.Rows, log *log.Logger) (*EasyStoreObjectFields, error) {

	results := EasyStoreObjectFields{}
	//results.fields = make(map[string]string)
	count := 0

	for rows.Next() {
		var name, value string
		err := rows.Scan(&name, &value)
		if err != nil {
			return nil, err
		}

		results[name] = value
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "fields(s) not found", ErrNotFound)
	}

	logDebug(log, fmt.Sprintf("found %d fields(s)", count))
	return &results, nil
}

func blobQueryResults(rows *sql.Rows, log *log.Logger) ([]EasyStoreBlob, error) {
	results := make([]EasyStoreBlob, 0)
	count := 0

	for rows.Next() {
		blob := easyStoreBlobImpl{}
		err := rows.Scan(&blob.name, &blob.mimeType, &blob.payload, &blob.created, &blob.modified)
		if err != nil {
			return nil, err
		}

		results = append(results, blob)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "blobs(s) not found", ErrNotFound)
	}

	logDebug(log, fmt.Sprintf("found %d blobs(s)", count))
	return results, nil
}

func keyQueryResults(rows *sql.Rows, log *log.Logger) ([]DataStoreKey, error) {
	results := make([]DataStoreKey, 0)
	count := 0

	for rows.Next() {
		var namespace string
		var oid string
		var ct int
		err := rows.Scan(&namespace, &oid, &ct)
		if err != nil {
			return nil, err
		}

		results = append(results, DataStoreKey{namespace, oid})
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "key(s) not found", ErrNotFound)
	}

	logDebug(log, fmt.Sprintf("found %d key(s)", count))
	return results, nil
}

// handles unwrapping certain classes of errors
func errorMapper(err error) error {
	if err != nil {
		// try postgres errors
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint") == true {
			return fmt.Errorf("%q: %w", err.Error(), ErrAlreadyExists)
		}

		// try sqlite errors
		var sqliteErr sqlite3.Error
		if errors.As(err, &sqliteErr) {
			if errors.Is(sqliteErr.Code, sqlite3.ErrConstraint) {
				return fmt.Errorf("%q: %w", sqliteErr.Error(), ErrAlreadyExists)
			}
		}

	}
	return err
}

//
// end of file
//
