//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func execPrepared(stmt *sql.Stmt, values ...any) error {
	_, err := stmt.Exec(values...)
	return errorMapper(err)
}

func objectQueryResults(rows *sql.Rows, log *log.Logger) (EasyStoreObject, error) {
	results := easyStoreObjectImpl{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.Namespace_, &results.Id_, &results.Vtag_, &results.Created_, &results.Modified_)
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

func objectsQueryResults(rows *sql.Rows, log *log.Logger) ([]EasyStoreObject, error) {
	results := make([]EasyStoreObject, 0)
	count := 0

	for rows.Next() {
		o := easyStoreObjectImpl{}
		err := rows.Scan(&o.Namespace_, &o.Id_, &o.Vtag_, &o.Created_, &o.Modified_)
		if err != nil {
			return nil, err
		}
		count++
		results = append(results, &o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrNotFound)
	}

	logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return results, nil
}

func fieldQueryResults(rows *sql.Rows, log *log.Logger) (*EasyStoreObjectFields, error) {

	results := EasyStoreObjectFields{}
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
		err := rows.Scan(&blob.Name_, &blob.MimeType_, &blob.Payload_, &blob.Created_, &blob.Modified_)
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
		//var sqliteErr sqlite3.Error
		//if errors.As(err, &sqliteErr) {
		//	if errors.Is(sqliteErr.Code, sqlite3.ErrConstraint) {
		//		return fmt.Errorf("%q: %w", sqliteErr.Error(), ErrAlreadyExists)
		//	}
		//}

	}
	return err
}

//
// end of file
//
