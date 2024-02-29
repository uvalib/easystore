//
// sqlite implementation of the datastore interface
//

package uvaeasystore

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"strings"

	"golang.org/x/exp/maps"
	"log"
)

// we store opaque metadata as a blob so need to distinguish it as special
var blobMetadataName = "metadata.secret.hidden"

// this is our DB implementation
type storage struct {
	log *log.Logger
	*sql.DB
}

// Check -- check our database health
func (s *storage) Check() error {
	return s.Ping()
}

// AddBlob -- add a new blob object
func (s *storage) AddBlob(key DataStoreKey, blob EasyStoreBlob) error {

	stmt, err := s.Prepare("INSERT INTO blobs( namespace, oid, name, mimetype, payload ) VALUES( $1,$2,$3,$4,$5 )")
	if err != nil {
		return err
	}

	// errors here are serialization errors
	buf, err := blob.Payload()
	if err != nil {
		return err
	}

	_, err = stmt.Exec(key.namespace, key.objectId, blob.Name(), blob.MimeType(), buf)
	return errorMapper(err)
}

// AddFields -- add a new fields object
func (s *storage) AddFields(key DataStoreKey, fields EasyStoreObjectFields) error {

	stmt, err := s.Prepare("INSERT INTO fields( namespace, oid, name, value ) VALUES( $1,$2,$3,$4 )")
	if err != nil {
		return err
	}

	for n, v := range fields {
		_, err = stmt.Exec(key.namespace, key.objectId, n, v)
		if err != nil {
			return errorMapper(err)
		}
	}
	return nil
}

// AddMetadata -- add a new metadata object
func (s *storage) AddMetadata(key DataStoreKey, obj EasyStoreMetadata) error {

	stmt, err := s.Prepare("INSERT INTO blobs( namespace, oid, name, mimetype, payload ) VALUES( $1,$2,$3,$4,$5 )")
	if err != nil {
		return err
	}

	// errors here are serialization errors
	buf, err := obj.Payload()
	if err != nil {
		return err
	}

	_, err = stmt.Exec(key.namespace, key.objectId, blobMetadataName, obj.MimeType(), buf)
	return errorMapper(err)
}

// AddObject -- add a new object
func (s *storage) AddObject(obj EasyStoreObject) error {

	stmt, err := s.Prepare("INSERT INTO objects( namespace, oid, accessid ) VALUES( $1,$2,$3 )")
	if err != nil {
		return err
	}

	return execPreparedBy3(stmt, obj.Namespace(), obj.Id(), obj.AccessId())
}

// GetBlobsByKey -- get all blob data associated with the specified object
func (s *storage) GetBlobsByKey(key DataStoreKey) ([]EasyStoreBlob, error) {

	rows, err := s.Query("SELECT name, mimetype, payload, created_at, updated_at FROM blobs WHERE namespace = $1 AND oid = $2 and name != $3 ORDER BY updated_at", key.namespace, key.objectId, blobMetadataName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return blobResults(rows, s.log)
}

// GetFieldsByKey -- get all field data associated with the specified object
func (s *storage) GetFieldsByKey(key DataStoreKey) (*EasyStoreObjectFields, error) {

	rows, err := s.Query("SELECT name, value FROM fields WHERE namespace = $1 AND oid = $2 ORDER BY updated_at", key.namespace, key.objectId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return fieldResults(rows, s.log)
}

// GetMetadataByKey -- get all field data associated with the specified object
func (s *storage) GetMetadataByKey(key DataStoreKey) (EasyStoreMetadata, error) {

	rows, err := s.Query("SELECT name, mimetype, payload, created_at, updated_at FROM blobs WHERE namespace = $1 AND oid = $2 and name = $3 LIMIT 1", key.namespace, key.objectId, blobMetadataName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	br, err := blobResults(rows, s.log)
	if err != nil {
		return nil, err
	}

	b, _ := br[0].(easyStoreBlobImpl)
	md := easyStoreMetadataImpl{
		mimeType: b.mimeType,
		payload:  b.payload,
		created:  b.created,
		modified: b.modified}

	return md, nil
}

// GetObjectByKey -- get all field data associated with the specified object
func (s *storage) GetObjectByKey(key DataStoreKey) (EasyStoreObject, error) {

	rows, err := s.Query("SELECT namespace, oid, accessid, created_at, updated_at FROM objects WHERE namespace = $1 AND oid = $2 LIMIT 1", key.namespace, key.objectId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return objectResults(rows, s.log)
}

// DeleteBlobsByKey -- delete all blob data associated with the specified object
func (s *storage) DeleteBlobsByKey(key DataStoreKey) error {

	stmt, err := s.Prepare("DELETE FROM blobs WHERE namespace = $1 AND oid = $2 and name != $3")
	if err != nil {
		return err
	}
	return execPreparedBy3(stmt, key.namespace, key.objectId, blobMetadataName)
}

// DeleteFieldsByKey -- delete all field data associated with the specified object
func (s *storage) DeleteFieldsByKey(key DataStoreKey) error {

	stmt, err := s.Prepare("DELETE FROM fields WHERE namespace = $1 AND oid = $2")
	if err != nil {
		return err
	}
	return execPreparedBy2(stmt, key.namespace, key.objectId)
}

// DeleteMetadataByKey -- delete all field data associated with the specified object
func (s *storage) DeleteMetadataByKey(key DataStoreKey) error {

	stmt, err := s.Prepare("DELETE FROM blobs WHERE namespace = $1 AND oid = $2 AND name = $3")
	if err != nil {
		return err
	}
	return execPreparedBy3(stmt, key.namespace, key.objectId, blobMetadataName)
}

// DeleteObjectByKey -- delete all field data associated with the specified object
func (s *storage) DeleteObjectByKey(key DataStoreKey) error {

	stmt, err := s.Prepare("DELETE FROM objects WHERE namespace = $1 AND oid = $2")
	if err != nil {
		return err
	}
	return execPreparedBy2(stmt, key.namespace, key.objectId)
}

// GetKeysByFields -- get a list of keys that have the supplied fields/values
func (s *storage) GetKeysByFields(namespace string, fields EasyStoreObjectFields) ([]DataStoreKey, error) {

	// currently do not support more than 1 field match
	if len(fields) > 1 {
		return nil, fmt.Errorf("%q: %w", "no support for multiple field matches", ErrNotImplemented)
	}

	var err error
	var rows *sql.Rows
	var query string
	//
	// support the following cases:
	// empty namespace (all namespaces) or specified namespace
	// no fields (all objects) or 1 set of fields
	//
	if len(fields) == 0 {
		if len(namespace) == 0 {
			query = "SELECT namespace, oid FROM objects ORDER BY namespace, oid"
			rows, err = s.Query(query)
		} else {
			query = "SELECT namespace, oid FROM objects where namespace = $1 ORDER BY namespace, oid"
			rows, err = s.Query(query, namespace)
		}
	} else {
		key := maps.Keys(fields)[0]
		value := fields[key]
		if len(namespace) == 0 {
			query = "SELECT namespace, oid FROM fields WHERE name = $1 AND value = $2 GROUP BY namespace, oid ORDER BY namespace, oid"
			rows, err = s.Query(query, key, value)
		} else {
			query = "SELECT namespace, oid FROM fields WHERE namespace = $1 AND name = $2 AND value = $3 GROUP BY namespace, oid ORDER BY namespace, oid"
			rows, err = s.Query(query, namespace, key, value)
		}
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return keyResults(rows, s.log)
}

//
// private implementation methods
//

func execPreparedBy2(stmt *sql.Stmt, value1 string, value2 string) error {
	_, err := stmt.Exec(value1, value2)
	return errorMapper(err)
}

func execPreparedBy3(stmt *sql.Stmt, value1 string, value2 string, value3 string) error {
	_, err := stmt.Exec(value1, value2, value3)
	return errorMapper(err)
}

func objectResults(rows *sql.Rows, log *log.Logger) (EasyStoreObject, error) {
	results := easyStoreObjectImpl{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.namespace, &results.id, &results.accessId, &results.created, &results.modified)
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

func fieldResults(rows *sql.Rows, log *log.Logger) (*EasyStoreObjectFields, error) {

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

func blobResults(rows *sql.Rows, log *log.Logger) ([]EasyStoreBlob, error) {
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

func keyResults(rows *sql.Rows, log *log.Logger) ([]DataStoreKey, error) {
	results := make([]DataStoreKey, 0)
	count := 0

	for rows.Next() {
		var namespace string
		var oid string
		err := rows.Scan(&namespace, &oid)
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
		return nil, fmt.Errorf("%q: %w", "id(s) not found", ErrNotFound)
	}

	logDebug(log, fmt.Sprintf("found %d id(s)", count))
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
