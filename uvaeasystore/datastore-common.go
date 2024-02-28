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
func (s *storage) AddBlob(oid string, blob EasyStoreBlob) error {

	stmt, err := s.Prepare("INSERT INTO blobs( oid, name, mimetype, payload ) VALUES( $1,$2,$3,$4 )")
	if err != nil {
		return err
	}

	// always store the payload in its native format
	_, err = stmt.Exec(oid, blob.Name(), blob.MimeType(), blob.PayloadNative())
	return errorMapper(err)
}

// AddFields -- add a new fields object
func (s *storage) AddFields(oid string, fields EasyStoreObjectFields) error {

	stmt, err := s.Prepare("INSERT INTO fields( oid, name, value ) VALUES( $1,$2,$3 )")
	if err != nil {
		return err
	}

	for n, v := range fields {
		_, err = stmt.Exec(oid, n, v)
		if err != nil {
			return errorMapper(err)
		}
	}
	return nil
}

// AddMetadata -- add a new metadata object
func (s *storage) AddMetadata(oid string, obj EasyStoreMetadata) error {

	stmt, err := s.Prepare("INSERT INTO blobs( oid, name, mimetype, payload ) VALUES( $1,$2,$3,$4 )")
	if err != nil {
		return err
	}

	// always store the payload in its native format
	_, err = stmt.Exec(oid, blobMetadataName, obj.MimeType(), obj.PayloadNative())
	return errorMapper(err)
}

// AddObject -- add a new object
func (s *storage) AddObject(obj EasyStoreObject) error {

	stmt, err := s.Prepare("INSERT INTO objects( oid, accessid ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(obj.Id(), obj.AccessId())
	return errorMapper(err)
}

// GetBlobsByOid -- get all blob data associated with the specified object
func (s *storage) GetBlobsByOid(oid string) ([]EasyStoreBlob, error) {

	rows, err := s.Query("SELECT name, mimetype, payload, created_at, updated_at FROM blobs WHERE oid = $1 and name != $2 ORDER BY updated_at", oid, blobMetadataName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return blobResults(rows, s.log)
}

// GetFieldsByOid -- get all field data associated with the specified object
func (s *storage) GetFieldsByOid(oid string) (*EasyStoreObjectFields, error) {

	rows, err := s.Query("SELECT name, value FROM fields WHERE oid = $1 ORDER BY updated_at", oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return fieldResults(rows, s.log)
}

// GetMetadataByOid -- get all field data associated with the specified object
func (s *storage) GetMetadataByOid(oid string) (EasyStoreMetadata, error) {

	rows, err := s.Query("SELECT name, mimetype, payload, created_at, updated_at FROM blobs WHERE oid = $1 and name = $2 LIMIT 1", oid, blobMetadataName)
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

// GetObjectOid -- get all field data associated with the specified object
func (s *storage) GetObjectByOid(oid string) (EasyStoreObject, error) {

	rows, err := s.Query("SELECT oid, accessid, created_at, updated_at FROM objects WHERE oid = $1 LIMIT 1", oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return objectResults(rows, s.log)
}

// DeleteBlobsByOid -- delete all blob data associated with the specified object
func (s *storage) DeleteBlobsByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM blobs WHERE oid = $1 and name != $2")
	if err != nil {
		return err
	}
	return execPreparedBy2(stmt, oid, blobMetadataName)
}

// DeleteFieldsByOid -- delete all field data associated with the specified object
func (s *storage) DeleteFieldsByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM fields WHERE oid = $1")
	if err != nil {
		return err
	}
	return execPreparedBy1(stmt, oid)
}

// DeleteMetadataByOid -- delete all field data associated with the specified object
func (s *storage) DeleteMetadataByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM blobs WHERE oid = $1 AND name = $2")
	if err != nil {
		return err
	}
	return execPreparedBy2(stmt, oid, blobMetadataName)
}

// DeleteObjectByOid -- delete all field data associated with the specified object
func (s *storage) DeleteObjectByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM objects WHERE oid = $1")
	if err != nil {
		return err
	}
	return execPreparedBy1(stmt, oid)
}

// GetIdsByFields -- get a list of ids that have the supplied fields/values
func (s *storage) GetIdsByFields(fields EasyStoreObjectFields) ([]string, error) {

	var err error
	var rows *sql.Rows
	// just support 2 cases, no fields which means all objects or 1 set of fields
	if len(fields) == 0 {
		query := "SELECT distinct(oid) FROM objects ORDER BY updated_at"
		rows, err = s.Query(query)
	} else {
		query := "SELECT distinct(oid) FROM fields WHERE name = $1 AND value = $2 ORDER BY updated_at"
		key := maps.Keys(fields)[0]
		value := fields[key]
		rows, err = s.Query(query, key, value)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return idResults(rows, s.log)
}

//
// private implementation methods
//

func execPreparedBy1(stmt *sql.Stmt, value1 string) error {
	_, err := stmt.Exec(value1)
	return err
}

func execPreparedBy2(stmt *sql.Stmt, value1 string, value2 string) error {
	_, err := stmt.Exec(value1, value2)
	return err
}

func objectResults(rows *sql.Rows, log *log.Logger) (EasyStoreObject, error) {
	results := easyStoreObjectImpl{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.id, &results.accessId, &results.created, &results.modified)
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

func idResults(rows *sql.Rows, log *log.Logger) ([]string, error) {
	results := make([]string, 0)
	count := 0

	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			return nil, err
		}

		results = append(results, id)
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
