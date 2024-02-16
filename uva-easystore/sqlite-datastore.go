//
//
//

package uva_easystore

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/exp/maps"
	"os"
)

var blobMetadataName = "metadata.secret.hidden"

// this is our DB implementation
type storage struct {
	*sql.DB
}

// newSqliteStore -- create a sqlite version of the DataStore
func newSqliteStore(namespace string) (DataStore, error) {

	// temp location for now
	dataSourceName := fmt.Sprintf("/tmp/%s.db", namespace)

	// make sure it exists so we do not create an empty schema
	_, err := os.Stat(dataSourceName)
	if err != nil {
		return nil, ErrNamespaceNotFound
	}

	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return &storage{db}, nil
}

// Check -- check our database health
func (s *storage) Check() error {
	return s.Ping()
}

// AddBlob -- add a new blob object
func (s *storage) AddBlob(oid string, blob EasyStoreBlob) error {

	stmt, err := s.Prepare("INSERT INTO blobs( oid, name, mimetype, payload ) VALUES( ?,?,?,? )")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(oid, blob.Name(), blob.MimeType(), "dummy payload")
	return err
}

// AddFields -- add a new fields object
func (s *storage) AddFields(oid string, fields EasyStoreObjectFields) error {

	stmt, err := s.Prepare("INSERT INTO fields( oid, name, value ) VALUES( ?,?,? )")
	if err != nil {
		return err
	}

	for n, v := range fields {
		_, err = stmt.Exec(oid, n, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddMetadata -- add a new metadata object
func (s *storage) AddMetadata(oid string, obj EasyStoreMetadata) error {

	stmt, err := s.Prepare("INSERT INTO blobs( oid, name, mimetype, payload ) VALUES( ?,?,?,? )")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(oid, blobMetadataName, obj.MimeType(), obj.Payload())
	return err
}

// AddObject -- add a new metadata object
func (s *storage) AddObject(obj EasyStoreObject) error {

	stmt, err := s.Prepare("INSERT INTO metadata( oid, accessid ) VALUES( ?,? )")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(obj.Id(), obj.AccessId())
	return err
}

// GetBlobsByOid -- get all blob data associated with the specified object
func (s *storage) GetBlobsByOid(oid string) ([]EasyStoreBlob, error) {

	rows, err := s.Query("SELECT name, mimetype, payload, created_at, updated_at FROM blobs WHERE oid = ? and name != ?", oid, blobMetadataName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return blobResults(rows)
}

// GetFieldsByOid -- get all field data associated with the specified object
func (s *storage) GetFieldsByOid(oid string) (*EasyStoreObjectFields, error) {

	rows, err := s.Query("SELECT name, value FROM fields WHERE oid = ?", oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return fieldResults(rows)
}

// GetMetadataByOid -- get all field data associated with the specified object
func (s *storage) GetMetadataByOid(oid string) (EasyStoreMetadata, error) {

	rows, err := s.Query("SELECT name, mimetype, payload, created_at, updated_at FROM blobs WHERE oid = ? and name = ? LIMIT 1", oid, blobMetadataName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	br, err := blobResults(rows)
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

	rows, err := s.Query("SELECT oid, accessid, created_at, updated_at FROM metadata WHERE oid = ? LIMIT 1", oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return objectResults(rows)
}

// DeleteBlobsByOid -- delete all blob data associated with the specified object
func (s *storage) DeleteBlobsByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM blobs WHERE oid = ?")
	if err != nil {
		return err
	}
	return execPreparedBy1(stmt, oid)
}

// DeleteFieldsByOid -- delete all field data associated with the specified object
func (s *storage) DeleteFieldsByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM fields WHERE oid = ?")
	if err != nil {
		return err
	}
	return execPreparedBy1(stmt, oid)
}

// DeleteMetadataByOid -- delete all field data associated with the specified object
func (s *storage) DeleteMetadataByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM blob WHERE oid = ? AND name = ?")
	if err != nil {
		return err
	}
	return execPreparedBy1(stmt, oid)
}

// DeleteObjectByOid -- delete all field data associated with the specified object
func (s *storage) DeleteObjectByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM metadata WHERE oid = ?")
	if err != nil {
		return err
	}
	return execPreparedBy1(stmt, oid)
}

// GetIdsByFields -- get a list of ids that have the supplied fields/values
func (s *storage) GetIdsByFields(fields EasyStoreObjectFields) ([]string, error) {

	query := "select distinct(oid) from fields"
	var err error
	var rows *sql.Rows
	// just support 2 cases, no fields which means all objects or 1 set of fields
	if len(fields) == 0 {
		rows, err = s.Query(query)
	} else {
		query = fmt.Sprintf("%s where name = ? and value = ?", query)
		key := maps.Keys(fields)[0]
		value := fields[key]
		rows, err = s.Query(query, key, value)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return idResults(rows)
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

func objectResults(rows *sql.Rows) (EasyStoreObject, error) {
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
		return nil, ErrNoResults
	}

	return &results, nil
}

func fieldResults(rows *sql.Rows) (*EasyStoreObjectFields, error) {

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
		return nil, ErrNoResults
	}

	return &results, nil
}

func blobResults(rows *sql.Rows) ([]EasyStoreBlob, error) {
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
		return nil, ErrNoResults
	}

	return results, nil
}

func idResults(rows *sql.Rows) ([]string, error) {
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
		return nil, ErrNoResults
	}

	return results, nil
}

//
// end of file
//
