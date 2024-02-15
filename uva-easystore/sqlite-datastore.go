//
//
//

package uva_easystore

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

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

	stmt, err := s.Prepare("INSERT INTO blobs( oid, name, mimetype ) VALUES( ?,?,? )")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(oid, blob.Name(), blob.MimeType())
	return err
}

// AddFields -- add a new fields object
func (s *storage) AddFields(oid string, fields EasyStoreObjectFields) error {

	stmt, err := s.Prepare("INSERT INTO fields( oid, name, value ) VALUES( ?,?,? )")
	if err != nil {
		return err
	}

	for n, v := range fields.fields {
		_, err = stmt.Exec(oid, n, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddMetadata -- add a new metadata object
func (s *storage) AddMetadata(obj EasyStoreObject) error {

	stmt, err := s.Prepare("INSERT INTO metadata( oid, accessid ) VALUES( ?,? )")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(obj.Id(), obj.AccessId())
	return err
}

// GetBlobsByOid -- get all blob data associated with the specified object
func (s *storage) GetBlobsByOid(oid string) ([]EasyStoreBlob, error) {

	rows, err := s.Query("SELECT name, mimetype, created_at, updated_at FROM blobs WHERE oid = ?", oid)
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
func (s *storage) GetMetadataByOid(oid string) (EasyStoreObject, error) {

	rows, err := s.Query("SELECT oid, accessid, created_at, updated_at FROM metadata WHERE oid = ? LIMIT 1", oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return metadataResults(rows)
}

// DeleteBlobsByOid -- delete all blob data associated with the specified object
func (s *storage) DeleteBlobsByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM blobs WHERE oid = ?")
	if err != nil {
		return err
	}
	return deletePreparedById(stmt, oid)
}

// DeleteFieldsByOid -- delete all field data associated with the specified object
func (s *storage) DeleteFieldsByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM fields WHERE oid = ?")
	if err != nil {
		return err
	}
	return deletePreparedById(stmt, oid)
}

// DeleteMetadataByOid -- delete all field data associated with the specified object
func (s *storage) DeleteMetadataByOid(oid string) error {

	stmt, err := s.Prepare("DELETE FROM metadata WHERE oid = ?")
	if err != nil {
		return err
	}
	return deletePreparedById(stmt, oid)
}

// private implementation methods
func deletePreparedById(stmt *sql.Stmt, oid string) error {
	_, err := stmt.Exec(oid)
	return err
}

func metadataResults(rows *sql.Rows) (EasyStoreObject, error) {
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
	results.fields = make(map[string]string)
	count := 0

	for rows.Next() {
		var name, value string
		err := rows.Scan(&name, &value)
		if err != nil {
			return nil, err
		}

		results.fields[name] = value
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
		err := rows.Scan(&blob.name, &blob.mimeType, &blob.created, &blob.modified)
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

//
// end of file
//