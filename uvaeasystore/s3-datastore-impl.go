//
//
//

package uvaeasystore

import (
	"database/sql"
	"fmt"
	"golang.org/x/exp/maps"
	"log"
)

var s3ObjectFileName = "object.json"
var s3FieldsFileName = "fields.json"
var s3MetadataFileName = "metadata.json"

// this is our S3 implementation
type s3Storage struct {
	bucket  string      // bucket name
	log     *log.Logger // logger
	*sql.DB             // database connection
}

// Check -- check our database health
func (s *s3Storage) Check() error {
	return s.Ping()
}

// UpdateObject -- update a couple of object fields
func (s *s3Storage) UpdateObject(key DataStoreKey) error {
	return ErrNotImplemented
}

// AddBlob -- add a new blob object
func (s *s3Storage) AddBlob(key DataStoreKey, blob EasyStoreBlob) error {
	// check asset does not exist
	err := s.checkNotExists(key.namespace, key.objectId, blob.Name())
	if err != nil {
		return err
	}
	return s.addBlob(key.namespace, key.objectId, blob)
}

// AddFields -- add a new fields object
func (s *s3Storage) AddFields(key DataStoreKey, fields EasyStoreObjectFields) error {
	// check asset does not exist
	err := s.checkNotExists(key.namespace, key.objectId, s3FieldsFileName)
	if err != nil {
		return err
	}

	// TODO: update database here

	return s.addFields(key.namespace, key.objectId, fields)
}

// AddMetadata -- add a new metadata object
func (s *s3Storage) AddMetadata(key DataStoreKey, metadata EasyStoreMetadata) error {
	// check asset does not exist
	err := s.checkNotExists(key.namespace, key.objectId, s3MetadataFileName)
	if err != nil {
		return err
	}
	return s.addMetadata(key.namespace, key.objectId, metadata)
}

// AddObject -- add a new object
func (s *s3Storage) AddObject(obj EasyStoreObject) error {
	// check asset does not exist
	err := s.checkNotExists(obj.Namespace(), obj.Id(), s3ObjectFileName)
	if err != nil {
		return err
	}
	return ErrNotImplemented
}

// GetBlobsByKey -- get all blob data associated with the specified object
func (s *s3Storage) GetBlobsByKey(key DataStoreKey) ([]EasyStoreBlob, error) {
	return nil, ErrNotImplemented
}

// GetFieldsByKey -- get all field data associated with the specified object
func (s *s3Storage) GetFieldsByKey(key DataStoreKey) (*EasyStoreObjectFields, error) {
	return nil, ErrNotImplemented
}

// GetMetadataByKey -- get all field data associated with the specified object
func (s *s3Storage) GetMetadataByKey(key DataStoreKey) (EasyStoreMetadata, error) {
	return nil, ErrNotImplemented
}

// GetObjectByKey -- get all field data associated with the specified object
func (s *s3Storage) GetObjectByKey(key DataStoreKey) (EasyStoreObject, error) {
	return nil, ErrNotImplemented
}

// DeleteBlobsByKey -- delete all blob data associated with the specified object
func (s *s3Storage) DeleteBlobsByKey(key DataStoreKey) error {
	return ErrNotImplemented
}

// DeleteFieldsByKey -- delete all field data associated with the specified object
func (s *s3Storage) DeleteFieldsByKey(key DataStoreKey) error {
	// delete from the database
	return s.removeAsset(key.namespace, key.objectId, s3FieldsFileName)
}

// DeleteMetadataByKey -- delete all field data associated with the specified object
func (s *s3Storage) DeleteMetadataByKey(key DataStoreKey) error {
	return s.removeAsset(key.namespace, key.objectId, s3MetadataFileName)
}

// DeleteObjectByKey -- delete all field data associated with the specified object
func (s *s3Storage) DeleteObjectByKey(key DataStoreKey) error {
	return s.removeAsset(key.namespace, key.objectId, s3ObjectFileName)
}

// GetKeysByFields -- get a list of keys that have the supplied fields/values
func (s *s3Storage) GetKeysByFields(namespace string, fields EasyStoreObjectFields) ([]DataStoreKey, error) {

	var err error
	var rows *sql.Rows
	var query string
	//
	// support the following cases:
	// empty namespace (all namespaces) or specified namespace
	// no fields (all objects) or variable set of fields
	//
	if len(fields) == 0 {
		if len(namespace) == 0 {
			query = "SELECT namespace, oid, 0 FROM objects ORDER BY namespace, oid"
			rows, err = s.Query(query)
		} else {
			query = "SELECT namespace, oid, 0 FROM objects where namespace = $1 ORDER BY namespace, oid"
			rows, err = s.Query(query, namespace)
		}
	} else {
		// dynamically build the query because we have a variable number of fields
		args := make([]any, 0)
		query = "SELECT namespace, oid, count(*) FROM fields WHERE "
		variableIx := 1
		if len(namespace) != 0 {
			query += fmt.Sprintf("namespace = $%d AND ", variableIx)
			args = append(args, namespace)
			variableIx++
		}

		for ix, k := range maps.Keys(fields) {
			query += fmt.Sprintf("(name = $%d AND value = $%d) ", variableIx, variableIx+1)
			variableIx += 2
			args = append(args, k, fields[k])
			if ix != (len(fields) - 1) {
				query += "OR "
			}
		}

		query += fmt.Sprintf("GROUP BY namespace, oid HAVING count(*) = %d", len(fields))
		rows, err = s.Query(query, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return keyQueryResults(rows, s.log)
}

//
// private implementation methods
//

func (s *s3Storage) checkNotExists(namespace string, identifier string, assetName string) error {
	return ErrNotImplemented
}

func (s *s3Storage) removeAsset(namespace string, identifier string, assetName string) error {
	return ErrNotImplemented
}

func (s *s3Storage) addBlob(namespace string, identifier string, blob EasyStoreBlob) error {
	return ErrNotImplemented
}

func (s *s3Storage) addFields(namespace string, identifier string, fields EasyStoreObjectFields) error {
	return ErrNotImplemented
}

func (s *s3Storage) addMetadata(namespace string, identifier string, metadata EasyStoreMetadata) error {
	return ErrNotImplemented
}

// assetName -- S3 assets are named as follows:
// s3://bucket-name/namespace/object-identifier/asset-name
func (s *s3Storage) assetKey(key DataStoreKey, asset string) string {
	return fmt.Sprintf("%s/%s/%s", key.namespace, key.objectId, asset)
}

//
// end of file
//
