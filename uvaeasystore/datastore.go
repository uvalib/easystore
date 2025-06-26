//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	_ "github.com/mattn/go-sqlite3"
)

type DataStoreKey struct {
	Namespace string
	ObjectId  string
}

// our dbStorage interface
type DataStore interface {
	Check() error

	// update methods
	UpdateObject(DataStoreKey) error

	// add methods
	AddBlob(DataStoreKey, EasyStoreBlob) error
	AddFields(DataStoreKey, EasyStoreObjectFields) error
	AddMetadata(DataStoreKey, EasyStoreMetadata) error
	AddObject(EasyStoreObject) error

	// get methods
	GetBlobsByKey(DataStoreKey) ([]EasyStoreBlob, error)
	GetFieldsByKey(DataStoreKey) (*EasyStoreObjectFields, error)
	GetMetadataByKey(DataStoreKey) (EasyStoreMetadata, error)
	GetObjectByKey(DataStoreKey) (EasyStoreObject, error)
	GetObjectsByKey([]DataStoreKey) ([]EasyStoreObject, error)

	// rename method
	RenameBlobByKey(DataStoreKey, string, string) error

	// delete methods
	DeleteBlobsByKey(DataStoreKey) error
	DeleteFieldsByKey(DataStoreKey) error
	DeleteMetadataByKey(DataStoreKey) error
	DeleteObjectByKey(DataStoreKey) error

	// search methods
	GetKeysByFields(string, EasyStoreObjectFields) ([]DataStoreKey, error)

	// close connections
	Close() error
}

// our factory
func NewDatastore(config EasyStoreImplConfig) (DataStore, error) {

	// check for a sqlite configuration
	_, ok := config.(DatastoreSqliteConfig)
	if ok == true {
		return newSqliteStore(config)
	}

	// check for postgres configuration
	_, ok = config.(DatastorePostgresConfig)
	if ok == true {
		return newPostgresStore(config)
	}

	// check for S3 configuration
	_, ok = config.(DatastoreS3Config)
	if ok == true {
		return newS3Store(config)
	}

	return nil, ErrNotImplemented
}

//
// end of file
//
