//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

//import (
//	_ "github.com/mattn/go-sqlite3"
//)

type DataStoreKey struct {
	Namespace string
	ObjectId  string
}

// do we use the contents from the cache or not
const (
	FROMCACHE = true
	NOCACHE   = false
)

// our dbStorage interface
type DataStore interface {
	Check() error

	// update methods
	UpdateBlob(key DataStoreKey, blob EasyStoreBlob) error
	UpdateFields(key DataStoreKey, fields EasyStoreObjectFields) error
	UpdateMetadata(key DataStoreKey, md EasyStoreMetadata) error
	UpdateObject(key DataStoreKey) error

	// add methods
	AddBlob(key DataStoreKey, blob EasyStoreBlob) error
	AddFields(key DataStoreKey, fields EasyStoreObjectFields) error
	AddMetadata(key DataStoreKey, md EasyStoreMetadata) error
	AddObject(obj EasyStoreObject) error

	// get multiples methods
	GetBlobsByKey(key DataStoreKey, useCache bool) ([]EasyStoreBlob, error)
	GetFieldsByKey(key DataStoreKey, useCache bool) (*EasyStoreObjectFields, error)
	GetMetadataByKey(key DataStoreKey, useCache bool) (EasyStoreMetadata, error)
	GetObjectsByKey(keys []DataStoreKey, useCache bool) ([]EasyStoreObject, error)

	// get single methods
	//GetBlobByKey(key DataStoreKey, curName string, useCache bool) ([]EasyStoreBlob, error)
	GetObjectByKey(key DataStoreKey, useCache bool) (EasyStoreObject, error)

	// rename method
	RenameBlobByKey(key DataStoreKey, curName string, newName string) error

	// delete multiple methods
	DeleteBlobsByKey(key DataStoreKey) error

	// delete single methods
	DeleteBlobByKey(key DataStoreKey, curName string) error
	DeleteFieldsByKey(key DataStoreKey) error
	DeleteMetadataByKey(key DataStoreKey) error
	DeleteObjectByKey(key DataStoreKey) error

	// search method
	GetKeysByFields(namespace string, fields EasyStoreObjectFields) ([]DataStoreKey, error)

	// close connections
	Close() error
}

// our factory
func NewDatastore(config EasyStoreImplConfig) (DataStore, error) {

	// check for a sqlite configuration
	//_, ok := config.(DatastoreSqliteConfig)
	//if ok == true {
	//	return newSqliteStore(config)
	//}

	// check for postgres configuration
	_, ok := config.(DatastorePostgresConfig)
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
