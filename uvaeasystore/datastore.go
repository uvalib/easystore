//
//
//

package uvaeasystore

import (
	_ "github.com/mattn/go-sqlite3"
)

type DataStoreKey struct {
	namespace string
	objectId  string
}

// our storage interface
type DataStore interface {
	Check() error

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

	// delete methods
	DeleteBlobsByKey(DataStoreKey) error
	DeleteFieldsByKey(DataStoreKey) error
	DeleteMetadataByKey(DataStoreKey) error
	DeleteObjectByKey(DataStoreKey) error

	// search methods
	GetKeysByFields(string, EasyStoreObjectFields) ([]DataStoreKey, error)
}

// our factory
func NewDatastore(config EasyStoreConfig) (DataStore, error) {

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

	return nil, ErrNotImplemented
}

//
// end of file
//
