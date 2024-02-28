//
//
//

package uvaeasystore

import (
	_ "github.com/mattn/go-sqlite3"
)

// our storage interface
type DataStore interface {
	Check() error

	// add methods
	AddBlob(string, EasyStoreBlob) error
	AddFields(string, EasyStoreObjectFields) error
	AddMetadata(string, EasyStoreMetadata) error
	AddObject(EasyStoreObject) error

	// get methods
	GetBlobsByKey(string, string) ([]EasyStoreBlob, error)
	GetFieldsByKey(string, string) (*EasyStoreObjectFields, error)
	GetMetadataByKey(string, string) (EasyStoreMetadata, error)
	GetObjectByKey(string, string) (EasyStoreObject, error)

	// delete methods
	DeleteBlobsByKey(string, string) error
	DeleteFieldsByKey(string, string) error
	DeleteMetadataByKey(string, string) error
	DeleteObjectByKey(string, string) error

	// search methods
	GetIdsByFields(string, EasyStoreObjectFields) ([]string, error)
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
