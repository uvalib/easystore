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
	GetBlobsByOid(string) ([]EasyStoreBlob, error)
	GetFieldsByOid(string) (*EasyStoreObjectFields, error)
	GetMetadataByOid(string) (EasyStoreMetadata, error)
	GetObjectByOid(string) (EasyStoreObject, error)

	// delete methods
	DeleteBlobsByOid(string) error
	DeleteFieldsByOid(string) error
	DeleteMetadataByOid(string) error
	DeleteObjectByOid(string) error

	// search methods
	GetIdsByFields(EasyStoreObjectFields) ([]string, error)
}

// our factory
func NewDatastore(config EasyStoreConfig) (DataStore, error) {

	// mock implementation here if necessary

	// check for a sqlite configuration
	_, ok := config.(datastoreSqliteConfig)
	if ok == true {
		return newSqliteStore(config)
	}

	return nil, ErrNotImplemented
}

//
// end of file
//
