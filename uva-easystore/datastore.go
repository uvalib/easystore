//
//
//

package uva_easystore

import (
	// needed
	_ "github.com/mattn/go-sqlite3"
)

// our storage interface
type Storage interface {
	Check() error

	// add methods
	AddBlob(string, EasyStoreBlob) error
	AddFields(string, EasyStoreObjectFields) error
	AddMetadata(EasyStoreObject) error

	// get methods
	GetBlobsByOid(string) ([]EasyStoreBlob, error)
	GetFieldsByOid(string) (*EasyStoreObjectFields, error)
	GetMetadataByOid(string) (EasyStoreObject, error)

	// delete methods
	DeleteBlobsByOid(string) error
	DeleteFieldsByOid(string) error
	DeleteMetadataByOid(string) error
}

// our singleton store
var Store Storage

// our factory
func NewDatastore(namespace string) error {
	var err error
	// mock implementation here
	Store, err = newDBStore(namespace)
	return err
}

//
// end of file
//
