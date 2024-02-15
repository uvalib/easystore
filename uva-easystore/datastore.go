//
//
//

package uva_easystore

import (
	// needed
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

var ErrNoResults = fmt.Errorf("no results")

// our storage interface
type DataStore interface {
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

// our factory
func NewDatastore(namespace string) (DataStore, error) {
	// mock implementation here if necessary
	return newSqliteStore(namespace)
}

//
// end of file
//
