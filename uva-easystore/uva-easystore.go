//
//
//

package uva_easystore

import (
	"fmt"
	"log"
	"time"
)

// errors
var ErrNotImplemented = fmt.Errorf("not implemented")
var ErrBadParameter = fmt.Errorf("bad parameter")
var ErrNotFound = fmt.Errorf("the object does not exist")
var ErrStaleObject = fmt.Errorf("the object is stale")
var ErrDuplicateId = fmt.Errorf("the object already exists")

// components that can appear in an object
type EasyStoreComponents uint

// an object can have no metadata, no stored json and no files
const (
	NoComponents EasyStoreComponents = 0x00  // no additional components
	Metadata                         = 0x01  // metadata component
	StoredJson                       = 0x10  // opaque json component
	FileDetails                      = 0x100 // file details

	AllComponents = 0x111 // all components
)

// object metadata, zero or mkore name/value pairs
type EasyStoreObjectMetadata struct {
	metadata map[string]string // name value pairs used in the metadata
}

// common fields for objects and blobs
type EasyStoreCommon interface {
	Created() time.Time  // created time
	Modified() time.Time // last modified time
}

// an iterator for enumerating a set of objects
type EasyStoreObjectSet interface {
	Count()                         // the number of items in the set
	Next() (EasyStoreObject, error) // the next object in the set
}

// the store abstraction (read only)
type EasyStoreReadonly interface {

	// get object(s) by identifier
	GetById(string, EasyStoreComponents) (EasyStoreObject, error)
	GetByIds([]string, EasyStoreComponents) (EasyStoreObjectSet, error)

	// get object(s) by metadata
	GetByMetadata(EasyStoreObjectMetadata, EasyStoreComponents) (EasyStoreObjectSet, error)
}

type EasyStore interface {

	// the read only features
	EasyStoreReadonly

	// create new object
	Create(EasyStoreObject) (EasyStoreObject, error)

	// updating all or part of existing object
	Update(EasyStoreObject, EasyStoreComponents) (EasyStoreObject, error)

	// delete all or part of an existing object
	Delete(EasyStoreObject, EasyStoreComponents) (EasyStoreObject, error)
}

type EasyStoreObject interface {
	Id() string                        // object Id
	VersionHandle() string             // object version handle
	Metadata() EasyStoreObjectMetadata // the non-opaque metadata
	StoredJson() EasyStoreBlob         // the opaque metadata/json

	Blobs() []EasyStoreBlob // the associated file(s)

	EasyStoreCommon // any common fields
}

type EasyStoreBlob interface {
	Id() string         // blob Id
	SourceName() string // original name
	MimeType() string   // can we type this in some way

	// access to actual payload, etc

	EasyStoreCommon // any common fields
}

// EasyStoreConfig our configuration structure
type EasyStoreConfig struct {
	Namespace string      // easystore namespace
	log       *log.Logger // logging support
}

// NewEasyStore factory for our EasyStore interface
func NewEasyStore(config EasyStoreConfig) (EasyStore, error) {

	// mock the implementation here if necessary
	es, err := newEasyStore(config)
	return es, err
}

// NewEasyStoreReadonly factory for our EasyStoreReadonly interface
func NewEasyStoreReadonly(config EasyStoreConfig) (EasyStoreReadonly, error) {

	// mock the implementation here if necessary
	esro, err := newEasyStoreReadonly(config)
	return esro, err
}

// NewEasyStoreObject factory for our easystore object (really a helper)
func NewEasyStoreObject(id string) EasyStoreObject {
	return newEasyStoreObject(id)
}

//
// end of file
//
