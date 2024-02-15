//
// An easystore is a simple object storage abstraction offering rudimentary find
// capabilities in addition to CRUD operations.
//
// Easystore objects all take a standard form consisting of a unique identifier,
// zero or more name/value pairs (referred to as fields) which are used in the find
// operations, an optional (opaque) block of textual fields (used by the caller, not the store)
// and zero or more binary objects (referred to as files).
//

package uva_easystore

import (
	"fmt"
	"log"
	"time"
)

// all errors returned by the easystore. Not all errors are wrapped so some
// native ones can also be returned
var ErrNotImplemented = fmt.Errorf("not implemented")
var ErrBadParameter = fmt.Errorf("bad parameter")
var ErrNotFound = fmt.Errorf("the object does not exist")
var ErrStaleObject = fmt.Errorf("the object is stale")
var ErrDuplicateId = fmt.Errorf("the object already exists")

// EasyStoreComponents - the components that can appear in an object
type EasyStoreComponents uint

// Note that an object can have no fields, no fields and no files
const (
	NoComponents EasyStoreComponents = 0x00  // no additional components
	Fields                           = 0x01  // fields component
	Metadata                         = 0x10  // opaque fields component
	Files                            = 0x100 // file details

	AllComponents = 0x111 // all components
)

// EasyStoreObjectFields - zero or more name/value pairs
type EasyStoreObjectFields struct {
	fields map[string]string // name value pairs
}

// EasyStoreCommon - common fields that appear in objects and blobs
type EasyStoreCommon interface {
	Created() time.Time  // created time
	Modified() time.Time // last modified time
}

// EasyStoreObjectSet - an iterator for enumerating a set of objects
type EasyStoreObjectSet interface {
	Count()                         // the number of items in the set
	Next() (EasyStoreObject, error) // the next object in the set
}

// EasyStoreReadonly - the store abstraction (read only)
type EasyStoreReadonly interface {

	// get object(s) by identifier
	GetById(string, EasyStoreComponents) (EasyStoreObject, error)
	GetByIds([]string, EasyStoreComponents) (EasyStoreObjectSet, error)

	// get object(s) by fields, all specified are combined in an AND operation
	GetByFields(EasyStoreObjectFields, EasyStoreComponents) (EasyStoreObjectSet, error)
}

// EasyStore - the store abstraction (read/write)
type EasyStore interface {

	// the read only features
	EasyStoreReadonly

	// create new object
	Create(EasyStoreObject) (EasyStoreObject, error)

	// update all or part of existing object, specify which components are to be updated
	Update(EasyStoreObject, EasyStoreComponents) (EasyStoreObject, error)

	// delete all or part of an existing object
	Delete(EasyStoreObject, EasyStoreComponents) (EasyStoreObject, error)
}

// EasyStoreObject - the objects stored in the easystore
type EasyStoreObject interface {
	Id() string                    // object Id
	AccessId() string              // object access handle
	Fields() EasyStoreObjectFields // the fields
	Metadata() EasyStoreBlob       // the opaque metadata

	Files() []EasyStoreBlob // the associated file(s)

	EasyStoreCommon // any common fields
}

// EasyStoreBlob - represents a binary (opaque) object
type EasyStoreBlob interface {
	Name() string     // original name
	MimeType() string // can we type this in some way

	// access to actual payload
	Url() string // not sure, one of these
	//Payload() []byte
	//io.Reader

	EasyStoreCommon // any common fields
}

// EasyStoreConfig - the configuration structure
type EasyStoreConfig interface {
	Namespace(string)   // easystore goodNamespace
	Logger(*log.Logger) // logging support

	EasyStoreImplConfig
}

// EasyStoreImplConfig - the implementation configuration structure
type EasyStoreImplConfig interface {
	// fill this in later, may end up hidden
}

// NewEasyStore - factory for our EasyStore interface
func NewEasyStore(config EasyStoreConfig) (EasyStore, error) {

	// mock the implementation here if necessary
	es, err := newEasyStore(config)
	return es, err
}

// NewEasyStoreReadonly - factory for our EasyStoreReadonly interface
func NewEasyStoreReadonly(config EasyStoreConfig) (EasyStoreReadonly, error) {

	// mock the implementation here if necessary
	esro, err := newEasyStoreReadonly(config)
	return esro, err
}

// NewEasyStoreObject - factory for our easystore object (really a helper)
func NewEasyStoreObject(id string) EasyStoreObject {
	return newEasyStoreObject(id)
}

// DefaultEasyStoreConfig - factory for the default easystore configuration object
func DefaultEasyStoreConfig() EasyStoreConfig {
	return newDefaultEasyStoreConfig()
}

//
// end of file
//
