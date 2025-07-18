//
// An easystore is a simple object dbStorage abstraction offering rudimentary find
// capabilities in addition to CRUD operations.
//
// Easystore objects all take a standard form consisting of a namespace/identifier pair,
// zero or more name/value pairs (referred to as fields) which are used in the find
// operations, an optional (opaque) block of content (used by the caller, not easystore)
// and zero or more binary objects (referred to as files).
//

package uvaeasystore

import (
	"fmt"
	"log"
	"time"
)

// all errors returned by the easystore. Not all errors are wrapped so some
// native ones can also be returned
var ErrNotImplemented = fmt.Errorf("not implemented")
var ErrBadParameter = fmt.Errorf("bad parameter")
var ErrFileNotFound = fmt.Errorf("the file does not exist")
var ErrNotFound = fmt.Errorf("the object does not exist")
var ErrStaleObject = fmt.Errorf("the object is stale")
var ErrAlreadyExists = fmt.Errorf("the object already exists")
var ErrSerialize = fmt.Errorf("serialization error")
var ErrDeserialize = fmt.Errorf("deserialization error")
var ErrBusNotConfigured = fmt.Errorf("bus not configured")
var ErrRecurse = fmt.Errorf("cannot recurse further")

// EasyStoreComponents - the components that can appear in an object
type EasyStoreComponents uint

// Note that an object can have no fields, no metadata and no files
const (
	BaseComponent EasyStoreComponents = 0x00  // no additional components
	Fields                            = 0x01  // fields component
	Files                             = 0x10  // file details
	Metadata                          = 0x100 // opaque metadata component

	AllComponents = 0x111 // all components
)

// EasyStoreObjectFields - zero or more name/value pairs
type EasyStoreObjectFields map[string]string // name value pairs

// EasyStoreCommon - common fields that appear in objects and blobs
type EasyStoreCommon interface {
	Created() time.Time  // Created_ time
	Modified() time.Time // last Modified_ time
}

// EasyStoreObjectSet - an iterator for enumerating a set of objects
type EasyStoreObjectSet interface {
	Count() uint                    // the number of items in the set
	Next() (EasyStoreObject, error) // the next object in the set
}

// EasyStoreBlobSet - an iterator for enumerating a set of blobs
type EasyStoreBlobSet interface {
	Count() uint                  // the number of items in the set
	Next() (EasyStoreBlob, error) // the next blob in the set
}

// EasyStoreReadonly - the store abstraction (read only)
type EasyStoreReadonly interface {

	// object API calls

	// get object(s) by identifier
	ObjectGetByKey(string, string, EasyStoreComponents) (EasyStoreObject, error)
	ObjectGetByKeys(string, []string, EasyStoreComponents) (EasyStoreObjectSet, error)

	// get object(s) by fields, all specified are combined in an AND operation
	ObjectGetByFields(string, EasyStoreObjectFields, EasyStoreComponents) (EasyStoreObjectSet, error)

	// file API calls

	// get file by identifier
	FileGetByKey(namespace string, oid string, name string) (EasyStoreBlob, error)

	// lifecycle API

	// close connections
	Close() error

	// check health/current status
	Check() error
}

// EasyStore - the store abstraction (read/write)
type EasyStore interface {

	// all the read only features
	EasyStoreReadonly

	// object API calls

	// create new object
	ObjectCreate(EasyStoreObject) (EasyStoreObject, error)

	// update all or part of existing object, specify which components are to be updated
	ObjectUpdate(EasyStoreObject, EasyStoreComponents) (EasyStoreObject, error)

	// delete all or part of an existing object, specify which components are to be deleted
	ObjectDelete(EasyStoreObject, EasyStoreComponents) (EasyStoreObject, error)

	// rename one of the blobs within the object, old name, new name
	//Rename(EasyStoreObject, EasyStoreComponents, string, string) (EasyStoreObject, error)

	// file API calls

	// create a file
	FileCreate(namespace string, oid string, file EasyStoreBlob) error

	// delete a file
	FileDelete(namespace string, oid string, name string) error

	// rename a file, old name, new name
	FileRename(namespace string, oid string, name string, new string) error

	// update a file
	FileUpdate(namespace string, oid string, file EasyStoreBlob) error
}

// EasyStoreObject - the objects stored in the easystore
type EasyStoreObject interface {
	Namespace() string // the object Namespace
	Id() string        // object Id
	VTag() string      // object version tag

	Fields() EasyStoreObjectFields // the fields
	Metadata() EasyStoreMetadata   // the opaque metadata
	Files() []EasyStoreBlob        // the associated file(s)

	SetNamespace(string)             // allows us to relocate into a different namespace
	SetFields(EasyStoreObjectFields) // the fields
	SetMetadata(EasyStoreMetadata)   // the opaque metadata
	SetFiles([]EasyStoreBlob)        // the associated file(s)

	EasyStoreCommon // any common fields
}

// EasyStoreBlob - represents a binary (opaque) object
type EasyStoreBlob interface {
	Name() string     // original name
	MimeType() string // can we type this in some way

	// access to actual payload, one of the following
	Url() string              // a url to stream the payload
	Payload() ([]byte, error) // the payload

	EasyStoreCommon // any common fields
}

// EasyStoreMetadata - represents a binary (opaque) object
type EasyStoreMetadata interface {
	MimeType() string         // can we type this in some way
	Payload() ([]byte, error) // the payload (might error due to serialization)

	EasyStoreCommon // any common fields
}

// EasyStoreImplConfig - the configuration structure for an implementation
type EasyStoreImplConfig interface {
	// logging support
	Logger() *log.Logger
	SetLogger(*log.Logger) // logging support

	// message bus configuration
	MessageBus() string    // name of the message bus to push telemetry to
	SetMessageBus(string)  // name of the message bus to push telemetry to
	EventSource() string   // telemetry events are tagged as coming from this source
	SetEventSource(string) // telemetry events are tagged as coming from this source
}

// EasyStoreProxyConfig - the configuration structure for a proxy
type EasyStoreProxyConfig interface {
	// logging support
	Logger() *log.Logger
	SetLogger(*log.Logger) // logging support

	// service endpoint
	Endpoint() string
	SetEndpoint(string)
	Timeout() int
	SetTimeout(int)
}

// EasyStoreSerializer - used to serialize and deserialize our objects
type EasyStoreSerializer interface {
	BlobDeserialize(interface{}) (EasyStoreBlob, error)
	BlobSerialize(EasyStoreBlob) interface{}
	FieldsDeserialize(interface{}) (EasyStoreObjectFields, error)
	FieldsSerialize(EasyStoreObjectFields) interface{}
	MetadataDeserialize(interface{}) (EasyStoreMetadata, error)
	MetadataSerialize(EasyStoreMetadata) interface{}
	ObjectDeserialize(interface{}) (EasyStoreObject, error)
	ObjectSerialize(EasyStoreObject) interface{}
}

//
// factory/helper methods
//

// NewEasyStoreProxy - factory for our EasyStoreProxy implementation
func NewEasyStoreProxy(config EasyStoreProxyConfig) (EasyStore, error) {
	return newEasyStoreProxy(config)
}

// NewEasyStoreProxyReadonly - factory for our EasyStoreProxyReadonly implementation
func NewEasyStoreProxyReadonly(config EasyStoreProxyConfig) (EasyStoreReadonly, error) {
	return newEasyStoreProxyReadonly(config)
}

// NewEasyStoreObject - factory for our easystore object
func NewEasyStoreObject(namespace string, id string) EasyStoreObject {
	return newEasyStoreObject(namespace, id)
}

func ProxyEasyStoreObject(namespace string, id string, vtag string) EasyStoreObject {
	return proxyEasyStoreObject(namespace, id, vtag)
}

// NewEasyStoreBlob - factory for our easystore blob object
func NewEasyStoreBlob(name string, mimeType string, payload []byte) EasyStoreBlob {
	return newEasyStoreBlob(name, mimeType, payload)
}

// NewEasyStoreMetadata - factory for our easystore blob object
func NewEasyStoreMetadata(mimeType string, payload []byte) EasyStoreMetadata {
	return newEasyStoreMetadata(mimeType, payload)
}

// DefaultEasyStoreFields - factory for the default easystore fields object
func DefaultEasyStoreFields() EasyStoreObjectFields {
	f := EasyStoreObjectFields{}
	return f
}

// DefaultEasyStoreSerializer - factory for the default easystore serializer
func DefaultEasyStoreSerializer() EasyStoreSerializer {
	return newEasyStoreSerializer()
}

//
// end of file
//
