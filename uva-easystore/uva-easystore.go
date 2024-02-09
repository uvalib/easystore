package uva_easystore

import (
	"fmt"
	"time"
)

// errors

var ErrBadParameter = fmt.Errorf("bad parameter")
var ErrNotFound = fmt.Errorf("the specified object does not exist")
var ErrStaleObject = fmt.Errorf("the specified object is stale")
var ErrDuplicateId = fmt.Errorf("the specified object already exists")

// object components
type EasyStoreComponents uint
const {
   Placeholder EasyStoreComponents = 0x00  // placeholder
   Metadata                        = 0x01  // metadata component
   StoredJson                      = 0x10  // opaque json component
   FileDetails                     = 0x100 // file details

   All                             = 0x111 // all components
}

type EasyStoreNVPairs {
   NVPairs map[string] string   // name value pairs used in the metadata
}

type EasyStoreSet {
   Count()                           // the number of items in the set
   Next() ( EasyStoreObject, error ) // the next object in the set
}

type EasyStoreCommonMetadata interface {
   Created() time.Time              // created time
   LastModified() time.Time         // last modified time
}

type EasyStore interface {

   // get object(s) by identifier
   GetById( string, EasyStoreComponents ) ( EasyStoreObject, error )
   GetByIds( string[], EasyStoreComponents ) ( EasyStoreSet, error )

   // get object(s) by metadata
   GetByMetadata( EasyStoreNVPairs, EasyStoreComponent ) ( EasyStoreSet, error )

   // create new object
   Create( EasyStoreObject ) ( EasyStoreObject, error )

   // updating existing object
   Update( EasyStoreObject, EasyStoreComponent ) ( EasyStoreObject, error )

   // delete all or part of an existing object
   Delete( EasyStoreObject, EasyStoreComponent ) ( EasyStoreObject, error )
}

// an object contains

type EasyStoreObject interface {
   Id() string                        // object Id
   VersionHandle() string             // object version handle
   Metadata() UvaEasyStoreNVPairs     // the non-opaque metadata
   StoredJson() UvaEasyStoreBlob      // the opaque metadata/json

   Blobs UvaEasyStoreBlob[]           // the associated file(s)

   // more stuff ?
}

type EasyStoreBlob interface {
   Id() string                 // blob Id
   SourceName() string         // original name
   MimeType() string           // can we type this in some way
   Created() time.Time         // created time
   LastModified() time.Time    // last modified time
        
   // actual payload, etc
}

// UvaEasyStoreConfig our configuration structure
type EasyStoreConfig struct {
        Namespace string  // easystore namespace
	log log.Logger    // logging support
}

// NewUvaEasyStore factory for our UvaEasyStore interface
func NewUvaEasyStore(config EasyStoreConfig) (EasyStore, error) {

	// mock the implementation here if necessary
	s3, err := newUvaEasyStore(config)
	return s3, err
}

//
// end of file
//
