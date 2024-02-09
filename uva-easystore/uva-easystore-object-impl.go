//
//
//

package uva_easystore

import (
	"time"
)

// this is our easystore object implementation
type easyStoreObjectImpl struct {
	id       string                  // object identifier
	version  string                  // object version (opaque)
	created  time.Time               // created time
	modified time.Time               // last modified time
	metadata EasyStoreObjectMetadata // object metadata
}

// factory for our easystore object interface
func newEasyStoreObject(id string) EasyStoreObject {
	return easyStoreObjectImpl{}
}

func (impl easyStoreObjectImpl) Id() string {
	return impl.id
}

func (impl easyStoreObjectImpl) VersionHandle() string {
	return impl.version
}

func (impl easyStoreObjectImpl) Created() time.Time {
	return impl.created
}

func (impl easyStoreObjectImpl) Modified() time.Time {
	return impl.modified
}

func (impl easyStoreObjectImpl) Metadata() EasyStoreObjectMetadata {
	return impl.metadata
}

func (impl easyStoreObjectImpl) StoredJson() EasyStoreBlob {
	sj := easyStoreBlobImpl{}
	return sj
}

func (impl easyStoreObjectImpl) Blobs() []EasyStoreBlob {
	b0 := easyStoreBlobImpl{}
	return []EasyStoreBlob{b0}
}

//
// end of file
//
