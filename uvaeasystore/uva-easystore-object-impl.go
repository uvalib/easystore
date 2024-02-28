//
//
//

package uvaeasystore

import (
	"time"
)

// this is our easystore object implementation
type easyStoreObjectImpl struct {
	namespace string                // object namespace
	id        string                // object identifier
	accessId  string                // object access Id (opaque)
	created   time.Time             // created time
	modified  time.Time             // last modified time
	fields    EasyStoreObjectFields // object fields
	metadata  EasyStoreMetadata     // object metadata (its an opaque blob)
	files     []EasyStoreBlob       // object files
}

// factory for our easystore object interface
func newEasyStoreObject(namespace string, id string) EasyStoreObject {

	// if a namespace is not provided, we use a default
	if len(namespace) == 0 {
		namespace = "default"
	}
	// if an identifier is not provided, one will be provided for you
	if len(id) == 0 {
		id = newObjectId()
	}
	return &easyStoreObjectImpl{
		namespace: namespace,
		id:        id,
		accessId:  newAccessId(),
	}
}

func (impl *easyStoreObjectImpl) Namespace() string {
	return impl.namespace
}

func (impl *easyStoreObjectImpl) Id() string {
	return impl.id
}

func (impl *easyStoreObjectImpl) AccessId() string {
	return impl.accessId
}

func (impl *easyStoreObjectImpl) Created() time.Time {
	return impl.created
}

func (impl *easyStoreObjectImpl) Modified() time.Time {
	return impl.modified
}

func (impl *easyStoreObjectImpl) Fields() EasyStoreObjectFields {
	return impl.fields
}

func (impl *easyStoreObjectImpl) Metadata() EasyStoreMetadata {
	return impl.metadata
}

func (impl *easyStoreObjectImpl) Files() []EasyStoreBlob {
	return impl.files
}

func (impl *easyStoreObjectImpl) SetFields(fields EasyStoreObjectFields) {
	impl.fields = fields
}

func (impl *easyStoreObjectImpl) SetMetadata(metadata EasyStoreMetadata) {
	impl.metadata = metadata
}

func (impl *easyStoreObjectImpl) SetFiles(files []EasyStoreBlob) {
	impl.files = files
}

//
// end of file
//
