//
//
//

package uvaeasystore

import (
	"time"
)

// this is our easystore object implementation
type easyStoreObjectImpl struct {
	Namespace_ string                `json:"namespace"`          // object namespace
	Id_        string                `json:"id"`                 // object identifier
	Vtag_      string                `json:"vtag"`               // object version tag (opaque)
	Created_   time.Time             `json:"created"`            // created time
	Modified_  time.Time             `json:"modified"`           // last modified time
	Fields_    EasyStoreObjectFields `json:"fields,omitempty"`   // object fields
	Metadata_  EasyStoreMetadata     `json:"metadata,omitempty"` // object metadata (its an opaque blob)
	Files_     []EasyStoreBlob       `json:"files,omitempty"`    // object files
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
	return proxyEasyStoreObject(namespace, id, newVtag())
}

// proxy object, used as a container for existing values
func proxyEasyStoreObject(namespace string, id string, vtag string) EasyStoreObject {
	return &easyStoreObjectImpl{
		Namespace_: namespace,
		Id_:        id,
		Vtag_:      vtag,
	}
}

func (impl *easyStoreObjectImpl) Namespace() string {
	return impl.Namespace_
}

func (impl *easyStoreObjectImpl) Id() string {
	return impl.Id_
}

func (impl *easyStoreObjectImpl) VTag() string {
	return impl.Vtag_
}

func (impl *easyStoreObjectImpl) Created() time.Time {
	return impl.Created_
}

func (impl *easyStoreObjectImpl) Modified() time.Time {
	return impl.Modified_
}

func (impl *easyStoreObjectImpl) Fields() EasyStoreObjectFields {
	return impl.Fields_
}

func (impl *easyStoreObjectImpl) Metadata() EasyStoreMetadata {
	return impl.Metadata_
}

func (impl *easyStoreObjectImpl) Files() []EasyStoreBlob {
	return impl.Files_
}

func (impl *easyStoreObjectImpl) SetNamespace(namespace string) {
	impl.Namespace_ = namespace
}

func (impl *easyStoreObjectImpl) SetFields(fields EasyStoreObjectFields) {
	impl.Fields_ = fields
}

func (impl *easyStoreObjectImpl) SetMetadata(metadata EasyStoreMetadata) {
	impl.Metadata_ = metadata
}

func (impl *easyStoreObjectImpl) SetFiles(files []EasyStoreBlob) {
	impl.Files_ = files
}

//
// end of file
//
