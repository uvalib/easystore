//
//
//

package uva_easystore

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"
)

// this is our easystore object implementation
type easyStoreObjectImpl struct {
	id       string                // object identifier
	accessId string                // object access Id (opaque)
	created  time.Time             // created time
	modified time.Time             // last modified time
	fields   EasyStoreObjectFields // object fields
	metadata EasyStoreMetadata     // object metadata (its an opaque blob)
	files    []EasyStoreBlob       // object files
}

// factory for our easystore object interface
func newEasyStoreObject(id string) EasyStoreObject {
	return easyStoreObjectImpl{
		id:       id,
		accessId: newAccessId(),
	}
}

func (impl easyStoreObjectImpl) Id() string {
	return impl.id
}

func (impl easyStoreObjectImpl) AccessId() string {
	return impl.accessId
}

func (impl easyStoreObjectImpl) Created() time.Time {
	return impl.created
}

func (impl easyStoreObjectImpl) Modified() time.Time {
	return impl.modified
}

func (impl easyStoreObjectImpl) Fields() EasyStoreObjectFields {
	return impl.fields
}

func (impl easyStoreObjectImpl) Metadata() EasyStoreMetadata {
	return impl.metadata
}

func (impl easyStoreObjectImpl) Files() []EasyStoreBlob {
	return impl.files
}

func newAccessId() string {
	b := make([]byte, 6) // equals 12 characters
	rand.Read(b)
	return fmt.Sprintf("aid:%s", hex.EncodeToString(b))
}

//
// end of file
//
