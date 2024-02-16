//
//
//

package uva_easystore

import (
	"time"
)

// this is our easystore metadata implementation
type easyStoreMetadataImpl struct {
	mimeType string    // mime type (if we know it)
	payload  []byte    // not exposed
	created  time.Time // created time
	modified time.Time // last modified time
}

// factory for our easystore metadata interface
func newEasyStoreMetadata(mimeType string, payload []byte) EasyStoreMetadata {
	return &easyStoreMetadataImpl{mimeType: mimeType, payload: payload}
}

func (impl easyStoreMetadataImpl) MimeType() string {
	return impl.mimeType
}

func (impl easyStoreMetadataImpl) Payload() []byte {
	return impl.payload
}

func (impl easyStoreMetadataImpl) Created() time.Time {
	return impl.created
}

func (impl easyStoreMetadataImpl) Modified() time.Time {
	return impl.modified
}

//
// end of file
//
