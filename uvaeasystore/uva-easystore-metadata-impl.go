//
//
//

package uvaeasystore

import (
	"time"
)

// this is our easystore metadata implementation
type easyStoreMetadataImpl struct {
	id       string    // metadata identifier (opaque)
	vtag     string    // metadata version tag
	mimeType string    // mime type (if we know it)
	payload  []byte    // not exposed
	created  time.Time // created time
	modified time.Time // last modified time
}

// factory for our easystore metadata interface
func newEasyStoreMetadata(mimeType string, payload []byte) EasyStoreMetadata {
	return &easyStoreMetadataImpl{
		id:       newBlobId(),
		vtag:     newVTag(),
		mimeType: mimeType,
		payload:  payload}
}

func (impl easyStoreMetadataImpl) Id() string {
	return impl.id
}

func (impl easyStoreMetadataImpl) VTag() string {
	return impl.vtag
}

func (impl easyStoreMetadataImpl) MimeType() string {
	return impl.mimeType
}

func (impl easyStoreMetadataImpl) Payload() ([]byte, error) {
	return impl.payload, nil
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
