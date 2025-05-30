//
//
//

package uvaeasystore

import (
	"time"
)

// this is our easystore metadata implementation
type easyStoreMetadataImpl struct {
	MimeType_ string    `json:"mimetype"` // mime type (if we know it)
	Payload_  []byte    `json:"payload"`  // opaque
	Created_  time.Time `json:"created"`  // created time
	Modified_ time.Time `json:"modified"` // last modified time
}

// factory for our easystore metadata interface
func newEasyStoreMetadata(mimeType string, payload []byte) EasyStoreMetadata {
	return &easyStoreMetadataImpl{MimeType_: mimeType, Payload_: payload}
}

func (impl easyStoreMetadataImpl) MimeType() string {
	return impl.MimeType_
}

func (impl easyStoreMetadataImpl) Payload() ([]byte, error) {
	return impl.Payload_, nil
}

func (impl easyStoreMetadataImpl) Created() time.Time {
	return impl.Created_
}

func (impl easyStoreMetadataImpl) Modified() time.Time {
	return impl.Modified_
}

//
// end of file
//
