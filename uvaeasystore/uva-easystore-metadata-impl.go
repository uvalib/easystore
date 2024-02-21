//
//
//

package uvaeasystore

import (
	"encoding/base64"
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
	p := payload
	if payload != nil {
		p = []byte(base64.StdEncoding.EncodeToString(payload))
	}
	return &easyStoreMetadataImpl{mimeType: mimeType, payload: p}
}

func (impl easyStoreMetadataImpl) MimeType() string {
	return impl.mimeType
}

func (impl easyStoreMetadataImpl) Payload() []byte {
	b, _ := base64.StdEncoding.DecodeString(string(impl.payload))
	return b
}

func (impl easyStoreMetadataImpl) PayloadNative() []byte {
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
