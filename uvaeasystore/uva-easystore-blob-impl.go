//
//
//

package uvaeasystore

import (
	"time"
)

// this is our easystore blob implementation
type easyStoreBlobImpl struct {
	Name_     string    `json:"name"`              // source file name
	MimeType_ string    `json:"mimetype"`          // mime type (if we know it)
	Url_      string    `json:"url,omitempty"`     // payload access url
	Payload_  []byte    `json:"payload,omitempty"` // payload
	Created_  time.Time `json:"created"`           // created time
	Modified_ time.Time `json:"modified"`          // last modified time
}

// factory for our easystore blob interface
func newEasyStoreBlob(name string, mimeType string, payload []byte) EasyStoreBlob {
	return &easyStoreBlobImpl{Name_: name, MimeType_: mimeType, Payload_: payload}
}

func (impl easyStoreBlobImpl) Name() string {
	return impl.Name_
}

func (impl easyStoreBlobImpl) MimeType() string {
	return impl.MimeType_
}

func (impl easyStoreBlobImpl) Url() string {
	return impl.Url_
}

func (impl easyStoreBlobImpl) Payload() ([]byte, error) {
	return impl.Payload_, nil
}

func (impl easyStoreBlobImpl) Created() time.Time {
	return impl.Created_
}

func (impl easyStoreBlobImpl) Modified() time.Time {
	return impl.Modified_
}

//
// end of file
//
