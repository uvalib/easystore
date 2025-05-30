//
//
//

package uvaeasystore

import (
	"time"
)

// this is our easystore blob implementation
type easyStoreBlobImpl struct {
	Name_     string    `json:"name"`     // source file name
	MimeType_ string    `json:"mimetype"` // mime type (if we know it)
	payload   []byte    // not exposed
	Created_  time.Time `json:"created"`  // created time
	Modified_ time.Time `json:"modified"` // last modified time
}

// factory for our easystore blob interface
func newEasyStoreBlob(name string, mimeType string, payload []byte) EasyStoreBlob {
	return &easyStoreBlobImpl{Name_: name, MimeType_: mimeType, payload: payload}
}

func (impl easyStoreBlobImpl) Name() string {
	return impl.Name_
}

func (impl easyStoreBlobImpl) MimeType() string {
	return impl.MimeType_
}

func (impl easyStoreBlobImpl) Url() string {
	return "https://does.not.work.fu"
}

func (impl easyStoreBlobImpl) Payload() ([]byte, error) {
	return impl.payload, nil
}

//func (impl easyStoreBlobImpl) Read(buf []byte) (int, error) { return 0, nil }

func (impl easyStoreBlobImpl) Created() time.Time {
	return impl.Created_
}

func (impl easyStoreBlobImpl) Modified() time.Time {
	return impl.Modified_
}

//
// end of file
//
