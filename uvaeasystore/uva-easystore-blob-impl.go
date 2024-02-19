//
//
//

package uvaeasystore

import (
	"time"
)

// this is our easystore blob implementation
type easyStoreBlobImpl struct {
	name     string    // source file name
	mimeType string    // mime type (if we know it)
	payload  []byte    // not exposed
	created  time.Time // created time
	modified time.Time // last modified time
}

// factory for our easystore blob interface
func newEasyStoreBlob(name string, mimeType string, payload []byte) EasyStoreBlob {
	return &easyStoreBlobImpl{name: name, mimeType: mimeType, payload: payload}
}

func (impl easyStoreBlobImpl) Name() string {
	return impl.name
}

func (impl easyStoreBlobImpl) MimeType() string {
	return impl.mimeType
}

func (impl easyStoreBlobImpl) Url() string {
	return "https://does.not.work.fu"
}

//func (impl easyStoreBlobImpl) Payload() []byte {
//	return nil
//}

//func (impl easyStoreBlobImpl) Read(buf []byte) (int, error) { return 0, nil }

func (impl easyStoreBlobImpl) Created() time.Time {
	return impl.created
}

func (impl easyStoreBlobImpl) Modified() time.Time {
	return impl.modified
}

//
// end of file
//
