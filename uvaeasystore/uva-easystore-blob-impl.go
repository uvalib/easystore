//
//
//

package uvaeasystore

import (
	"encoding/base64"
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
	p := payload
	if payload != nil {
		p = []byte(base64.StdEncoding.EncodeToString(payload))
	}
	return &easyStoreBlobImpl{name: name, mimeType: mimeType, payload: p}
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

func (impl easyStoreBlobImpl) Payload() []byte {
	b, _ := base64.StdEncoding.DecodeString(string(impl.payload))
	return b
}

func (impl easyStoreBlobImpl) PayloadNative() []byte {
	return impl.payload
}

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
