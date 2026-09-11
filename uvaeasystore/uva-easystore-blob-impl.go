//
//
//

package uvaeasystore

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"time"
)

// number of leading bytes used when we determine a mime type
var mimeDetectBytes = 512

// this is our easystore blob implementation
type easyStoreBlobImpl struct {
	Name_     string    `json:"name"`              // source file name
	MimeType_ string    `json:"mimetype"`          // mime type (if we know it)
	Url_      string    `json:"url,omitempty"`     // payload access url
	Payload_  []byte    `json:"payload,omitempty"` // payload
	Created_  time.Time `json:"created"`           // created time
	Modified_ time.Time `json:"modified"`          // last modified time

	// when this is set, the payload is available as a stream only (and can be read
	// just the once). It is never serialized
	reader io.ReadCloser
}

// factory for our easystore blob interface
func newEasyStoreBlob(name string, mimeType string, payload []byte) EasyStoreBlob {
	return &easyStoreBlobImpl{Name_: name, MimeType_: mimeType, Payload_: payload}
}

// factory for our easystore blob interface, the payload is streamed from the reader
func newEasyStoreBlobFromReader(name string, mimeType string, payload io.ReadCloser) EasyStoreBlob {
	return &easyStoreBlobImpl{Name_: name, MimeType_: mimeType, reader: payload}
}

// factory for our easystore blob interface, the payload is streamed from the named file
func newEasyStoreBlobFromFile(name string, mimeType string, fileName string) (EasyStoreBlob, error) {

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	// do we need to determine the mime type
	if len(mimeType) == 0 {
		mimeType, err = detectMimeType(file)
		if err != nil {
			file.Close()
			return nil, err
		}
	}

	return newEasyStoreBlobFromReader(name, mimeType, file), nil
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

	// a streamed payload is not available as a buffer
	if impl.reader != nil {
		return nil, ErrPayloadNotBuffered
	}
	return impl.Payload_, nil
}

func (impl easyStoreBlobImpl) PayloadReader() (io.ReadCloser, error) {

	// a streamed payload, hand out the stream
	if impl.reader != nil {
		return impl.reader, nil
	}

	// otherwise stream from the buffer we already have
	return io.NopCloser(bytes.NewReader(impl.Payload_)), nil
}

func (impl easyStoreBlobImpl) Created() time.Time {
	return impl.Created_
}

func (impl easyStoreBlobImpl) Modified() time.Time {
	return impl.Modified_
}

//
// private helpers
//

// blobIsStreaming -- is the payload of this blob only available as a stream
func blobIsStreaming(blob EasyStoreBlob) bool {

	// implementations are handed around as both pointers and values
	switch impl := blob.(type) {
	case *easyStoreBlobImpl:
		return impl.reader != nil
	case easyStoreBlobImpl:
		return impl.reader != nil
	}
	return false
}

// blobPayload -- the payload of this blob as a buffer, streamed payloads are consumed
// in their entirety so only use this when a buffer is unavoidable
func blobPayload(blob EasyStoreBlob) ([]byte, error) {

	if blobIsStreaming(blob) == false {
		return blob.Payload()
	}

	reader, err := blob.PayloadReader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// detectMimeType -- determine the mime type from the leading bytes of the file, the
// file is rewound afterwards so the payload can be streamed from the start
func detectMimeType(file *os.File) (string, error) {

	buf := make([]byte, mimeDetectBytes)
	count, err := file.Read(buf)
	if err != nil && errors.Is(err, io.EOF) == false {
		return "", err
	}

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	return http.DetectContentType(buf[0:count]), nil
}

//
// end of file
//
