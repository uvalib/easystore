//
//
//

package uva_easystore

import (
	"time"
)

// this is our easystore blob implementation
type easyStoreBlobImpl struct {
	id         string    // blob identifier
	sourceName string    // source file name
	mimeType   string    // mime type (if we know it)
	created    time.Time // created time
	modified   time.Time // last modified time
}

func (impl easyStoreBlobImpl) Id() string {
	return impl.id
}

func (impl easyStoreBlobImpl) SourceName() string {
	return impl.sourceName
}

func (impl easyStoreBlobImpl) MimeType() string {
	return impl.mimeType
}

func (impl easyStoreBlobImpl) Created() time.Time {
	return impl.created
}

func (impl easyStoreBlobImpl) Modified() time.Time {
	return impl.modified
}

//
// end of file
//
