//
//
//

package uvaeasystore

import (
	"log"
)

// this is our easystore config implementation
type easyStoreConfigImpl struct {
	namespace string      //
	log       *log.Logger // logging support
	// other stuff here
}

func (impl *easyStoreConfigImpl) Namespace(namespace string) {
	impl.namespace = namespace
}

func (impl *easyStoreConfigImpl) Logger(log *log.Logger) {
	impl.log = log
}

func newDefaultEasyStoreConfig() EasyStoreConfig {
	return &easyStoreConfigImpl{}
}

//
// end of file
//
