//
//
//

package uva_easystore

import (
	"io"
)

// this is our easystore object implementation
type easyStoreObjectSetImpl struct {
	current uint              // current object index
	objects []EasyStoreObject // object list
}

// factory for our easystore object set interface
func newEasyStoreObjectSet(objs []EasyStoreObject) EasyStoreObjectSet {
	return &easyStoreObjectSetImpl{current: 0, objects: objs}
}

func (impl *easyStoreObjectSetImpl) Count() uint {
	return uint(len(impl.objects))
}

func (impl *easyStoreObjectSetImpl) Next() (EasyStoreObject, error) {
	if impl.current == uint(len(impl.objects)) {
		return nil, io.EOF
	}

	prev := impl.current
	impl.current++
	return impl.objects[prev], nil
}

//
// end of file
//
