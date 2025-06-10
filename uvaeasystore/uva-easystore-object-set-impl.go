//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	"io"
)

// this is our easystore object implementation
type easyStoreObjectSetImpl struct {
	current uint                  // current object index
	which   EasyStoreComponents   // which components are we requesting
	objects []EasyStoreObject     // object list
	store   easyStoreReadonlyImpl // we get objects when required
}

// factory for our easystore object set interface
func newEasyStoreObjectSet(store easyStoreReadonlyImpl, objs []EasyStoreObject, which EasyStoreComponents) EasyStoreObjectSet {
	return &easyStoreObjectSetImpl{
		current: 0,
		which:   which,
		objects: objs,
		store:   store,
	}
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
	return impl.store.populateObject(impl.objects[prev], impl.which)
}

//
// end of file
//
