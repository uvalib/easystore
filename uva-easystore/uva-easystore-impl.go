//
//
//

package uva_easystore

// this is our easystore implementation
type easyStoreImpl struct {
	easyStoreReadonlyImpl
}

// factory for our easystore interface
func newEasyStore(config EasyStoreConfig) (EasyStore, error) {
	return easyStoreImpl{}, nil
}

func (impl easyStoreImpl) Create(obj EasyStoreObject) (EasyStoreObject, error) {
	return nil, ErrNotImplemented
}

func (impl easyStoreImpl) Update(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {
	return nil, ErrNotImplemented
}

func (impl easyStoreImpl) Delete(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {
	return nil, ErrNotImplemented
}

//
// end of file
//
