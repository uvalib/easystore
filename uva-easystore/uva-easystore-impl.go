//
//
//

package uva_easystore

// this is our easystore implementation
type easyStoreImpl struct {
}

// factory for our easystore interface
func newEasyStore(config EasyStoreConfig) (EasyStore, error) {
	return easyStoreImpl{}, nil
}

func (impl easyStoreImpl) GetById(id string, which EasyStoreComponents) (EasyStoreObject, error) {
	return nil, ErrNotImplemented
}

func (impl easyStoreImpl) GetByIds(ids []string, which EasyStoreComponents) (EasyStoreSet, error) {
	return nil, ErrNotImplemented
}

func (impl easyStoreImpl) GetByMetadata(metadata EasyStoreObjectMetadata, which EasyStoreComponents) (EasyStoreSet, error) {
	return nil, ErrNotImplemented
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
