//
//
//

package uva_easystore

// this is our easystore readonly implementation
type easyStoreReadonlyImpl struct {
}

// factory for our easystore interface
func newEasyStoreReadonly(config EasyStoreConfig) (EasyStoreReadonly, error) {
	return easyStoreReadonlyImpl{}, nil
}

func (impl easyStoreReadonlyImpl) GetById(id string, which EasyStoreComponents) (EasyStoreObject, error) {
	return nil, ErrNotImplemented
}

func (impl easyStoreReadonlyImpl) GetByIds(ids []string, which EasyStoreComponents) (EasyStoreObjectSet, error) {
	return nil, ErrNotImplemented
}

func (impl easyStoreReadonlyImpl) GetByFields(metadata EasyStoreObjectFields, which EasyStoreComponents) (EasyStoreObjectSet, error) {
	return nil, ErrNotImplemented
}

//
// end of file
//
