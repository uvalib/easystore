//
//
//

package uva_easystore

import "log"

// this is our easystore implementation
type easyStoreImpl struct {
	easyStoreReadonlyImpl
}

// factory for our easystore interface
func newEasyStore(config EasyStoreConfig) (EasyStore, error) {

	// we know it's one of these
	c, _ := config.(*easyStoreConfigImpl)

	// validate the namespace
	if len(c.namespace) == 0 {
		return nil, ErrBadParameter
	}

	// add standard logger if none provided
	if c.log == nil {
		c.log = log.Default()
	}

	c.log.Printf("INFO: new easystore (ns: %s)", c.namespace)
	return easyStoreImpl{easyStoreReadonlyImpl{config: c}}, nil
}

func (impl easyStoreImpl) Create(obj EasyStoreObject) (EasyStoreObject, error) {

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object id
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	return nil, ErrNotImplemented
}

func (impl easyStoreImpl) Update(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object id
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	return nil, ErrNotImplemented
}

func (impl easyStoreImpl) Delete(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object id
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	return nil, ErrNotImplemented
}

//
// end of file
//
