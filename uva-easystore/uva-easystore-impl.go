//
//
//

package uva_easystore

import "fmt"

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

	// create the data store for this namespace
	s, err := NewDatastore(c.namespace)
	if err != nil {
		return nil, err
	}

	logInfo(c.log, fmt.Sprintf("new easystore (ns: %s)", c.namespace))
	return easyStoreImpl{easyStoreReadonlyImpl{config: c, store: s}}, nil
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

	which := NoComponents
	logInfo(impl.config.log, fmt.Sprintf("creating new oid [%s]", obj.Id()))

	// add the object
	err := impl.store.AddObject(obj)
	if err != nil {
		return nil, err
	}

	// do we add metadata
	if obj.Metadata() != nil {
		logDebug(impl.config.log, fmt.Sprintf("adding metadata for oid [%s]", obj.Id()))
		err = impl.store.AddMetadata(obj.Id(), obj.Metadata())
		if err != nil {
			return nil, err
		}
		which += Metadata
	}

	// do we add fields
	if len(obj.Fields()) != 0 {
		logDebug(impl.config.log, fmt.Sprintf("adding fields for oid [%s]", obj.Id()))
		err = impl.store.AddFields(obj.Id(), obj.Fields())
		if err != nil {
			return nil, err
		}
		which += Fields
	}

	// do we add files
	if len(obj.Files()) != 0 {
		logDebug(impl.config.log, fmt.Sprintf("adding files for oid [%s]", obj.Id()))
		for _, b := range obj.Files() {
			err = impl.store.AddBlob(obj.Id(), b)
			if err != nil {
				return nil, err
			}
		}
		which += Files
	}

	// get the full object
	return impl.GetById(obj.Id(), which)
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
