//
//
//

package uva_easystore

import "fmt"

// this is our easystore readonly implementation
type easyStoreReadonlyImpl struct {
	config *easyStoreConfigImpl // configuration info
	store  DataStore            // storage/persistence implementation
}

// factory for our easystore interface
func newEasyStoreReadonly(config EasyStoreConfig) (EasyStoreReadonly, error) {

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

	logInfo(c.log, fmt.Sprintf("new readonly easystore (ns: %s)", c.namespace))
	return easyStoreReadonlyImpl{config: c, store: s}, nil
}

func (impl easyStoreReadonlyImpl) GetById(id string, which EasyStoreComponents) (EasyStoreObject, error) {

	// validate the id
	if len(id) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	logDebug(impl.config.log, fmt.Sprintf("getting id [%s]", id))

	// first get the base object (always required)
	o, err := impl.store.GetMetadataByOid(id)
	if err != nil {
		logInfo(impl.config.log, fmt.Sprintf("no metadata found for id [%s]", id))
		return nil, ErrObjectNotFound
	}

	// we know it's one of these
	obj, _ := o.(*easyStoreObjectImpl)

	// then get the opaque metadata (if required)
	if (which & Metadata) == Metadata {

	}

	// then get the fields (if required)
	if (which & Fields) == Fields {
		logDebug(impl.config.log, fmt.Sprintf("getting fields for id [%s]", id))
		fields, err := impl.store.GetFieldsByOid(id)
		if err == nil {
			obj.fields = *fields
		} else {
			logInfo(impl.config.log, fmt.Sprintf("no fields found for id [%s]", id))
		}
	}

	// lastly, the blobs (if required)
	if (which & Files) == Files {
		logDebug(impl.config.log, fmt.Sprintf("getting files for id [%s]", id))
		blobs, err := impl.store.GetBlobsByOid(id)
		if err == nil {
			obj.files = blobs
		} else {
			logInfo(impl.config.log, fmt.Sprintf("no blobs found for id [%s]", id))
		}
	}

	return o, nil
}

func (impl easyStoreReadonlyImpl) GetByIds(ids []string, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// validate the id list
	if len(ids) == 0 {
		return nil, ErrBadParameter
	}

	// validate each member
	for _, id := range ids {
		if len(id) == 0 {
			return nil, ErrBadParameter
		}
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	return nil, ErrNotImplemented
}

func (impl easyStoreReadonlyImpl) GetByFields(metadata EasyStoreObjectFields, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	return nil, ErrNotImplemented
}

//
// end of file
//
