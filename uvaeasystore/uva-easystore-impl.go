//
//
//

package uvaeasystore

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

	which := BaseComponent
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

	// do we update fields
	if (which & Fields) == Fields {
		logDebug(impl.config.log, fmt.Sprintf("updating fields for oid [%s]", obj.Id()))
		// delete the current fields
		err := impl.store.DeleteFieldsByOid(obj.Id())
		if err != nil {
			return nil, err
		}

		// if we have new fields, add them
		if len(obj.Fields()) != 0 {
			err := impl.store.AddFields(obj.Id(), obj.Fields())
			if err != nil {
				return nil, err
			}
		}
	}

	// do we update files
	if (which & Files) == Files {
		logDebug(impl.config.log, fmt.Sprintf("updating files for oid [%s]", obj.Id()))
		// delete the current files
		err := impl.store.DeleteBlobsByOid(obj.Id())
		if err != nil {
			return nil, err
		}

		// if we have new files, add them
		if len(obj.Files()) != 0 {
			for _, b := range obj.Files() {
				err = impl.store.AddBlob(obj.Id(), b)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// do we update metadata
	if (which & Metadata) == Metadata {
		logDebug(impl.config.log, fmt.Sprintf("updating metadata for oid [%s]", obj.Id()))
		// delete the current metadata
		err := impl.store.DeleteMetadataByOid(obj.Id())
		if err != nil {
			return nil, err
		}

		// if we have new metadata, add it
		if obj.Metadata() != nil {
			err := impl.store.AddMetadata(obj.Id(), obj.Metadata())
			if err != nil {
				return nil, err
			}
		}

	}

	// return the original object
	return obj, nil
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

	// special case, if we are asking for the base component, it means delete everything
	if which == BaseComponent {
		logDebug(impl.config.log, fmt.Sprintf("deleting oid [%s]", obj.Id()))
		err := impl.store.DeleteObjectByOid(obj.Id())
		if err != nil {
			return nil, err
		}

		// and delete remaining components
		which = AllComponents
	}

	// do we delete fields
	if (which & Fields) == Fields {
		logDebug(impl.config.log, fmt.Sprintf("deleting fields for oid [%s]", obj.Id()))
		err := impl.store.DeleteFieldsByOid(obj.Id())
		if err != nil {
			return nil, err
		}
	}

	// do we delete files
	if (which & Files) == Files {
		logDebug(impl.config.log, fmt.Sprintf("deleting files for oid [%s]", obj.Id()))
		err := impl.store.DeleteBlobsByOid(obj.Id())
		if err != nil {
			return nil, err
		}
	}

	// do we delete metadata
	if (which & Metadata) == Metadata {
		logDebug(impl.config.log, fmt.Sprintf("deleting metadata for oid [%s]", obj.Id()))
		err := impl.store.DeleteMetadataByOid(obj.Id())
		if err != nil {
			return nil, err
		}
	}

	// return the original object
	return obj, nil
}

//
// end of file
//
