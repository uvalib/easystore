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

	// create the data store for this Namespace
	s, err := NewDatastore(config)
	if err != nil {
		return nil, err
	}

	logInfo(config.Logger(), fmt.Sprintf("new easystore"))
	return easyStoreImpl{easyStoreReadonlyImpl{config: config, store: s}}, nil
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

	logInfo(impl.config.Logger(), fmt.Sprintf("creating new oid [%s]", obj.Id()))

	// add the object
	err := impl.store.AddObject(obj)
	if err != nil {
		return nil, err
	}

	// do we add metadata
	if obj.Metadata() != nil {
		logDebug(impl.config.Logger(), fmt.Sprintf("adding metadata for oid [%s]", obj.Id()))
		err = impl.store.AddMetadata(DataStoreKey{obj.Namespace(), obj.Id()}, obj.Metadata())
		if err != nil {
			return nil, err
		}
	}

	// do we add fields
	if len(obj.Fields()) != 0 {
		logDebug(impl.config.Logger(), fmt.Sprintf("adding fields for oid [%s]", obj.Id()))
		err = impl.store.AddFields(DataStoreKey{obj.Namespace(), obj.Id()}, obj.Fields())
		if err != nil {
			return nil, err
		}
	}

	// do we add files
	if len(obj.Files()) != 0 {
		logDebug(impl.config.Logger(), fmt.Sprintf("adding files for oid [%s]", obj.Id()))
		for _, b := range obj.Files() {
			err = impl.store.AddBlob(DataStoreKey{obj.Namespace(), obj.Id()}, b)
			if err != nil {
				return nil, err
			}
		}
	}

	// get the full object
	return impl.GetByKey(obj.Namespace(), obj.Id(), AllComponents)
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
		logDebug(impl.config.Logger(), fmt.Sprintf("updating fields for oid [%s]", obj.Id()))
		// delete the current fields
		err := impl.store.DeleteFieldsByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}

		// if we have new fields, add them
		if len(obj.Fields()) != 0 {
			err := impl.store.AddFields(DataStoreKey{obj.Namespace(), obj.Id()}, obj.Fields())
			if err != nil {
				return nil, err
			}
		}
	}

	// do we update files
	if (which & Files) == Files {
		logDebug(impl.config.Logger(), fmt.Sprintf("updating files for oid [%s]", obj.Id()))
		// delete the current files
		err := impl.store.DeleteBlobsByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}

		// if we have new files, add them
		if len(obj.Files()) != 0 {
			for _, b := range obj.Files() {
				err = impl.store.AddBlob(DataStoreKey{obj.Namespace(), obj.Id()}, b)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// do we update metadata
	if (which & Metadata) == Metadata {
		logDebug(impl.config.Logger(), fmt.Sprintf("updating metadata for oid [%s]", obj.Id()))
		// delete the current metadata
		err := impl.store.DeleteMetadataByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}

		// if we have new metadata, add it
		if obj.Metadata() != nil {
			err := impl.store.AddMetadata(DataStoreKey{obj.Namespace(), obj.Id()}, obj.Metadata())
			if err != nil {
				return nil, err
			}
		}

	}

	// get the full object
	return impl.GetByKey(obj.Namespace(), obj.Id(), AllComponents)
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
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting oid [%s]", obj.Id()))
		err := impl.store.DeleteObjectByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}

		// and delete remaining components
		which = AllComponents
	}

	// do we delete fields
	if (which & Fields) == Fields {
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting fields for oid [%s]", obj.Id()))
		err := impl.store.DeleteFieldsByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}
	}

	// do we delete files
	if (which & Files) == Files {
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting files for oid [%s]", obj.Id()))
		err := impl.store.DeleteBlobsByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}
	}

	// do we delete metadata
	if (which & Metadata) == Metadata {
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting metadata for oid [%s]", obj.Id()))
		err := impl.store.DeleteMetadataByKey(DataStoreKey{obj.Namespace(), obj.Id()})
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
