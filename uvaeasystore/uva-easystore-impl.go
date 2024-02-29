//
//
//

package uvaeasystore

import (
	"fmt"
	"github.com/uvalib/librabus-sdk/uvalibrabus"
)

// this is our easystore implementation
type easyStoreImpl struct {
	messageBus            uvalibrabus.UvaBus // the event bus
	easyStoreReadonlyImpl                    // the read only implementation
}

// factory for our easystore interface
func newEasyStore(config EasyStoreConfig) (EasyStore, error) {

	// create the data store
	store, err := NewDatastore(config)
	if err != nil {
		return nil, err
	}

	// create the message bus
	bus, err := NewEventBus(config.EventSource(), config.MessageBus(), config.Logger())
	if err != nil {
		return nil, err
	}

	logInfo(config.Logger(), fmt.Sprintf("new easystore"))
	return easyStoreImpl{bus, easyStoreReadonlyImpl{config: config, store: store}}, nil
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

	logInfo(impl.config.Logger(), fmt.Sprintf("creating new ns/oid [%s/%s]", obj.Namespace(), obj.Id()))

	// add the object
	err := impl.store.AddObject(obj)
	if err != nil {
		return nil, err
	}

	// do we add metadata
	if obj.Metadata() != nil {
		logDebug(impl.config.Logger(), fmt.Sprintf("adding metadata for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		err = impl.store.AddMetadata(DataStoreKey{obj.Namespace(), obj.Id()}, obj.Metadata())
		if err != nil {
			return nil, err
		}
	}

	// do we add fields
	if len(obj.Fields()) != 0 {
		logDebug(impl.config.Logger(), fmt.Sprintf("adding fields for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		err = impl.store.AddFields(DataStoreKey{obj.Namespace(), obj.Id()}, obj.Fields())
		if err != nil {
			return nil, err
		}
	}

	// do we add files
	if len(obj.Files()) != 0 {
		logDebug(impl.config.Logger(), fmt.Sprintf("adding files for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		for _, b := range obj.Files() {
			err = impl.store.AddBlob(DataStoreKey{obj.Namespace(), obj.Id()}, b)
			if err != nil {
				return nil, err
			}
		}
	}

	// publish the appropriate event
	_ = pubObjectCreate(impl.messageBus, obj)

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
		logDebug(impl.config.Logger(), fmt.Sprintf("updating fields for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
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
		logDebug(impl.config.Logger(), fmt.Sprintf("updating files for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
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

				_ = pubFileCreate(impl.messageBus, obj)
			}
		}
	}

	// do we update metadata
	if (which & Metadata) == Metadata {
		logDebug(impl.config.Logger(), fmt.Sprintf("updating metadata for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
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
			_ = pubMetadataUpdate(impl.messageBus, obj)
		}
	}

	// publish the appropriate event
	_ = pubObjectUpdate(impl.messageBus, obj)

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
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		err := impl.store.DeleteObjectByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}

		// and delete remaining components
		which = AllComponents
	}

	// do we delete fields
	if (which & Fields) == Fields {
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting fields for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		err := impl.store.DeleteFieldsByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}
	}

	// do we delete files
	if (which & Files) == Files {
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting files for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		err := impl.store.DeleteBlobsByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}
	}

	// do we delete metadata
	if (which & Metadata) == Metadata {
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting metadata for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		err := impl.store.DeleteMetadataByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}
	}

	// publish the appropriate event
	_ = pubObjectDelete(impl.messageBus, obj)

	// return the original object
	return obj, nil
}

//
// end of file
//
