//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	"errors"
	"fmt"
	"github.com/uvalib/librabus-sdk/uvalibrabus"
)

// this is our easystore implementation
type easyStoreImpl struct {
	messageBus            uvalibrabus.UvaBus // the event bus
	easyStoreReadonlyImpl                    // the read-only implementation
}

// factory for our easystore interface
func newEasyStore(config EasyStoreImplConfig) (EasyStore, error) {

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

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
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

	// publish the appropriate event, errors are not too important
	err = pubObjectCreate(impl.messageBus, obj)
	if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
		logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
	}

	// get the full object
	return impl.GetByKey(obj.Namespace(), obj.Id(), AllComponents)
}

func (impl easyStoreImpl) Update(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	// get the current object and compare the vtag
	current, err := impl.GetByKey(obj.Namespace(), obj.Id(), BaseComponent)
	if err != nil {
		return nil, err
	}
	if current.VTag() != obj.VTag() {
		logError(impl.config.Logger(), fmt.Sprintf("stale vtag; req [%s], cur [%s]", obj.VTag(), current.VTag()))
		return nil, ErrStaleObject
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

				// publish the appropriate event, errors are not too important
				err = pubFileCreate(impl.messageBus, obj)
				if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
					logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
				}
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

			// publish the appropriate event, errors are not too important
			err = pubMetadataUpdate(impl.messageBus, obj)
			if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
				logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
			}
		}
	}

	// update the object (timestamp and vtag)
	err = impl.store.UpdateObject(DataStoreKey{obj.Namespace(), obj.Id()})
	if err != nil {
		return nil, err
	}

	// publish the appropriate event, errors are not too important
	err = pubObjectUpdate(impl.messageBus, obj)
	if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
		logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
	}

	// get the full object
	return impl.GetByKey(obj.Namespace(), obj.Id(), AllComponents)
}

func (impl easyStoreImpl) Delete(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	// get the current object and compare the vtag
	current, err := impl.GetByKey(obj.Namespace(), obj.Id(), BaseComponent)
	if err != nil {
		return nil, err
	}
	if current.VTag() != obj.VTag() {
		logError(impl.config.Logger(), fmt.Sprintf("stale vtag; req [%s], cur [%s]", obj.VTag(), current.VTag()))
		return nil, ErrStaleObject
	}

	// special case, if we are asking for the base component, it means delete everything
	deleteAll := false
	if which == BaseComponent {
		logDebug(impl.config.Logger(), fmt.Sprintf("deleting ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		err := impl.store.DeleteObjectByKey(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}

		// and delete remaining components
		which = AllComponents
		deleteAll = true
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

	// if we did not delete the component
	if deleteAll == false {
		// update the object (timestamp and vtag)
		err = impl.store.UpdateObject(DataStoreKey{obj.Namespace(), obj.Id()})
		if err != nil {
			return nil, err
		}
	}

	// publish the appropriate event, errors are not too important
	err = pubObjectDelete(impl.messageBus, obj)
	if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
		logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
	}

	// return the original object
	return obj, nil
}

func (impl easyStoreImpl) Rename(obj EasyStoreObject, name string, newName string) (EasyStoreObject, error) {

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return nil, ErrBadParameter
	}

	// ensure our inputs are good
	if len(name) == 0 {
		return nil, ErrBadParameter
	}
	if len(newName) == 0 {
		return nil, ErrBadParameter
	}

	// ensure we actually have files
	files := obj.Files()
	if files == nil {
		return nil, ErrBadParameter
	}
	// and we have one named as specified and not one named as its replacement
	found := false
	duplicate := false
	for _, file := range files {
		if file.Name() == name {
			found = true
		}
		if file.Name() == newName {
			duplicate = true
		}
	}
	if found == false || duplicate == true {
		return nil, ErrBadParameter
	}

	return nil, ErrNotImplemented
}

//
// end of file
//
