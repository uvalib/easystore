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
)

// this is our easystore readonly implementation
type easyStoreReadonlyImpl struct {
	config EasyStoreImplConfig // configuration info
	store  DataStore           // dbStorage/persistence implementation
}

// maximum number of ns/oid pairs to request in a single query
var querySplitCount = 250

// factory for our easystore interface
func newEasyStoreReadonly(config EasyStoreImplConfig) (EasyStoreReadonly, error) {

	// create the data store for this namespace
	s, err := NewDatastore(config)
	if err != nil {
		return nil, err
	}

	logInfo(config.Logger(), fmt.Sprintf("new readonly easystore"))
	return easyStoreReadonlyImpl{config: config, store: s}, nil
}

func (impl easyStoreReadonlyImpl) Close() error {
	return impl.store.Close()
}

func (impl easyStoreReadonlyImpl) Check() error {
	return impl.store.Check()
}

func (impl easyStoreReadonlyImpl) ObjectGetByKey(namespace string, id string, which EasyStoreComponents) (EasyStoreObject, error) {

	// preflight validation
	if err := GetByKeyPreflight(namespace, id, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// get the base object
	o, err := impl.getByKey(namespace, id)
	if err != nil {
		return nil, err
	}

	// populate the object and return it
	return impl.populateObject(o, which)
}

func (impl easyStoreReadonlyImpl) ObjectGetByKeys(namespace string, ids []string, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// preflight validation
	if err := GetByKeysPreflight(namespace, ids, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build our list of objects
	keys := make([]DataStoreKey, 0)
	for _, id := range ids {
		keys = append(keys, DataStoreKey{namespace, id})
	}

	objs, err := impl.getByKeys(keys)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		} else {
			return nil, err
		}
	}

	// bail out if we did not find any
	if len(objs) == 0 {
		return nil, ErrNotFound
	}

	// we get objects only when they are required
	return newEasyStoreObjectSet(impl, objs, which), nil
}

func (impl easyStoreReadonlyImpl) ObjectGetByFields(namespace string, fields EasyStoreObjectFields, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// preflight validation
	if err := GetByFieldsPreflight(namespace, fields, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	logDebug(impl.config.Logger(), fmt.Sprintf("getting by fields"))

	// first get the base objects (always required)
	keys, err := impl.store.GetKeysByFields(namespace, fields)
	if err != nil {
		// known error
		if errors.Is(err, ErrNotFound) {
			logInfo(impl.config.Logger(), fmt.Sprintf("no objects found"))
		} else {
			return nil, err
		}
	}

	// bail out if we did not find any
	// I think returning an error is better but this is what was requested
	objs := make([]EasyStoreObject, 0)
	if len(keys) == 0 {
		return newEasyStoreObjectSet(impl, objs, which), nil
	}

	objs, err = impl.getByKeys(keys)
	if err != nil {
		return nil, err
	}

	// we get objects only when they are required
	return newEasyStoreObjectSet(impl, objs, which), nil
}

func (impl easyStoreReadonlyImpl) FileGetByKey(namespace string, oid string, name string) (EasyStoreBlob, error) {
	return nil, ErrNotImplemented
}

//
// private methods
//

func (impl easyStoreReadonlyImpl) getByKey(namespace string, id string) (EasyStoreObject, error) {

	logDebug(impl.config.Logger(), fmt.Sprintf("getting ns/oid [%s/%s]", namespace, id))

	// get the base object (always required)
	o, err := impl.store.GetObjectByKey(DataStoreKey{namespace, id}, FROMCACHE)
	if err != nil {
		// known error
		if errors.Is(err, ErrNotFound) {
			logInfo(impl.config.Logger(), fmt.Sprintf("no object found for ns/oid [%s/%s]", namespace, id))
			return nil, ErrNotFound
		} else {
			return nil, err
		}
	}
	return o, nil
}

func (impl easyStoreReadonlyImpl) getByKeys(keys []DataStoreKey) ([]EasyStoreObject, error) {

	if len(keys) > querySplitCount {

		half := len(keys) / 2
		if half == 0 {
			// an insane situation, bomb out
			logError(impl.config.Logger(), "cannot split block further")
			return nil, ErrRecurse
		}

		logDebug(impl.config.Logger(), fmt.Sprintf("blocksize too large, splitting at %d", half))
		obj1, err1 := impl.getByKeys(keys[0:half])
		obj2, err2 := impl.getByKeys(keys[half:])
		obj1 = append(obj1, obj2...)
		if err1 != nil {
			return obj1, err1
		} else {
			return obj1, err2
		}

	}
	return impl.store.GetObjectsByKey(keys, FROMCACHE)
}

func (impl easyStoreReadonlyImpl) populateObject(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// first get the fields (if required)
	if (which & Fields) == Fields {
		logDebug(impl.config.Logger(), fmt.Sprintf("getting fields for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		fields, err := impl.store.GetFieldsByKey(DataStoreKey{obj.Namespace(), obj.Id()}, FROMCACHE)
		if err == nil {
			obj.SetFields(*fields)
		} else {
			// known error
			if errors.Is(err, ErrNotFound) {
				logInfo(impl.config.Logger(), fmt.Sprintf("no fields found for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
			} else {
				return nil, err
			}
		}
	}

	// then, the blobs (if required)
	if (which & Files) == Files {
		logDebug(impl.config.Logger(), fmt.Sprintf("getting blobs for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		blobs, err := impl.store.GetBlobsByKey(DataStoreKey{obj.Namespace(), obj.Id()}, FROMCACHE)
		if err == nil {
			obj.SetFiles(blobs)
		} else {
			// known error
			if errors.Is(err, ErrNotFound) {
				logInfo(impl.config.Logger(), fmt.Sprintf("no blobs found for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
			} else {
				return nil, err
			}
		}
	}

	// lastly the opaque metadata (if required)
	if (which & Metadata) == Metadata {
		logDebug(impl.config.Logger(), fmt.Sprintf("getting metadata for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
		md, err := impl.store.GetMetadataByKey(DataStoreKey{obj.Namespace(), obj.Id()}, FROMCACHE)
		if err == nil {
			obj.SetMetadata(md)
		} else {
			// known error
			if errors.Is(err, ErrNotFound) {
				logInfo(impl.config.Logger(), fmt.Sprintf("no metadata found for ns/oid [%s/%s]", obj.Namespace(), obj.Id()))
			} else {
				return nil, err
			}
		}
	}

	return obj, nil
}

//
// end of file
//
