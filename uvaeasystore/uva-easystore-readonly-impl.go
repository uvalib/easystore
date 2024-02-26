//
//
//

package uvaeasystore

import (
	"errors"
	"fmt"
)

// this is our easystore readonly implementation
type easyStoreReadonlyImpl struct {
	config EasyStoreConfig // configuration info
	store  DataStore       // storage/persistence implementation
}

// factory for our easystore interface
func newEasyStoreReadonly(config EasyStoreConfig) (EasyStoreReadonly, error) {

	// create the data store for this Namespace
	s, err := NewDatastore(config)
	if err != nil {
		return nil, err
	}

	logInfo(config.Logger(), fmt.Sprintf("new readonly easystore"))
	return easyStoreReadonlyImpl{config: config, store: s}, nil
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

	// get the base object
	o, err := impl.getById(id)
	if err != nil {
		return nil, err
	}

	// poopulate the object and return it
	return impl.populateObject(o, which)
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

	// our results set
	objs := make([]EasyStoreObject, 0)

	// build our list of objects
	for _, id := range ids {
		// get the base object
		o, err := impl.getById(id)
		if err == nil {
			objs = append(objs, o)
		} else {
			if errors.Is(err, ErrNotFound) {
				// do nothing, this is OK
			} else {
				return nil, err
			}
		}
	}

	// bail out if we did not find any
	if len(objs) == 0 {
		return nil, ErrNotFound
	}

	// fully populate the objects
	//for ix, o := range objs {
	//	var err error
	//	objs[ix], err = impl.populateObject(o, which)
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	// we get objects only when they are required
	return newEasyStoreObjectSet(impl, objs, which), nil
}

func (impl easyStoreReadonlyImpl) GetByFields(fields EasyStoreObjectFields, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	logDebug(impl.config.Logger(), fmt.Sprintf("getting by fields"))

	// first get the base objects (always required)
	ids, err := impl.store.GetIdsByFields(fields)
	if err != nil {
		// known error
		if errors.Is(err, ErrNotFound) {
			logInfo(impl.config.Logger(), fmt.Sprintf("no objects found"))
		} else {
			return nil, err
		}
	}

	// bail out if we did not find any
	if len(ids) == 0 {
		return nil, ErrNotFound
	}

	// our results set
	objs := make([]EasyStoreObject, 0)

	// build our list of objects
	for _, id := range ids {
		// get the base object
		o, err := impl.getById(id)
		if err == nil {
			objs = append(objs, o)
		} else {
			return nil, err
		}
	}

	// bail out if we did not find any
	if len(objs) == 0 {
		return nil, ErrNotFound
	}

	// fully populate the objects
	//for ix, o := range objs {
	//	var err error
	//	objs[ix], err = impl.populateObject(o, which)
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	// we get objects only when they are required
	return newEasyStoreObjectSet(impl, objs, which), nil
}

//
// private methods
//

func (impl easyStoreReadonlyImpl) getById(id string) (EasyStoreObject, error) {

	logDebug(impl.config.Logger(), fmt.Sprintf("getting oid [%s]", id))

	// get the base object (always required)
	o, err := impl.store.GetObjectByOid(id)
	if err != nil {
		// known error
		if errors.Is(err, ErrNotFound) {
			logInfo(impl.config.Logger(), fmt.Sprintf("no object found for oid [%s]", id))
			return nil, ErrNotFound
		} else {
			return nil, err
		}
	}
	return o, nil
}

func (impl easyStoreReadonlyImpl) populateObject(eso EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// first get the fields (if required)
	if (which & Fields) == Fields {
		logDebug(impl.config.Logger(), fmt.Sprintf("getting fields for oid [%s]", eso.Id()))
		fields, err := impl.store.GetFieldsByOid(eso.Id())
		if err == nil {
			eso.SetFields(*fields)
		} else {
			// known error
			if errors.Is(err, ErrNotFound) {
				logInfo(impl.config.Logger(), fmt.Sprintf("no fields found for oid [%s]", eso.Id()))
			} else {
				return nil, err
			}
		}
	}

	// then, the blobs (if required)
	if (which & Files) == Files {
		logDebug(impl.config.Logger(), fmt.Sprintf("getting blobs for oid [%s]", eso.Id()))
		blobs, err := impl.store.GetBlobsByOid(eso.Id())
		if err == nil {
			eso.SetFiles(blobs)
		} else {
			// known error
			if errors.Is(err, ErrNotFound) {
				logInfo(impl.config.Logger(), fmt.Sprintf("no blobs found for oid [%s]", eso.Id()))
			} else {
				return nil, err
			}
		}
	}

	// lastly the opaque metadata (if required)
	if (which & Metadata) == Metadata {
		logDebug(impl.config.Logger(), fmt.Sprintf("getting metadata for oid [%s]", eso.Id()))
		md, err := impl.store.GetMetadataByOid(eso.Id())
		if err == nil {
			eso.SetMetadata(md)
		} else {
			// known error
			if errors.Is(err, ErrNotFound) {
				logInfo(impl.config.Logger(), fmt.Sprintf("no metadata found for oid [%s]", eso.Id()))
			} else {
				return nil, err
			}
		}
	}

	return eso, nil
}

//
// end of file
//
