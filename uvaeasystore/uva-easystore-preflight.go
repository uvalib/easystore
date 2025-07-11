//
//
//

package uvaeasystore

func GetByKeyPreflight(namespace string, id string, which EasyStoreComponents) error {

	// validate the namespace
	if len(namespace) == 0 {
		return ErrBadParameter
	}

	// validate the id
	if len(id) == 0 {
		return ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return ErrBadParameter
	}

	return nil
}

func GetByKeysPreflight(namespace string, ids []string, which EasyStoreComponents) error {

	// validate the namespace
	if len(namespace) == 0 {
		return ErrBadParameter
	}

	// validate the id list
	if len(ids) == 0 {
		return ErrBadParameter
	}

	// validate each member
	for _, id := range ids {
		if len(id) == 0 {
			return ErrBadParameter
		}
	}

	// validate the component request
	if which > AllComponents {
		return ErrBadParameter
	}

	// preflight good
	return nil
}

func GetByFieldsPreflight(namespace string, fields EasyStoreObjectFields, which EasyStoreComponents) error {

	// namespace can be blank for this query
	// validate the namespace
	//if len(namespace) == 0 {
	//	return ErrBadParameter
	//}

	// validate fields here!!!
	for k, v := range fields {
		if len(k) == 0 || len(v) == 0 {
			return ErrBadParameter
		}
	}

	// validate the component request
	if which > AllComponents {
		return ErrBadParameter
	}

	// preflight good
	return nil
}

func ObjectCreatePreflight(obj EasyStoreObject) error {

	// validate the object
	if obj == nil {
		return ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return ErrBadParameter
	}

	// preflight good
	return nil
}

func ObjectUpdatePreflight(obj EasyStoreObject, which EasyStoreComponents) error {

	// validate the object
	if obj == nil {
		return ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return ErrBadParameter
	}

	// preflight good
	return nil
}

func ObjectDeletePreflight(obj EasyStoreObject, which EasyStoreComponents) error {

	// validate the object
	if obj == nil {
		return ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return ErrBadParameter
	}

	// preflight good
	return nil
}

func FileCreatePreflight(namespace string, oid string, file EasyStoreBlob) error {

	// preflight good
	return nil
}

func FileDeletePreflight(namespace string, oid string, name string) error {

	// preflight good
	return nil
}

func FileRenamePreflight(namespace string, oid string, name string, new string) error {

	// preflight good
	return nil
}

func FileUpdatePreflight(namespace string, oid string, file EasyStoreBlob) error {

	// preflight good
	return nil
}

func RenamePreflight(obj EasyStoreObject, which EasyStoreComponents, curName string, newName string) error {

	// validate the object
	if obj == nil {
		return ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return ErrBadParameter
	}

	// ensure our inputs are good
	if len(curName) == 0 {
		return ErrBadParameter
	}
	if len(newName) == 0 {
		return ErrBadParameter
	}
	if curName == newName {
		return ErrBadParameter
	}

	// preflight good
	return nil
}

//
// end of file
//
