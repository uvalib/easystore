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

	// validate the namespace
	if len(namespace) == 0 {
		return ErrBadParameter
	}

	// validate fields here!!!

	// validate the component request
	if which > AllComponents {
		return ErrBadParameter
	}

	// preflight good
	return nil
}

func CreatePreflight(obj EasyStoreObject) error {

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

func UpdatePreflight(obj EasyStoreObject, which EasyStoreComponents) error {

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

func DeletePreflight(obj EasyStoreObject, which EasyStoreComponents) error {

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

func RenamePreflight(obj EasyStoreObject, name string, newName string) error {

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

	// ensure our inputs are good
	if len(name) == 0 {
		return ErrBadParameter
	}
	if len(newName) == 0 {
		return ErrBadParameter
	}

	// ensure we actually have files
	files := obj.Files()
	if files == nil {
		return ErrBadParameter
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
		return ErrBadParameter
	}

	// preflight good
	return nil
}

//
// end of file
//
