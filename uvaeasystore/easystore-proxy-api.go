package uvaeasystore

import (
	"encoding/json"
	"fmt"
	"strings"
)

type GetObjectsRequest struct {
	Ids []string `json:"ids"`
}

type RenameBlobRequest struct {
	CurrentName string `json:"current-name"`
	NewName     string `json:"new-name"`
}

type GetObjectsResponse struct {
	Results []easyStoreObjectImpl `json:"results"`
}

type SearchObjectsResponse struct {
	Results []easyStoreObjectImpl `json:"results"`
}

// look up a member of the raw map. A member that is absent, or that is present but
// explicitly null, is reported as missing. Note that a null decodes to a nil pointer
// so it must be treated the same as absent or dereferencing it panics
func rawMember(objmap map[string]*json.RawMessage, name string) (json.RawMessage, bool) {
	val, ok := objmap[name]
	if ok == false || val == nil {
		return nil, false
	}
	return *val, true
}

// unmarshal a member we must have, a missing one means the payload is not one of ours
func requiredMember(objmap map[string]*json.RawMessage, name string, target interface{}) error {
	raw, ok := rawMember(objmap, name)
	if ok == false {
		return fmt.Errorf("%q: %w", fmt.Sprintf("missing required member [%s]", name), ErrDeserialize)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("%q: %w", err, ErrDeserialize)
	}
	return nil
}

// unmarshal a member we may not have, an absent one is not an error
func optionalMember(objmap map[string]*json.RawMessage, name string, target interface{}) (bool, error) {
	raw, ok := rawMember(objmap, name)
	if ok == false {
		return false, nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return false, fmt.Errorf("%q: %w", err, ErrDeserialize)
	}
	return true, nil
}

// we need a custom unmarshaler because the implementation specifies some fields as interfaces
// so the default unmarshaler will not know how to unmarshal them
// see: https://mariadesouza.com/2017/09/07/custom-unmarshal-json-in-golang/
func (obj *easyStoreObjectImpl) UnmarshalJSON(data []byte) error {

	// unmarshal into a simple map of raw json
	var objmap map[string]*json.RawMessage
	err := json.Unmarshal(data, &objmap)
	if err != nil {
		return fmt.Errorf("%q: %w", err, ErrDeserialize)
	}

	// a json null deserializes to a nil map, which has no members at all
	if objmap == nil {
		return fmt.Errorf("%q: %w", "empty object payload", ErrDeserialize)
	}

	// then unmarshal each field we are interested in

	// namespace (we always have this)
	if err = requiredMember(objmap, "namespace", &obj.Namespace_); err != nil {
		return err
	}

	// id (we always have this)
	if err = requiredMember(objmap, "id", &obj.Id_); err != nil {
		return err
	}

	// vtag (we always have this)
	if err = requiredMember(objmap, "vtag", &obj.Vtag_); err != nil {
		return err
	}

	// created (we always have this)
	if err = requiredMember(objmap, "created", &obj.Created_); err != nil {
		return err
	}

	// modified (we always have this)
	if err = requiredMember(objmap, "modified", &obj.Modified_); err != nil {
		return err
	}

	// fields (optional)
	if _, err = optionalMember(objmap, "fields", &obj.Fields_); err != nil {
		return err
	}

	// metadata (optional)
	var md easyStoreMetadataImpl
	present, err := optionalMember(objmap, "metadata", &md)
	if err != nil {
		return err
	}
	if present == true {
		obj.SetMetadata(&md)
	}

	// files (optional)
	var flist []easyStoreBlobImpl
	present, err = optionalMember(objmap, "files", &flist)
	if err != nil {
		return err
	}
	if present == true {
		alist := make([]EasyStoreBlob, len(flist))
		for i, _ := range flist {
			alist[i] = &flist[i]
		}
		obj.SetFiles(alist)
	}

	return nil
}

// maps http reponse payload into an easystore error (if possible)
func mapResponseToError(strErr string) error {

	if strings.Contains(strErr, ErrNotImplemented.Error()) {
		return ErrNotImplemented
	}
	if strings.Contains(strErr, ErrBadParameter.Error()) {
		return ErrBadParameter
	}
	if strings.Contains(strErr, ErrFileNotFound.Error()) {
		return ErrFileNotFound
	}
	if strings.Contains(strErr, ErrNotFound.Error()) {
		return ErrNotFound
	}
	if strings.Contains(strErr, ErrStaleObject.Error()) {
		return ErrStaleObject
	}
	if strings.Contains(strErr, ErrAlreadyExists.Error()) {
		return ErrAlreadyExists
	}
	if strings.Contains(strErr, ErrSerialize.Error()) {
		return ErrSerialize
	}
	if strings.Contains(strErr, ErrDeserialize.Error()) {
		return ErrDeserialize
	}
	if strings.Contains(strErr, ErrBusNotConfigured.Error()) {
		return ErrBusNotConfigured
	}
	if strings.Contains(strErr, ErrRecurse.Error()) {
		return ErrRecurse
	}

	return fmt.Errorf("%s", strErr)
}

//
// end of file
//
