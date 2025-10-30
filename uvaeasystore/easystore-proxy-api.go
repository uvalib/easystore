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

// we need a custom unmarshaler because the implementation specifies some fields as interfaces
// so the default unmarshaler will not know how to unmarshal them
// see: https://mariadesouza.com/2017/09/07/custom-unmarshal-json-in-golang/
func (obj *easyStoreObjectImpl) UnmarshalJSON(data []byte) error {

	// unmarshal into a simple map of raw json
	var objmap map[string]*json.RawMessage
	err := json.Unmarshal(data, &objmap)
	if err != nil {
		return err
	}

	// then unmarshal each field we are interested in

	// namespace (we always have this)
	err = json.Unmarshal(*objmap["namespace"], &obj.Namespace_)
	if err != nil {
		return err
	}

	// id (we always have this)
	err = json.Unmarshal(*objmap["id"], &obj.Id_)
	if err != nil {
		return err
	}

	// vtag (we always have this)
	err = json.Unmarshal(*objmap["vtag"], &obj.Vtag_)
	if err != nil {
		return err
	}

	// created (we always have this)
	err = json.Unmarshal(*objmap["created"], &obj.Created_)
	if err != nil {
		return err
	}

	// modified (we always have this)
	err = json.Unmarshal(*objmap["modified"], &obj.Modified_)
	if err != nil {
		return err
	}

	// fields (optional)
	if val, ok := objmap["fields"]; ok {
		err = json.Unmarshal(*val, &obj.Fields_)
		if err != nil {
			return err
		}
	}

	// metadata (optional)
	if val, ok := objmap["metadata"]; ok {
		var md easyStoreMetadataImpl
		err = json.Unmarshal(*val, &md)
		if err != nil {
			return err
		}
		obj.SetMetadata(&md)
	}

	// files (optional)
	if val, ok := objmap["files"]; ok {
		var flist []easyStoreBlobImpl
		err = json.Unmarshal(*val, &flist)
		if err != nil {
			return err
		}
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
