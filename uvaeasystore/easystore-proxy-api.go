package uvaeasystore

import (
	"encoding/json"
)

type getObjectsRequest struct {
	Ids []string `json:"ids"`
}

type getObjectsResponse struct {
	Results []easyStoreObjectImpl `json:"results"`
}

type searchObjectsResponse struct {
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
		obj.SetMetadata(md)
	}

	// files (optional)
	if val, ok := objmap["files"]; ok {
		var flist []easyStoreBlobImpl
		err = json.Unmarshal(*val, &flist)
		if err != nil {
			return err
		}
		alist := make([]EasyStoreBlob, len(flist))
		for i, o := range flist {
			alist[i] = o
		}
		obj.SetFiles(alist)
	}

	return nil
}

//
// end of file
//
