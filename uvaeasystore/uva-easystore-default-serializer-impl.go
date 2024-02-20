//
//
//

package uvaeasystore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// this is our easystore blob implementation
type easyStoreSerializerImpl struct {
}

func (impl easyStoreSerializerImpl) ObjectSerialize(o EasyStoreObject) interface{} {

	template := "{\"id\":\"%s\",\"accessid\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		o.Id(),
		o.AccessId(),
		o.Created().UTC(),
		o.Modified().UTC(),
	)
	return []byte(str)
}

func (impl easyStoreSerializerImpl) ObjectDeserialize(i interface{}) (EasyStoreObject, error) {

	// convert to a map
	omap, err := interfaceToMap(i)
	if err != nil {
		return nil, err
	}

	o := newEasyStoreObject(omap["id"].(string))
	obj := o.(easyStoreObjectImpl)
	obj.accessId = omap["accessid"].(string)
	obj.created, obj.modified, err = timestampExtract(omap)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (impl easyStoreSerializerImpl) FieldsSerialize(f EasyStoreObjectFields) interface{} {
	nvTemplate := "{\"%s\":\"%s\"}"
	arrTemplate := "[%s]"
	fields := ""
	for n, v := range f {
		if len(fields) != 0 {
			fields += ","
		}
		fields += fmt.Sprintf(nvTemplate, n, v)
	}
	str := fmt.Sprintf(arrTemplate, fields)
	return []byte(str)
}

func (impl easyStoreSerializerImpl) FieldsDeserialize(i interface{}) (EasyStoreObjectFields, error) {

	// convert to an array of maps
	omap, err := interfaceToArrayMap(i)
	if err != nil {
		return nil, err
	}

	f := DefaultEasyStoreFields()
	for _, nv := range omap {
		for n, v := range nv {
			f[n] = v.(string)
		}
	}

	return f, nil
}

func (impl easyStoreSerializerImpl) BlobSerialize(b EasyStoreBlob) interface{} {

	template := "{\"name\":\"%s\",\"mime-type\":\"%s\",\"payload\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		b.Name(),
		b.MimeType(),
		b.Url(),
		b.Created().UTC(),
		b.Modified().UTC(),
	)
	return []byte(str)
}

func (impl easyStoreSerializerImpl) BlobDeserialize(i interface{}) (EasyStoreBlob, error) {

	// convert to a map
	omap, err := interfaceToMap(i)
	if err != nil {
		return nil, err
	}

	b := newEasyStoreBlob(
		omap["name"].(string),
		omap["mime-type"].(string),
		[]byte(omap["payload"].(string)))

	return b, nil
}

func (impl easyStoreSerializerImpl) MetadataSerialize(o EasyStoreMetadata) interface{} {

	template := "{\"mime-type\":\"%s\",\"payload\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		o.MimeType(),
		base64.StdEncoding.EncodeToString(o.Payload()),
		o.Created().UTC(),
		o.Modified().UTC(),
	)
	return []byte(str)
}

func (impl easyStoreSerializerImpl) MetadataDeserialize(i interface{}) (EasyStoreMetadata, error) {

	// convert to a map
	omap, err := interfaceToMap(i)
	if err != nil {
		return nil, err
	}

	payload, _ := base64.StdEncoding.DecodeString(omap["payload"].(string))
	md := newEasyStoreMetadata(omap["mime-type"].(string), payload)
	meta := md.(*easyStoreMetadataImpl)
	meta.created, meta.modified, err = timestampExtract(omap)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

//
// private methods
//

func interfaceToMap(i interface{}) (map[string]interface{}, error) {

	// assume we are being passed a []byte
	s, ok := i.([]byte)
	if ok != true {
		fmt.Printf("cast error deserializing: %s", i)
		return nil, ErrDeserialize
	}

	// deserialize to a map
	var objmap map[string]interface{}
	if err := json.Unmarshal([]byte(s), &objmap); err != nil {
		fmt.Printf("unmarshal error deserializing: %s", i)
		return nil, ErrDeserialize
	}

	return objmap, nil
}

func interfaceToArrayMap(i interface{}) ([]map[string]interface{}, error) {

	// assume we are being passed a []byte
	s, ok := i.([]byte)
	if ok != true {
		fmt.Printf("cast error deserializing: %s", i)
		return nil, ErrDeserialize
	}

	// deserialize to a map
	var objmap []map[string]interface{}
	if err := json.Unmarshal([]byte(s), &objmap); err != nil {
		fmt.Printf("unmarshal error deserializing: %s", i)
		return nil, ErrDeserialize
	}

	return objmap, nil
}

func timestampExtract(omap map[string]interface{}) (time.Time, time.Time, error) {

	created, err1 := time.Parse("2006-01-02 15:04:05 -0700 MST", omap["created"].(string))
	modified, err2 := time.Parse("2006-01-02 15:04:05 -0700 MST", omap["modified"].(string))

	if err1 != nil {
		return time.Now(), time.Now(), err1
	}

	if err2 != nil {
		return time.Now(), time.Now(), err2
	}

	return created, modified, nil
}

func newEasyStoreSerializer() EasyStoreSerializer {
	return &easyStoreSerializerImpl{}
}

//
// end of file
//
