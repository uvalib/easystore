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

// this is our easystore serializer implementation
type easyStoreSerializerImpl struct {
}

func (impl easyStoreSerializerImpl) ObjectSerialize(o EasyStoreObject) interface{} {

	template := "{\"ns\":\"%s\",\"id\":\"%s\",\"vtag\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		o.Namespace(),
		o.Id(),
		o.VTag(),
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

	o := newEasyStoreObject(omap["ns"].(string), omap["id"].(string))
	obj := o.(*easyStoreObjectImpl)
	obj.Vtag_ = newVtag() // vtags must be unique so mint a new one here
	obj.Created_, obj.Modified_, err = timestampExtract(omap)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (impl easyStoreSerializerImpl) FieldsSerialize(f EasyStoreObjectFields) interface{} {
	nvTemplate := "{\"%s\":%s}"
	arrTemplate := "[%s]"
	fields := ""
	for n, v := range f {
		if len(fields) != 0 {
			fields += ","
		}
		b, _ := json.Marshal(v)
		fields += fmt.Sprintf(nvTemplate, n, string(b))
	}
	str := fmt.Sprintf(arrTemplate, fields)
	return []byte(str)
}

func (impl easyStoreSerializerImpl) FieldsDeserialize(i interface{}) (EasyStoreObjectFields, error) {

	// assume we are being passed a []byte
	//s, ok := i.([]byte)
	//if ok != true {
	//	return nil, fmt.Errorf("%q: %w", "cast error deserializing, interface probably not a []byte", ErrDeserialize)
	//}

	// unquote it cos the fields may have been quoted when serialized
	//str, _ := strconv.Unquote(string(s))

	// convert to an array of maps
	omap, err := interfaceToArrayMap(i)
	if err != nil {
		return nil, err
	}

	f := DefaultEasyStoreFields()
	for _, nv := range omap {
		for n, v := range nv {
			f[n], _ = v.(string)
		}
	}

	return f, nil
}

func (impl easyStoreSerializerImpl) BlobSerialize(b EasyStoreBlob) interface{} {

	// assume no error here
	buf, _ := b.Payload()
	enc := base64.StdEncoding.EncodeToString(buf)

	template := "{\"name\":\"%s\",\"mimetype\":\"%s\",\"payload\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		b.Name(),
		b.MimeType(),
		enc,
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

	str := omap["payload"].(string)
	buf, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, err
	}

	b := newEasyStoreBlob(
		omap["name"].(string),
		omap["mimetype"].(string),
		buf)

	blob := b.(*easyStoreBlobImpl)
	blob.Created_, blob.Modified_, err = timestampExtract(omap)
	if err != nil {
		return nil, err
	}

	return blob, nil
}

func (impl easyStoreSerializerImpl) MetadataSerialize(o EasyStoreMetadata) interface{} {

	// assume no error here
	buf, _ := o.Payload()
	enc := base64.StdEncoding.EncodeToString(buf)

	template := "{\"mimetype\":\"%s\",\"payload\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		o.MimeType(),
		enc,
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

	str := omap["payload"].(string)
	buf, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, err
	}

	md := newEasyStoreMetadata(omap["mimetype"].(string), buf)
	meta := md.(*easyStoreMetadataImpl)
	meta.Created_, meta.Modified_, err = timestampExtract(omap)
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
		return nil, fmt.Errorf("%q: %w", "cast error deserializing, interface probably not a []byte", ErrDeserialize)
	}

	// deserialize to a map
	var objmap map[string]interface{}
	if err := json.Unmarshal([]byte(s), &objmap); err != nil {
		return nil, fmt.Errorf("%q: %w", err.Error(), ErrDeserialize)
	}

	return objmap, nil
}

func interfaceToArrayMap(i interface{}) ([]map[string]interface{}, error) {

	// assume we are being passed a []byte
	s, ok := i.([]byte)
	if ok != true {
		return nil, fmt.Errorf("%q: %w", "cast error deserializing, interface probably not a []byte", ErrDeserialize)
	}

	// deserialize to a map
	var objmap []map[string]interface{}
	if err := json.Unmarshal(s, &objmap); err != nil {
		return nil, fmt.Errorf("%q: %w", err.Error(), ErrDeserialize)
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
