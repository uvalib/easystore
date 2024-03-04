//
//
//

package uvaeasystore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// this is our easystore blob implementation
type easyStoreSerializerImpl struct {
	namespace string // the object namespace
}

func (impl easyStoreSerializerImpl) ObjectSerialize(o EasyStoreObject) interface{} {

	template := "{\"id\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		o.Id(),
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

	o := newEasyStoreObject(impl.namespace, omap["id"].(string))
	obj := o.(*easyStoreObjectImpl)
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

	buf, _ := b.Payload()
	template := "{\"id\":\"%s\",\"vtag\":\"%s\",\"name\":\"%s\",\"mime-type\":\"%s\",\"payload\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		b.Id(),
		b.VTag(),
		b.Name(),
		b.MimeType(),
		buf, // might need to json escape here?
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

	blob := b.(*easyStoreBlobImpl)
	blob.id = omap["id"].(string)
	blob.vtag = omap["vtag"].(string)
	blob.created, blob.modified, err = timestampExtract(omap)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (impl easyStoreSerializerImpl) MetadataSerialize(o EasyStoreMetadata) interface{} {

	buf, _ := o.Payload()
	template := "{\"id\":\"%s\",\"vtag\":\"%s\",\"mime-type\":\"%s\",\"payload\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		o.Id(),
		o.VTag(),
		o.MimeType(),
		jsonEscape(buf),
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

	md := newEasyStoreMetadata(omap["mime-type"].(string), []byte(omap["payload"].(string)))
	meta := md.(*easyStoreMetadataImpl)
	meta.id = omap["id"].(string)
	meta.vtag = omap["vtag"].(string)
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
	if err := json.Unmarshal([]byte(s), &objmap); err != nil {
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

func jsonEscape(buf []byte) []byte {

	b := string(buf)
	str := strings.Replace(b, "\"", "\\\"", -1)
	return []byte(str)
}

func newEasyStoreSerializer(namespace string) EasyStoreSerializer {
	return &easyStoreSerializerImpl{namespace: namespace}
}

//
// end of file
//
