//
//
//

package uvaeasystore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
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

	namespace, err := mapString(omap, "ns")
	if err != nil {
		return nil, err
	}
	id, err := mapString(omap, "id")
	if err != nil {
		return nil, err
	}
	vtag, err := mapString(omap, "vtag")
	if err != nil {
		return nil, err
	}

	o := newEasyStoreObject(namespace, id)
	obj := o.(*easyStoreObjectImpl)
	//obj.Vtag_ = newVtag() // vtags must be unique so mint a new one here
	obj.Vtag_ = vtag
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

	template := "{\"name\":%s,\"mimetype\":\"%s\",\"payload\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	str := fmt.Sprintf(template,
		jsonEncode(b.Name()),
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

	str, err := mapString(omap, "payload")
	if err != nil {
		return nil, err
	}
	buf, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", err.Error(), ErrDeserialize)
	}

	name, err := mapString(omap, "name")
	if err != nil {
		return nil, err
	}
	mimeType, err := mapString(omap, "mimetype")
	if err != nil {
		return nil, err
	}

	b := newEasyStoreBlobFromBuffer(
		jsonUnencode(name),
		mimeType,
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

	str, err := mapString(omap, "payload")
	if err != nil {
		return nil, err
	}
	buf, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", err.Error(), ErrDeserialize)
	}

	mimeType, err := mapString(omap, "mimetype")
	if err != nil {
		return nil, err
	}

	md := newEasyStoreMetadata(mimeType, buf)
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

func jsonEncode(value string) string {
	//fmt.Printf("ENC: [%s]\n", value)
	v := strconv.Quote(value)
	//fmt.Printf("RES: [%s]\n", v)
	return strconv.Quote(v)
}

func jsonUnencode(value string) string {
	//fmt.Printf("DEC: [%s]\n", value)
	s, err := strconv.Unquote(value)
	if err != nil {
		return value
	}
	//fmt.Printf("RES: [%s]\n", s)
	return s
}

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

// mapString -- extract a string member from a deserialized map. A member that is absent,
// or that is present but is not a string, means the stored data is not what we wrote. The
// store holds assets we do not control the lifetime of so this must be an error and never
// a panic, otherwise reading one damaged asset takes down the caller
func mapString(omap map[string]interface{}, name string) (string, error) {

	val, ok := omap[name]
	if ok == false {
		return "", fmt.Errorf("%q: %w", fmt.Sprintf("missing member [%s] deserializing", name), ErrDeserialize)
	}

	str, ok := val.(string)
	if ok == false {
		return "", fmt.Errorf("%q: %w", fmt.Sprintf("member [%s] is not a string deserializing", name), ErrDeserialize)
	}

	return str, nil
}

func timestampExtract(omap map[string]interface{}) (time.Time, time.Time, error) {

	createdStr, err := mapString(omap, "created")
	if err != nil {
		return time.Now(), time.Now(), err
	}
	modifiedStr, err := mapString(omap, "modified")
	if err != nil {
		return time.Now(), time.Now(), err
	}

	created, err1 := time.Parse("2006-01-02 15:04:05 -0700 MST", createdStr)
	modified, err2 := time.Parse("2006-01-02 15:04:05 -0700 MST", modifiedStr)

	if err1 != nil {
		return time.Now(), time.Now(), fmt.Errorf("%q: %w", err1.Error(), ErrDeserialize)
	}

	if err2 != nil {
		return time.Now(), time.Now(), fmt.Errorf("%q: %w", err2.Error(), ErrDeserialize)
	}

	return created, modified, nil
}

func newEasyStoreSerializer() EasyStoreSerializer {
	return &easyStoreSerializerImpl{}
}

//
// end of file
//
