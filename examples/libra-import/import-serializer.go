//
//
//

package main

import (
	//"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"time"
)

type libraOpenSerializer struct {
}
type libraEtdSerializer struct {
}

//
// Libra Open content deserializer
//

func (impl libraOpenSerializer) BlobDeserialize(i interface{}) (uvaeasystore.EasyStoreBlob, error) {

	blob := uvaeasystore.NewEasyStoreBlob("the name", "application/json", []byte("bla bla bla"))
	return blob, nil
}

func (impl libraOpenSerializer) FieldsDeserialize(i interface{}) (uvaeasystore.EasyStoreObjectFields, error) {

	fields := uvaeasystore.DefaultEasyStoreFields()
	return fields, nil
}

func (impl libraOpenSerializer) MetadataDeserialize(i interface{}) (uvaeasystore.EasyStoreMetadata, error) {
	metadata := libraMetadata{
		mimeType: "application/plain",
		payload:  []byte("blablabla"),
	}
	return metadata, nil
}

func (impl libraOpenSerializer) ObjectDeserialize(i interface{}) (uvaeasystore.EasyStoreObject, error) {

	// convert to a map
	omap, err := interfaceToMap(i)
	if err != nil {
		return nil, err
	}

	o := uvaeasystore.NewEasyStoreObject(omap["id"].(string))
	return o, nil
}

//
// Libra ETD content deserializer
//

func (impl libraEtdSerializer) BlobDeserialize(i interface{}) (uvaeasystore.EasyStoreBlob, error) {
	return nil, uvaeasystore.ErrNotImplemented
}

func (impl libraEtdSerializer) FieldsDeserialize(i interface{}) (uvaeasystore.EasyStoreObjectFields, error) {
	return nil, uvaeasystore.ErrNotImplemented
}

func (impl libraEtdSerializer) MetadataDeserialize(i interface{}) (uvaeasystore.EasyStoreMetadata, error) {
	return nil, uvaeasystore.ErrNotImplemented
}

func (impl libraEtdSerializer) ObjectDeserialize(i interface{}) (uvaeasystore.EasyStoreObject, error) {
	return nil, uvaeasystore.ErrNotImplemented
}

// custom metadata container
type libraMetadata struct {
	mimeType string    // mime type (if we know it)
	payload  []byte    // not exposed
	created  time.Time // created time
	modified time.Time // last modified time
}

func (impl libraMetadata) MimeType() string {
	return impl.mimeType
}

func (impl libraMetadata) Payload() []byte {
	return impl.payload
}

func (impl libraMetadata) PayloadNative() []byte {
	return impl.payload
}

func (impl libraMetadata) Created() time.Time {
	return impl.created
}

func (impl libraMetadata) Modified() time.Time {
	return impl.modified
}

//
// NOT REQUIRED
//

func (impl libraOpenSerializer) BlobSerialize(b uvaeasystore.EasyStoreBlob) interface{} {
	return nil
}

func (impl libraOpenSerializer) FieldsSerialize(f uvaeasystore.EasyStoreObjectFields) interface{} {
	return nil
}

func (impl libraOpenSerializer) MetadataSerialize(o uvaeasystore.EasyStoreMetadata) interface{} {
	return nil
}

func (impl libraOpenSerializer) ObjectSerialize(o uvaeasystore.EasyStoreObject) interface{} {
	return nil
}

func (impl libraEtdSerializer) BlobSerialize(b uvaeasystore.EasyStoreBlob) interface{} {
	return nil
}

func (impl libraEtdSerializer) FieldsSerialize(f uvaeasystore.EasyStoreObjectFields) interface{} {
	return nil
}

func (impl libraEtdSerializer) MetadataSerialize(o uvaeasystore.EasyStoreMetadata) interface{} {
	return nil
}

func (impl libraEtdSerializer) ObjectSerialize(o uvaeasystore.EasyStoreObject) interface{} {
	return nil
}

//
// private methods
//

func interfaceToMap(i interface{}) (map[string]interface{}, error) {

	// assume we are being passed a []byte
	s, ok := i.([]byte)
	if ok != true {
		//fmt.Printf("cast error deserializing: %s", i)
		//return nil, ErrDeserialize
		return nil, fmt.Errorf("%q: %w", "cast error deserializing, interface probably not a []byte", uvaeasystore.ErrDeserialize)
	}

	// deserialize to a map
	var objmap map[string]interface{}
	if err := json.Unmarshal([]byte(s), &objmap); err != nil {
		//fmt.Printf("unmarshal error deserializing: %s", i)
		//return nil, ErrDeserialize
		return nil, fmt.Errorf("%q: %w", err.Error(), uvaeasystore.ErrDeserialize)
	}

	return objmap, nil
}

func interfaceToArrayMap(i interface{}) ([]map[string]interface{}, error) {

	// assume we are being passed a []byte
	s, ok := i.([]byte)
	if ok != true {
		//fmt.Printf("cast error deserializing: %s", i)
		//return nil, ErrDeserialize
		return nil, fmt.Errorf("%q: %w", "cast error deserializing, interface probably not a []byte", uvaeasystore.ErrDeserialize)
	}

	// deserialize to a map
	var objmap []map[string]interface{}
	if err := json.Unmarshal([]byte(s), &objmap); err != nil {
		//fmt.Printf("unmarshal error deserializing: %s", i)
		//return nil, ErrDeserialize
		return nil, fmt.Errorf("%q: %w", err.Error(), uvaeasystore.ErrDeserialize)
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

//
// end of file
//
