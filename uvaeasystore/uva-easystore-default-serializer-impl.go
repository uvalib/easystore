//
//
//

package uvaeasystore

import "fmt"

// this is our easystore blob implementation
type easyStoreSerializerImpl struct {
}

func (impl easyStoreSerializerImpl) ObjectSerialize(o EasyStoreObject) interface{} {

	template := "{\"id\":\"%s\",\"accessid\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	return fmt.Sprintf(template,
		o.Id(),
		o.AccessId(),
		o.Created().UTC(),
		o.Modified().UTC(),
	)
}

func (impl easyStoreSerializerImpl) FieldsSerialize(f EasyStoreObjectFields) interface{} {
	nvTemplate := "{\"name\",\"%s\",\"value\",\"%s\"}"
	arrTemplate := "[%s]"
	fields := ""
	for n, v := range f {
		if len(fields) != 0 {
			fields += ","
		}
		fields += fmt.Sprintf(nvTemplate, n, v)
	}
	return fmt.Sprintf(arrTemplate, fields)
}

func (impl easyStoreSerializerImpl) BlobSerialize(b EasyStoreBlob) interface{} {

	template := "{\"name\":\"%s\",\"mime-type\":\"%s\",\"url\":\"%s\",\"created\":\"%s\",\"modified\":\"%s\"}"
	return fmt.Sprintf(template,
		b.Name(),
		b.MimeType(),
		b.Url(),
		b.Created().UTC(),
		b.Modified().UTC(),
	)

}

func newEasyStoreSerializer() EasyStoreSerializer {
	return &easyStoreSerializerImpl{}
}

//
// end of file
//
