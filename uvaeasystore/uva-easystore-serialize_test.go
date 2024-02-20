//
//
//

package uvaeasystore

import (
	"testing"
)

func TestObjectSerialize(t *testing.T) {
	obj := newTestObject("")
	serializer := DefaultEasyStoreSerializer()

	// serialize and cast appropriately
	i := serializer.ObjectSerialize(obj)
	bytes := i.([]byte)

	// convert to string and test
	str := string(bytes)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestObjectDeserialize(t *testing.T) {
	obj := newTestObject("")
	serializer := DefaultEasyStoreSerializer()

	// serialize and deserialize
	i := serializer.ObjectSerialize(obj)
	c, err := serializer.ObjectDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := serializer.ObjectSerialize(c)

	// cast as necessary
	bytes := i.([]byte)
	bytesCopy := ci.([]byte)

	// convert to string and test
	if string(bytes) != string(bytesCopy) {
		t.Fatalf("serialized copy not equal\n")
	}
}

func TestBlobSerialize(t *testing.T) {
	blob := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	serializer := DefaultEasyStoreSerializer()

	// serialize and cast appropriately
	i := serializer.BlobSerialize(blob)
	bytes := i.([]byte)

	// convert to string and test
	str := string(bytes)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestBlobDeserialize(t *testing.T) {
	blob := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	serializer := DefaultEasyStoreSerializer()

	// serialize and deserialize
	i := serializer.BlobSerialize(blob)
	c, err := serializer.BlobDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := serializer.BlobSerialize(c)

	// cast as necessary
	bytes := i.([]byte)
	bytesCopy := ci.([]byte)

	// convert to string and test
	if string(bytes) != string(bytesCopy) {
		t.Fatalf("serialized copy not equal\n")
	}
}

func TestFieldsSerialize(t *testing.T) {
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	serializer := DefaultEasyStoreSerializer()

	// serialize and cast appropriately
	i := serializer.FieldsSerialize(fields)
	bytes := i.([]byte)

	// convert to string and test
	str := string(bytes)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestFieldsDeserialize(t *testing.T) {
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	serializer := DefaultEasyStoreSerializer()

	// serialize and deserialize
	i := serializer.FieldsSerialize(fields)
	c, err := serializer.FieldsDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := serializer.FieldsSerialize(c)

	// cast as necessary
	bytes := i.([]byte)
	bytesCopy := ci.([]byte)

	// convert to string and test
	if string(bytes) != string(bytesCopy) {
		t.Fatalf("serialized copy not equal\n")
	}
}

func TestMetadataSerialize(t *testing.T) {

	// add some metadata
	mimeType := "application/json"
	payload := "{\"id\":12345}"
	metadata := newEasyStoreMetadata(mimeType, []byte(payload))
	serializer := DefaultEasyStoreSerializer()

	// serialize and cast appropriately
	i := serializer.MetadataSerialize(metadata)
	bytes := i.([]byte)

	// convert to string and test
	str := string(bytes)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestMetadataDeserialize(t *testing.T) {

	// add some metadata
	mimeType := "application/json"
	payload := "{\"id\":12345}"
	metadata := newEasyStoreMetadata(mimeType, []byte(payload))
	serializer := DefaultEasyStoreSerializer()

	// serialize and deserialize
	i := serializer.MetadataSerialize(metadata)
	c, err := serializer.MetadataDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := serializer.MetadataSerialize(c)

	// cast as necessary
	bytes := i.([]byte)
	bytesCopy := ci.([]byte)

	// convert to string and test
	if string(bytes) != string(bytesCopy) {
		t.Fatalf("serialized copy not equal\n")
	}
}

//
// end of file
//
