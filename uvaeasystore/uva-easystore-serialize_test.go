//
//
//

package uvaeasystore

import (
	"bytes"
	"testing"
)

func TestObjectSerialize(t *testing.T) {
	obj := NewEasyStoreObject(goodNamespace, "")
	serializer := DefaultEasyStoreSerializer()

	// serialize and cast appropriately
	i := serializer.ObjectSerialize(obj)
	b := i.([]byte)

	// convert to string and test
	str := string(b)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestObjectDeserialize(t *testing.T) {
	obj := NewEasyStoreObject(goodNamespace, "")
	serializer := DefaultEasyStoreSerializer()

	// serialize and deserialize
	i := serializer.ObjectSerialize(obj)
	c, err := serializer.ObjectDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := serializer.ObjectSerialize(c)

	// cast as necessary
	b := i.([]byte)
	bCopy := ci.([]byte)

	// convert to string and test
	if string(b) != string(bCopy) {
		t.Fatalf("serialized copy not equal\n")
	}
}

func TestBlobSerialize(t *testing.T) {
	blob := newBinaryBlob("file1.bin")
	serializer := DefaultEasyStoreSerializer()

	// serialize and cast appropriately
	i := serializer.BlobSerialize(blob)
	b := i.([]byte)

	// convert to string and test
	str := string(b)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestBlobDeserialize(t *testing.T) {
	blob := newBinaryBlob("file1.bin")
	serializer := DefaultEasyStoreSerializer()

	// serialize and deserialize
	i := serializer.BlobSerialize(blob)
	c, err := serializer.BlobDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := serializer.BlobSerialize(c)

	// cast as necessary
	b := i.([]byte)
	bCopy := ci.([]byte)

	// convert to string and test
	if bytes.Equal(b, bCopy) == false {
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
	b := i.([]byte)

	// convert to string and test
	str := string(b)
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

	// verify they are the same (cant use the string compare model above cos gloang
	// maps are not ordered!)
	if len(fields) != len(c) {
		t.Fatalf("expected %d but got %d\n", len(fields), len(c))
	}
	if fields["field1"] != c["field1"] {
		t.Fatalf("expected '%s' but got '%s'\n", fields["field1"], c["field1"])
	}
	if fields["field2"] != c["field2"] {
		t.Fatalf("expected '%s' but got '%s'\n", fields["field2"], c["field2"])
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
	b := i.([]byte)

	// convert to string and test
	str := string(b)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestMetadataDeserialize(t *testing.T) {

	// add some metadata
	mimeType := "application/json"
	payload := []byte("{\"id\":\"xyz\"}")
	metadata := newEasyStoreMetadata(mimeType, payload)
	serializer := DefaultEasyStoreSerializer()

	// serialize and deserialize
	i := serializer.MetadataSerialize(metadata)
	c, err := serializer.MetadataDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := serializer.MetadataSerialize(c)

	// cast as necessary
	b := i.([]byte)
	bCopy := ci.([]byte)

	// convert to string and test
	if string(b) != string(bCopy) {
		t.Fatalf("serialized copy not equal\n")
	}
}

//
// end of file
//
