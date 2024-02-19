//
//
//

package uvaeasystore

import (
	"testing"
)

func TestObjectSerialize(t *testing.T) {
	o := newTestObject("")
	s := newEasyStoreSerializer()

	i := s.ObjectSerialize(o)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestBlobSerialize(t *testing.T) {
	b := newEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	s := newEasyStoreSerializer()

	i := s.BlobSerialize(b)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestFieldsSerialize(t *testing.T) {
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	s := newEasyStoreSerializer()

	i := s.FieldsSerialize(fields)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestMetadataSerialize(t *testing.T) {

	// add some metadata
	mimeType := "application/json"
	payload := "{\"id\":12345}"
	m := newEasyStoreMetadata(mimeType, []byte(payload))
	s := newEasyStoreSerializer()

	i := s.MetadataSerialize(m)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

//
// end of file
//
