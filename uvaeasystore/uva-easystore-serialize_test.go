//
//
//

package uvaeasystore

import (
	"testing"
)

func TestObjectSerialize(t *testing.T) {
	o := newTestObject("")
	s := DefaultEasyStoreSerializer()

	i := s.ObjectSerialize(o)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestObjectDeserialize(t *testing.T) {
	o := newTestObject("")
	s := DefaultEasyStoreSerializer()

	i := s.ObjectSerialize(o)
	c, err := s.ObjectDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := s.ObjectSerialize(c)
	if i.(string) != ci.(string) {
		t.Fatalf("serialized copy not equal\n")
	}
}

func TestBlobSerialize(t *testing.T) {
	b := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	s := DefaultEasyStoreSerializer()

	i := s.BlobSerialize(b)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestBlobDeserialize(t *testing.T) {
	b := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	s := DefaultEasyStoreSerializer()

	i := s.BlobSerialize(b)
	c, err := s.BlobDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := s.BlobSerialize(c)
	if i.(string) != ci.(string) {
		t.Fatalf("serialized copy not equal\n")
	}
}

func TestFieldsSerialize(t *testing.T) {
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	s := DefaultEasyStoreSerializer()

	i := s.FieldsSerialize(fields)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestFieldsDeserialize(t *testing.T) {
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	s := DefaultEasyStoreSerializer()

	i := s.FieldsSerialize(fields)
	c, err := s.FieldsDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := s.FieldsSerialize(c)
	if i.(string) != ci.(string) {
		t.Fatalf("serialized copy not equal\n")
	}
}

func TestMetadataSerialize(t *testing.T) {

	// add some metadata
	mimeType := "application/json"
	payload := "{\"id\":12345}"
	m := newEasyStoreMetadata(mimeType, []byte(payload))
	s := DefaultEasyStoreSerializer()

	i := s.MetadataSerialize(m)
	str := i.(string)
	if len(str) == 0 {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

func TestMetadataDeserialize(t *testing.T) {

	// add some metadata
	mimeType := "application/json"
	payload := "{\"id\":12345}"
	m := newEasyStoreMetadata(mimeType, []byte(payload))
	s := DefaultEasyStoreSerializer()

	i := s.MetadataSerialize(m)
	c, err := s.MetadataDeserialize(i)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	ci := s.MetadataSerialize(c)
	if i.(string) != ci.(string) {
		t.Fatalf("serialized copy not equal\n")
	}
}

//
// end of file
//
