//
//
//

package uvaeasystore

import (
	"bytes"
	"errors"
	"testing"
)

func TestObjectCreate(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")

	// create the new object
	o, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, BaseComponent)
}

func TestDuplicateObjectCreate(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")

	// create the new object
	o, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// try and create it again
	expected := ErrAlreadyExists
	o, err = es.Create(o)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestFieldsCreate(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// add some fields
	obj.fields = DefaultEasyStoreFields()
	obj.fields["field1"] = "value1"
	obj.fields["field2"] = "value2"

	// create the new object
	o, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, Fields)
	if obj.fields["field1"] != "value1" {
		t.Fatalf("expected 'value1' but got '%s'\n", obj.fields["field1"])
	}
	if obj.fields["field2"] != "value2" {
		t.Fatalf("expected 'value2' but got '%s'\n", obj.fields["field2"])
	}
}

func TestFilesCreate(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// add some files
	f1 := newEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	f2 := newEasyStoreBlob("file2.txt", "text/plain;charset=UTF-8", []byte("file2: bla bla bla"))
	obj.files = []EasyStoreBlob{f1, f2}

	// create the new object
	o, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, Files)
	if len(o.Files()) != 2 {
		t.Fatalf("expected '2' but got '%d'\n", len(o.Files()))
	}
	if o.Files()[0].Name() != "file1.txt" {
		t.Fatalf("expected 'file1.txt' but got '%s'\n", o.Files()[0].Name())
	}
	if o.Files()[1].Name() != "file2.txt" {
		t.Fatalf("expected 'file2.txt' but got '%s'\n", o.Files()[0].Name())
	}
}

func TestDuplicateFilesCreate(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// add some files
	f1 := newEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	obj.files = []EasyStoreBlob{f1, f1}

	// create the new object
	expected := ErrAlreadyExists
	o, err := es.Create(obj)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestMetadataCreate(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// add some metadata
	mimeType := "application/json"
	obj.metadata = newEasyStoreMetadata(mimeType, jsonPayload)

	// create the new object
	o, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, Metadata)

	if o.Metadata().MimeType() != mimeType {
		t.Fatalf("expected '%s' but got '%s'\n", mimeType, o.Metadata().MimeType())
	}

	if bytes.Equal(o.Metadata().Payload(), jsonPayload) == false {
		t.Fatalf("expected '%s' but got '%s'\n", jsonPayload, string(o.Metadata().Payload()))
	}
}

//
// end of file
//