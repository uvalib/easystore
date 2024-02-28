//
//
//

package uvaeasystore

import (
	"errors"
	"testing"
)

func TestObjectCreate(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

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
	o := NewEasyStoreObject(goodNamespace, "")

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
	o := NewEasyStoreObject(goodNamespace, "")

	// add some fields
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	o.SetFields(fields)

	// create the new object
	o, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, Fields)
	testEqual(t, "value1", o.Fields()["field1"])
	testEqual(t, "value2", o.Fields()["field2"])
}

func TestFilesCreate(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	f2 := NewEasyStoreBlob("file2.txt", "text/plain;charset=UTF-8", []byte("file2: bla bla bla"))
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// create the new object
	o, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, Files)
	if len(o.Files()) != 2 {
		t.Fatalf("expected '2' but got '%d'\n", len(o.Files()))
	}
	testEqual(t, "file1.txt", o.Files()[0].Name())
	testEqual(t, "file2.txt", o.Files()[1].Name())
}

func TestDuplicateFilesCreate(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	files := []EasyStoreBlob{f1, f1}
	o.SetFiles(files)

	// create the new object
	expected := ErrAlreadyExists
	o, err := es.Create(o)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestMetadataCreate(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// add some metadata
	mimeType := "application/json"
	metadata := newEasyStoreMetadata(mimeType, jsonPayload)
	o.SetMetadata(metadata)

	// create the new object
	o, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, Metadata)

	testEqual(t, mimeType, o.Metadata().MimeType())
	testEqual(t, string(jsonPayload), string(o.Metadata().Payload()))
}

//
// end of file
//
