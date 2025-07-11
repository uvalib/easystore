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
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, BaseComponent)
}

func TestDuplicateObjectCreate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// try and create it again
	expected := ErrAlreadyExists
	o, err = es.ObjectCreate(o)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestObjectWithFieldsCreate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some fields
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	o.SetFields(fields)

	// create the new object
	o, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, o, Fields)
	testEqual(t, "value1", o.Fields()["field1"])
	testEqual(t, "value2", o.Fields()["field2"])
}

func TestObjectWithFilesCreate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// create the new object
	after, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, after, Files)
	if len(after.Files()) != 2 {
		t.Fatalf("expected '2' but got '%d'\n", len(after.Files()))
	}
	testEqual(t, "file1.bin", after.Files()[0].Name())
	testEqual(t, "file2.bin", after.Files()[1].Name())

	//fmt.Printf("SIGNED URL: %s\n", after.Files()[0].Url())
	//fmt.Printf("SIGNED URL: %s\n", after.Files()[1].Url())
}

func TestObjectWithDuplicateFilesCreate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := newBinaryBlob("file1.bin")
	files := []EasyStoreBlob{f1, f1}
	o.SetFiles(files)

	// create the new object
	expected := ErrAlreadyExists
	_, err := es.ObjectCreate(o)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestObjectWithMetadataCreate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some metadata
	mimeType := "application/json"
	metadata := newEasyStoreMetadata(mimeType, jsonPayload)
	o.SetMetadata(metadata)

	// create the new object
	after, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// validate the object we got in return
	validateObject(t, after, Metadata)

	testEqual(t, mimeType, after.Metadata().MimeType())
	buf, _ := after.Metadata().Payload()
	testEqual(t, string(jsonPayload), string(buf))
}

//
// end of file
//
