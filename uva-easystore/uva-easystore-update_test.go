//
//
//

package uva_easystore

import (
	"testing"
)

func TestUpdateFields(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// create the new object with no fields
	_, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	before, err := es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some fields
	obj.fields = DefaultEasyStoreFields()
	obj.fields["field1"] = "value1"
	obj.fields["field2"] = "value2"

	// update the object
	_, err = es.Update(obj, Fields)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if len(before.Fields()) != 0 {
		t.Fatalf("unexpected object fields\n")
	}

	if len(after.Fields()) != 2 {
		t.Fatalf("missing object fields\n")
	}
}

func TestUpdateFiles(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// create the new object with no files
	_, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	before, err := es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some files
	f1 := newEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	f2 := newEasyStoreBlob("file2.txt", "text/plain;charset=UTF-8", []byte("file2: bla bla bla"))
	obj.files = []EasyStoreBlob{f1, f2}

	// update the object
	_, err = es.Update(obj, Files)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if len(before.Files()) != 0 {
		t.Fatalf("unexpected object files\n")
	}

	if len(after.Files()) != 2 {
		t.Fatalf("missing object files\n")
	}
}

func TestUpdateMetadata(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// create the new object with no fields
	_, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	before, err := es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some metadata
	mimeType := "application/json"
	obj.metadata = newEasyStoreMetadata(mimeType, jsonPayload)

	// update the object
	_, err = es.Update(obj, Metadata)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if before.Metadata() != nil {
		t.Fatalf("unexpected object metadata\n")
	}

	if after.Metadata() == nil {
		t.Fatalf("missing object metadata\n")
	}
}

//
// end of file
//
