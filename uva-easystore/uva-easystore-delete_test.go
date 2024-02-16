//
//
//

package uva_easystore

import (
	"errors"
	"testing"
)

func TestSimpleDelete(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")

	// create the new object
	_, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	_, err = es.GetById(o.Id(), BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// now delete it
	_, err = es.Delete(o, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// now we cant
	_, err = es.GetById(o.Id(), BaseComponent)
	if errors.Is(err, ErrObjectNotFound) == false {
		if err != nil {
			t.Fatalf("expected '%s' but got '%s'\n", ErrObjectNotFound, err)
		}
	}
}

func TestFieldsDelete(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// add some fields
	obj.fields = DefaultEasyStoreFields()
	obj.fields["field1"] = "value1"
	obj.fields["field2"] = "value2"

	// create the new object
	_, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	_, err = es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// now delete it
	_, err = es.Delete(o, Fields)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	o, err = es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if len(o.Fields()) != 0 {
		t.Fatalf("unexpected object fields\n")
	}
}

func TestFilesDelete(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// add some files
	f1 := newEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	f2 := newEasyStoreBlob("file2.txt", "text/plain;charset=UTF-8", []byte("file2: bla bla bla"))
	obj.files = []EasyStoreBlob{f1, f2}

	// create the new object
	_, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	_, err = es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// now delete it
	_, err = es.Delete(o, Files)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	o, err = es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if len(o.Files()) != 0 {
		t.Fatalf("unexpected object files\n")
	}
}

func TestMetadataDelete(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")
	obj := o.(easyStoreObjectImpl)

	// add some metadata
	mimeType := "application/json"
	obj.metadata = newEasyStoreMetadata(mimeType, jsonPayload)

	// create the new object
	_, err := es.Create(obj)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	_, err = es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// now delete it
	_, err = es.Delete(o, Metadata)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	o, err = es.GetById(o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if o.Metadata() != nil {
		t.Fatalf("unexpected object metadata\n")
	}
}

//
// end of file
//
