//
//
//

package uvaeasystore

import (
	"testing"
)

func TestUpdateFields(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object with no fields
	_, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	before, err := es.ObjectGetByKey(goodNamespace, o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some fields
	fields := DefaultEasyStoreFields()
	fields["field1"] = "value1"
	fields["field2"] = "value2"
	o.SetFields(fields)

	// update the object
	_, err = es.ObjectUpdate(o, Fields)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.ObjectGetByKey(goodNamespace, o.Id(), AllComponents)
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
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object with no files
	_, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	before, err := es.ObjectGetByKey(goodNamespace, o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// update the object
	_, err = es.ObjectUpdate(o, Files)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.ObjectGetByKey(goodNamespace, o.Id(), AllComponents)
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
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object with no fields
	_, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	before, err := es.ObjectGetByKey(goodNamespace, o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some metadata
	mimeType := "application/json"
	metadata := newEasyStoreMetadata(mimeType, jsonPayload)
	o.SetMetadata(metadata)

	// update the object
	_, err = es.ObjectUpdate(o, Metadata)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.ObjectGetByKey(goodNamespace, o.Id(), AllComponents)
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
