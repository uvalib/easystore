//
//
//

package uvaeasystore

import (
	"errors"
	"testing"
)

func TestUpdateBadVTagFiles(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object with no files
	before, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// update the object
	after, err := es.Update(before, Files)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// check the vtags are updated
	if before.VTag() == after.VTag() {
		t.Fatalf("object vtags are equal but should not be\n")
	}

	// update the object using the old object
	expected := ErrStaleObject
	_, err = es.Update(before, Files)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestUpdateBadVTagMetadata(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	before, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some metadata
	mimeType := "application/json"
	metadata := newEasyStoreMetadata(mimeType, jsonPayload)
	o.SetMetadata(metadata)

	// update the object
	after, err := es.Update(before, Metadata)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if before.VTag() == after.VTag() {
		t.Fatalf("object vtags are equal but should not be\n")
	}

	// update the object using the old object
	expected := ErrStaleObject
	_, err = es.Update(before, Metadata)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

//
// end of file
//
