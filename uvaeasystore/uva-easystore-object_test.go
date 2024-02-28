//
//
//

package uvaeasystore

import (
	"fmt"
	"testing"
)

func TestObjectBlobsUpdate(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if o.Files() != nil {
		t.Fatalf("expected empty but got non-empty\n")
	}

	// add some files
	file1Name := "file1.txt"
	file1Contents := fmt.Sprintf("%s: bla bla bla", file1Name)
	file2Name := "file2.txt"
	file2Contents := fmt.Sprintf("%s: bla bla bla", file2Name)
	fileType := "text/plain;charset=UTF-8"
	f1 := NewEasyStoreBlob(file1Name, fileType, []byte(file1Contents))
	f2 := NewEasyStoreBlob(file2Name, fileType, []byte(file2Contents))
	blobs := []EasyStoreBlob{f1, f2}

	// update the object
	o.SetFiles(blobs)
	o, err = es.Update(o, Files)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test we got back what we expect
	if o.Files() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	files := o.Files()
	if len(files) != 2 {
		t.Fatalf("expected 2 but got %d\n", len(files))
	}

	b1 := files[0]
	b2 := files[1]
	testEqual(t, file1Name, b1.Name())
	testEqual(t, file2Name, b2.Name())
	testEqual(t, fileType, b1.MimeType())
	testEqual(t, fileType, b2.MimeType())
	testEqual(t, file1Contents, string(b1.Payload()))
	testEqual(t, file2Contents, string(b2.Payload()))
}

func TestObjectMetadataUpdate(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if o.Metadata() != nil {
		t.Fatalf("expected empty but got non-empty\n")
	}

	// add some metadata
	mimeType := "application/json"
	m := newEasyStoreMetadata(mimeType, jsonPayload)
	o.SetMetadata(m)

	// update the object
	o, err = es.Update(o, Metadata)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if o.Metadata() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	testEqual(t, mimeType, o.Metadata().MimeType())
	testEqual(t, string(jsonPayload), string(o.Metadata().Payload()))
}

//
// end of file
//
