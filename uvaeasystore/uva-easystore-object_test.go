//
//
//

package uvaeasystore

import (
	"bytes"
	"testing"
)

func TestObjectBlobsUpdate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
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
	file1Name := "file1.bin"
	file2Name := "file2.bin"
	f1 := newBinaryBlob(file1Name)
	f2 := newBinaryBlob(file2Name)
	blobs := []EasyStoreBlob{f1, f2}

	file1Contents, _ := f1.Payload()
	file1Type := f1.MimeType()
	file2Contents, _ := f2.Payload()
	file2Type := f2.MimeType()

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
	testEqual(t, file1Type, b1.MimeType())
	testEqual(t, file2Type, b2.MimeType())
	buf1, _ := b1.Payload()
	buf2, _ := b2.Payload()

	if bytes.Equal(file1Contents, buf1) == false {
		t.Fatalf("byte slices are not equal\n")
	}
	if bytes.Equal(file2Contents, buf2) == false {
		t.Fatalf("byte slices are not equal\n")
	}
}

func TestObjectMetadataUpdate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
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
	buf, _ := o.Metadata().Payload()
	testEqual(t, string(jsonPayload), string(buf))
}

//
// end of file
//
