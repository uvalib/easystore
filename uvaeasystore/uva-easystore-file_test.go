//
//
//

package uvaeasystore

import (
	"testing"
)

func TestFileCreate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.ObjectCreate(o)
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
	//blobs := []EasyStoreBlob{f1, f2}

	// add the files via the file API
	err = es.FileCreate(o.Namespace(), o.Id(), f1)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	err = es.FileCreate(o.Namespace(), o.Id(), f2)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.GetByKey(goodNamespace, o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test we got back what we expect
	if after.Files() == nil {
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
	testEqual(t, f1.MimeType(), b1.MimeType())
	testEqual(t, f2.MimeType(), b2.MimeType())
	buf1, _ := b1.Payload()
	buf2, _ := b2.Payload()
	url1 := b1.Url()
	url2 := b2.Url()

	if (buf1 == nil || len(buf1) == 0) && len(url1) == 0 {
		t.Fatalf("file payload AND url are empty\n")
	}
	if (buf2 == nil || len(buf2) == 0) && len(url2) == 0 {
		t.Fatalf("file payload AND url are empty\n")
	}
}

//
// end of file
//
