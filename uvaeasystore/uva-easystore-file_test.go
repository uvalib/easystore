//
//
//

package uvaeasystore

import (
	"bytes"
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
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")

	// add the files via the file API
	err = es.FileCreate(o.Namespace(), o.Id(), f1)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	err = es.FileCreate(o.Namespace(), o.Id(), f2)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// get the current object
	after, err := es.GetByKey(o.Namespace(), o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test we got back what we expect
	if after.Files() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	files := after.Files()
	if len(files) != 2 {
		t.Fatalf("expected 2 but got %d\n", len(files))
	}

	b1 := files[0]
	b2 := files[1]
	testEqual(t, f1.Name(), b1.Name())
	testEqual(t, f2.Name(), b2.Name())
	testEqual(t, f1.MimeType(), b1.MimeType())
	testEqual(t, f2.MimeType(), b2.MimeType())
	url1 := b1.Url()
	url2 := b2.Url()

	if len(url1) == 0 {
		t.Fatalf("file 1 url is empty\n")
	}
	if len(url2) == 0 {
		t.Fatalf("file 2 url is empty\n")
	}

	// verify payloads are correct
	plBefore1, _ := f1.Payload()
	plBefore2, _ := f2.Payload()
	plAfter1, _ := getFileContents(url1)
	plAfter2, _ := getFileContents(url2)

	if !bytes.Equal(plBefore1, plAfter1) {
		t.Fatalf("file payloads are unequal but should be\n")
	}

	if !bytes.Equal(plBefore2, plAfter2) {
		t.Fatalf("file payloads are unequal but should be\n")
	}

	// check the vtags are updated
	if o.VTag() == after.VTag() {
		t.Fatalf("object vtags are equal but should not be\n")
	}
}

func TestFileDelete(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// create the new object
	_, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// delete the first file
	err = es.FileDelete(o.Namespace(), o.Id(), f1.Name())
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// get the current object
	after, err := es.GetByKey(o.Namespace(), o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test we got back what we expect
	if after.Files() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	files = after.Files()
	if len(files) != 1 {
		t.Fatalf("expected 1 but got %d\n", len(files))
	}

	file1 := files[0]
	testEqual(t, f2.Name(), file1.Name())
	testEqual(t, f2.MimeType(), file1.MimeType())
	url1 := file1.Url()

	if len(url1) == 0 {
		t.Fatalf("file 1 url is empty\n")
	}

	// verify payloads are correct
	plBefore2, _ := f2.Payload()
	plAfter1, _ := getFileContents(url1)

	if !bytes.Equal(plBefore2, plAfter1) {
		t.Fatalf("file payloads are unequal but should be\n")
	}

	// check the vtags are updated
	if o.VTag() == after.VTag() {
		t.Fatalf("object vtags are equal but should not be\n")
	}
}

func TestFileRename(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// create the new object
	_, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// rename the first file
	newName := "file99.bin"
	err = es.FileRename(o.Namespace(), o.Id(), f1.Name(), newName)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// get the current object
	after, err := es.GetByKey(o.Namespace(), o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test we got back what we expect
	if after.Files() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	files = after.Files()
	if len(files) != 2 {
		t.Fatalf("expected 1 but got %d\n", len(files))
	}

	//fmt.Printf("Name 0: [%s]\n", files[0].Name())
	//fmt.Printf("Name 1: [%s]\n", files[1].Name())
	if files[0].Name() == "file2.bin" {
		if files[1].Name() != newName {
			t.Fatalf("expected '%s' but got '%s'\n", newName, files[1].Name())
		}
	} else {
		if files[0].Name() == newName {
			if files[1].Name() != "file2.bin" {
				t.Fatalf("expected 'file2.bin' but got '%s'\n", files[0].Name())
			}
		} else {
			t.Fatalf("unexpected name, got '%s'\n", files[0].Name())
		}
	}

	// check the vtags are updated
	if o.VTag() == after.VTag() {
		t.Fatalf("object vtags are equal but should not be\n")
	}
}

func TestFileUpdate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// create the new object
	_, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// rewrite the existing content
	f3 := newBinaryBlob("file2.bin")

	err = es.FileUpdate(o.Namespace(), o.Id(), f3)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// get the current object
	after, err := es.GetByKey(o.Namespace(), o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test we got back what we expect
	if after.Files() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	files = after.Files()
	if len(files) != 2 {
		t.Fatalf("expected 1 but got %d\n", len(files))
	}

	b1 := files[0]
	b2 := files[1]
	testEqual(t, f1.Name(), b1.Name())
	testEqual(t, f2.Name(), b2.Name())
	testEqual(t, f1.MimeType(), b1.MimeType())
	testEqual(t, f2.MimeType(), b2.MimeType())
	url1 := b1.Url()
	url2 := b2.Url()

	if len(url1) == 0 {
		t.Fatalf("file 1 url is empty\n")
	}
	if len(url2) == 0 {
		t.Fatalf("file 2 url is empty\n")
	}

	// verify payloads are correct
	plBefore1, _ := f1.Payload()
	plBefore2, _ := f3.Payload()
	plAfter1, _ := getFileContents(url1)
	plAfter2, _ := getFileContents(url2)

	if !bytes.Equal(plBefore1, plAfter1) {
		t.Fatalf("file payloads are unequal but should be\n")
	}

	if !bytes.Equal(plBefore2, plAfter2) {
		t.Fatalf("file payloads are unequal but should be\n")
	}

	// check the vtags are updated
	if o.VTag() == after.VTag() {
		t.Fatalf("object vtags are equal but should not be\n")
	}
}

//
// end of file
//
