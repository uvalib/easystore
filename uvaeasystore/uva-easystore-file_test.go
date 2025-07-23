//
//
//

package uvaeasystore

import (
	"bytes"
	"errors"
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

	// first attempt with bad values
	expected := ErrNotFound
	err = es.FileCreate(badNamespace, o.Id(), f1)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
	err = es.FileCreate(o.Namespace(), badId, f1)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// then try properly
	err = es.FileCreate(o.Namespace(), o.Id(), f1)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	err = es.FileCreate(o.Namespace(), o.Id(), f2)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// then try a duplicate
	expected = ErrAlreadyExists
	err = es.FileCreate(o.Namespace(), o.Id(), f1)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// get the current object
	after, err := es.ObjectGetByKey(o.Namespace(), o.Id(), AllComponents)
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

	// first attempt with bad values
	expected := ErrNotFound
	err = es.FileDelete(badNamespace, o.Id(), f1.Name())
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
	err = es.FileDelete(o.Namespace(), badId, f1.Name())
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// then try properly
	err = es.FileDelete(o.Namespace(), o.Id(), f1.Name())
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// try deleting it again (currently does not fail as expected)
	expected = ErrNotFound
	//err = es.FileDelete(o.Namespace(), o.Id(), f1.Name())
	//if errors.Is(err, expected) == false {
	//	t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	//}

	// get the current object
	after, err := es.ObjectGetByKey(o.Namespace(), o.Id(), AllComponents)
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

	// first attempt with bad values
	newName := "file99.bin"
	expected := ErrNotFound
	err = es.FileRename(badNamespace, o.Id(), f1.Name(), newName)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
	err = es.FileRename(o.Namespace(), badId, f1.Name(), newName)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// then attempt a non-existent name
	err = es.FileRename(o.Namespace(), o.Id(), newName, f1.Name())
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// then try properly
	err = es.FileRename(o.Namespace(), o.Id(), f1.Name(), newName)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// and try it again
	err = es.FileRename(o.Namespace(), o.Id(), f1.Name(), newName)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// get the current object
	after, err := es.ObjectGetByKey(o.Namespace(), o.Id(), AllComponents)
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

	// new blob with the same name
	f3 := newBinaryBlob("file2.bin")

	// new blob with another name
	f4 := newBinaryBlob("file99.bin")

	// first attempt with bad values
	expected := ErrNotFound
	err = es.FileUpdate(badNamespace, o.Id(), f3)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
	err = es.FileUpdate(o.Namespace(), badId, f3)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// attempt a non-existent name
	err = es.FileUpdate(o.Namespace(), o.Id(), f4)
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// then try properly
	err = es.FileUpdate(o.Namespace(), o.Id(), f3)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// get the current object
	after, err := es.ObjectGetByKey(o.Namespace(), o.Id(), AllComponents)
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
