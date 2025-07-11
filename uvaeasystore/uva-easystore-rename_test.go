//
//
//

package uvaeasystore

import (
	"errors"
	"testing"
)

func TestRenameFiles(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// add some files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	files := []EasyStoreBlob{f1, f2}
	o.SetFiles(files)

	// create it
	o, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if len(o.Files()) != 2 {
		t.Fatalf("missing object files\n")
	}

	// rename a non existent file
	expected := ErrNotFound
	_, err = es.Rename(o, Files, "file99.bin", "file100.bin")
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// rename a duplicate file
	expected = ErrAlreadyExists
	_, err = es.Rename(o, Files, "file1.bin", "file2.bin")
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// correct file rename
	after, err := es.Rename(o, Files, "file1.bin", "file3.bin")
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	files = after.Files()
	if len(after.Files()) != 2 {
		t.Fatalf("missing object files\n")
	}

	//fmt.Printf("Name 0: [%s]\n", files[0].Name())
	//fmt.Printf("Name 1: [%s]\n", files[1].Name())
	if files[0].Name() == "file2.bin" {
		if files[1].Name() != "file3.bin" {
			t.Fatalf("expected 'file3.bin' but got '%s'\n", files[1].Name())
		}
	} else {
		if files[0].Name() == "file3.bin" {
			if files[1].Name() != "file2.bin" {
				t.Fatalf("expected 'file2.bin' but got '%s'\n", files[1].Name())
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

//
// end of file
//
