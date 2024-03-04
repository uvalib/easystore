//
//
//

package uvaeasystore

import (
	"testing"
)

//func TestVTagFiles(t *testing.T) {
//	es := testSetup(t)
//	o := NewEasyStoreObject(goodNamespace, "")
//
//	// create the new object with no files
//	_, err := es.Create(o)
//	if err != nil {
//		t.Fatalf("expected 'OK' but got '%s'\n", err)
//	}
//
//	// we can get it
//	before, err := es.GetByKey(goodNamespace, o.Id(), AllComponents)
//	if err != nil {
//		t.Fatalf("expected 'OK' but got '%s'\n", err)
//	}
//
//	// add some files
//	f1 := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
//	f2 := NewEasyStoreBlob("file2.txt", "text/plain;charset=UTF-8", []byte("file2: bla bla bla"))
//	files := []EasyStoreBlob{f1, f2}
//	o.SetFiles(files)
//
//	// update the object
//	_, err = es.Update(o, Files)
//	if err != nil {
//		t.Fatalf("expected 'OK' but got '%s'\n", err)
//	}
//
//	// we can still get it
//	after, err := es.GetByKey(goodNamespace, o.Id(), AllComponents)
//	if err != nil {
//		t.Fatalf("expected 'OK' but got '%s'\n", err)
//	}
//
//	if len(before.Files()) != 0 {
//		t.Fatalf("unexpected object files\n")
//	}
//
//	if len(after.Files()) != 2 {
//		t.Fatalf("missing object files\n")
//	}
//}

func TestVTagMetadata(t *testing.T) {
	es := testSetup(t)
	o := NewEasyStoreObject(goodNamespace, "")

	// add some metadata
	mimeType := "application/json"
	metadata := newEasyStoreMetadata(mimeType, jsonPayload)
	o.SetMetadata(metadata)

	// create the new object
	before, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// update the object
	_, err = es.Update(before, Metadata)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can still get it
	after, err := es.GetByKey(goodNamespace, o.Id(), AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if before.Metadata() == nil {
		t.Fatalf("missing before object metadata\n")
	}

	if after.Metadata() == nil {
		t.Fatalf("missing after object metadata\n")
	}

	if len(before.Metadata().VTag()) == 0 {
		t.Fatalf("empty before object metadata vtag\n")
	}

	if len(after.Metadata().VTag()) == 0 {
		t.Fatalf("empty after object metadata vtag\n")
	}

	//if before.Metadata().VTag() == after.Metadata().VTag() {
	//	t.Fatalf("vtags are equal and should not be\n")
	//}
}

//
// end of file
//
