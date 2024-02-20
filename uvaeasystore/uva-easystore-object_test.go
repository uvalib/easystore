//
//
//

package uvaeasystore

import (
	"testing"
)

func TestObjectBlobsUpdate(t *testing.T) {
	obj := newTestObject("")
	if obj.Files() != nil {
		t.Fatalf("expected empty but got non-empty\n")
	}

	// add some files
	f1 := NewEasyStoreBlob("file1.txt", "text/plain;charset=UTF-8", []byte("file1: bla bla bla"))
	f2 := NewEasyStoreBlob("file2.txt", "text/plain;charset=UTF-8", []byte("file2: bla bla bla"))
	blobs := []EasyStoreBlob{f1, f2}

	// update the object
	obj.SetFiles(blobs)

	if obj.Files() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}
}

//
// end of file
//
