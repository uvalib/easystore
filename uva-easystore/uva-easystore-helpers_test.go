//
//
//

package uva_easystore

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"
)

// test invariants
var goodNamespace = "libraopen"
var badNamespace = "blablabla"
var goodId = "1234567890"
var badId = "blablabla"
var jsonPayload = []byte("{}")

func newTestObject(id string) EasyStoreObject {
	if len(id) == 0 {
		id = newUniqueId()
	}
	return NewEasyStoreObject(id)
}

func newUniqueId() string {
	b := make([]byte, 6) // equals 12 characters
	rand.Read(b)
	return fmt.Sprintf("oid:%s", hex.EncodeToString(b))
}

func testSetupReadonly(t *testing.T) EasyStoreReadonly {
	config := DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace(goodNamespace)
	//config.Logger(log.Default())

	esro, err := NewEasyStoreReadonly(config)
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return esro
}

func testSetup(t *testing.T) EasyStore {
	config := DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace(goodNamespace)
	//config.Logger(log.Default())

	es, err := NewEasyStore(config)
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return es
}

func validateObject(t *testing.T, obj EasyStoreObject, which EasyStoreComponents) {

	// test the contents of the object
	if len(obj.Id()) == 0 {
		t.Fatalf("object id is empty\n")
	}

	// should it have fields
	fieldCount := len(obj.Fields())
	if (which & Fields) == Fields {
		if fieldCount != 0 {
			for n, v := range obj.Fields() {
				if len(n) == 0 {
					t.Fatalf("object field key is empty\n")
				}
				if len(v) == 0 {
					t.Fatalf("object field value is empty\n")
				}
			}
		} else {
			t.Fatalf("expected object fields but got none\n")
		}
	} else {
		if fieldCount != 0 {
			t.Fatalf("unexpected object fields\n")
		}
	}

	// should it have files
	fileCount := len(obj.Files())
	if (which & Files) == Files {
		if fileCount != 0 {
			for _, f := range obj.Files() {
				if len(f.Name()) == 0 {
					t.Fatalf("file name is empty\n")
				}
				if len(f.MimeType()) == 0 {
					t.Fatalf("file mime type is empty\n")
				}
				if len(f.Url()) == 0 {
					t.Fatalf("file url is empty\n")
				}
				if f.Created().IsZero() == true {
					t.Fatalf("file create time is empty\n")
				}
				if f.Modified().IsZero() == true {
					t.Fatalf("file modified time is empty\n")
				}
			}
		} else {
			t.Fatalf("expected object files but got none\n")
		}
	} else {
		if fileCount != 0 {
			t.Fatalf("unexpected object files\n")
		}
	}

	// should it have metadata
	md := obj.Metadata()
	if (which & Metadata) == Metadata {
		if md != nil {
			if len(md.MimeType()) == 0 {
				t.Fatalf("object mime type is empty\n")
			}
			if md.Created().IsZero() == true {
				t.Fatalf("object create time is empty\n")
			}
			if md.Modified().IsZero() == true {
				t.Fatalf("object modified time is empty\n")
			}
		} else {
			t.Fatalf("expected object metadata but got none\n")
		}
	} else {
		if md != nil {
			t.Fatalf("unexpected object metadata\n")
		}
	}
}

//
// end of file
//
