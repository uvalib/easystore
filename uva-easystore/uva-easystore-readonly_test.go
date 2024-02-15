//
//
//

package uva_easystore

import (
	"errors"
	"testing"
)

func TestEmptyNamespace(t *testing.T) {
	config := DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace("")

	_, err := NewEasyStoreReadonly(config)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("Expected: '%s', got '%s'\n", expected, err)
	}
}

func TestNotFoundNamespace(t *testing.T) {
	config := DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace(badNamespace)

	_, err := NewEasyStoreReadonly(config)
	expected := ErrNamespaceNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("Expected: '%s', got '%s'\n", expected, err)
	}
}

func TestDefaultGetById(t *testing.T) {
	esro := testSetupReadonly(t)

	// empty id
	_, err := esro.GetById("", NoComponents)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("Expected: '%s', got '%s'\n", expected, err)
	}

	// bad id (not found)
	_, err = esro.GetById(badId, NoComponents)
	expected = ErrObjectNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("Expected: '%s', got '%s'\n", expected, err)
	}

	// good id
	obj, err := esro.GetById(goodId, AllComponents)
	if err != nil {
		t.Fatalf("Expected: 'OK', got '%s'\n", err)
	}

	// test the contents of the object
	if obj.Id() != goodId {
		t.Fatalf("Expected: '%s', got '%s'\n", goodId, obj.Id())
	}

	// does it have fields
	if len(obj.Fields().fields) == 0 {
		t.Fatalf("Expected: fields, got none\n")
	}

	// does it have metadata
	if obj.Metadata() == nil {
		t.Fatalf("Expected: metadata, got none\n")
	}

	// does it have files
	if len(obj.Files()) == 0 {
		t.Fatalf("Expected: files, got none\n")
	}
}

func TestDefaultGetByIds(t *testing.T) {
	esro := testSetupReadonly(t)

	// bad id (not found)
	ids := []string{badId}
	_, err := esro.GetByIds(ids, NoComponents)
	expected := ErrObjectNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("Expected: '%s', got '%s'\n", expected, err)
	}

	// good id
	ids = []string{goodId}
	iter, err := esro.GetByIds(ids, NoComponents)
	if err != nil {
		t.Fatalf("Expected: 'OK', got '%s'\n", err)
	}

	// ensure we received 1 object
	if iter.Count() == 1 {
		o, err := iter.Next()
		if err != nil {
			t.Fatalf("Expected: 'OK', got '%s'\n", err)
		}
		if o.Id() != goodId {
			t.Fatalf("Expected: '%s', got '%s'\n", goodId, o.Id())
		}
	}

	// good and bad id
	ids = []string{goodId, badId}
	iter, err = esro.GetByIds(ids, NoComponents)
	if err != nil {
		t.Fatalf("Expected: 'OK', got '%s'\n", err)
	}

	// ensure we received 1 object
	if iter.Count() == 1 {
		o, err := iter.Next()
		if err != nil {
			t.Fatalf("Expected: 'OK', got '%s'\n", err)
		}
		if o.Id() != goodId {
			t.Fatalf("Expected: '%s', got '%s'\n", goodId, o.Id())
		}
	}
}

func TestDefaultGetByFields(t *testing.T) {
	esro := testSetupReadonly(t)
	fields := EasyStoreObjectFields{}

	//empty fields, should be all items
	iter, err := esro.GetByFields(fields, NoComponents)
	if err != nil {
		t.Fatalf("Expected: 'OK', got '%s'\n", err)
	}

	// ensure we received some objects
	if iter.Count() == 0 {
		t.Fatalf("Expected: objects but got none\n")
	}

}

//
// end of file
//
