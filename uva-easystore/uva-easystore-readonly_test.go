//
//
//

package uva_easystore

import (
	"errors"
	"testing"
)

func TestBadConfig(t *testing.T) {
	config := DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace("")

	_, err := NewEasyStoreReadonly(config)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("Expected: '%s', got '%s'\n", expected, err)
	}
}

func TestGetById(t *testing.T) {
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

//
// end of file
//
