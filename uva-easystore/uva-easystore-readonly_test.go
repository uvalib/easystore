//
//
//

package uva_easystore

import (
	"errors"
	"io"
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

	validateObject(t, obj, AllComponents)
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
		validateObject(t, o, NoComponents)
	}
}

func TestDefaultGetByFields(t *testing.T) {
	esro := testSetupReadonly(t)
	fields := EasyStoreObjectFields{}
	fields.fields = make(map[string]string)
	fields.fields["thekey"] = "thevalue"

	//empty fields, should be all items
	iter, err := esro.GetByFields(fields, NoComponents)
	if err != nil {
		t.Fatalf("Expected: 'OK', got '%s'\n", err)
	}

	// ensure we received some objects
	if iter.Count() == 0 {
		t.Fatalf("Expected: objects but got none\n")
	}

	// go through the list of objects and validate
	o, err := iter.Next()
	for err == nil {
		validateObject(t, o, NoComponents)
		o, err = iter.Next()
	}

	if errors.Is(err, io.EOF) != true {
		t.Fatalf("Expected: '%s', got '%s'\n", io.EOF, err)
	}
}

func TestDefaultGetByEmptyFields(t *testing.T) {
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

	// go through the list of objects and validate
	o, err := iter.Next()
	for err == nil {
		validateObject(t, o, NoComponents)
		o, err = iter.Next()
	}

	if errors.Is(err, io.EOF) != true {
		t.Fatalf("Expected: '%s', got '%s'\n", io.EOF, err)
	}
}

//
// end of file
//
