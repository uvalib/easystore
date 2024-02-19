//
//
//

package uvaeasystore

import (
	"errors"
	"io"
	"testing"
)

func TestGetById(t *testing.T) {
	esro := testSetupReadonly(t)

	// empty id
	_, err := esro.GetById("", BaseComponent)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// bad id (not found)
	_, err = esro.GetById(badId, BaseComponent)
	expected = ErrNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// good id
	obj, err := esro.GetById(goodId, AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test the contents of the object
	if obj.Id() != goodId {
		t.Fatalf("expected '%s' but got '%s'\n", goodId, obj.Id())
	}

	validateObject(t, obj, AllComponents)
}

func TestGetByIds(t *testing.T) {
	esro := testSetupReadonly(t)

	// bad id (not found)
	ids := []string{badId}
	_, err := esro.GetByIds(ids, BaseComponent)
	expected := ErrNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// good id
	ids = []string{goodId}
	iter, err := esro.GetByIds(ids, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received 1 object
	if iter.Count() == 1 {
		o, err := iter.Next()
		if err != nil {
			t.Fatalf("expected 'OK' but got '%s'\n", err)
		}
		if o.Id() != goodId {
			t.Fatalf("expected '%s' but got '%s'\n", goodId, o.Id())
		}
	}

	// good and bad id
	ids = []string{goodId, badId}
	iter, err = esro.GetByIds(ids, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received 1 object
	if iter.Count() == 1 {
		o, err := iter.Next()
		if err != nil {
			t.Fatalf("expected 'OK' but got '%s'\n", err)
		}
		if o.Id() != goodId {
			t.Fatalf("expected '%s' but got '%s'\n", goodId, o.Id())
		}
		validateObject(t, o, BaseComponent)
	}
}

func TestGetByFields(t *testing.T) {
	esro := testSetupReadonly(t)
	fields := DefaultEasyStoreFields()
	//fields = make(map[string]string)
	fields["thekey"] = "thevalue"

	//empty fields, should be all items
	iter, err := esro.GetByFields(fields, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received some objects
	if iter.Count() == 0 {
		t.Fatalf("expected objects but got none\n")
	}

	// go through the list of objects and validate
	o, err := iter.Next()
	for err == nil {
		validateObject(t, o, BaseComponent)
		o, err = iter.Next()
	}

	if errors.Is(err, io.EOF) != true {
		t.Fatalf("expected '%s' but got '%s'\n", io.EOF, err)
	}
}

func TestGetByEmptyFields(t *testing.T) {
	esro := testSetupReadonly(t)
	fields := EasyStoreObjectFields{}

	//empty fields, should be all items
	iter, err := esro.GetByFields(fields, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received some objects
	if iter.Count() == 0 {
		t.Fatalf("expected objects but got none\n")
	}

	// go through the list of objects and validate
	o, err := iter.Next()
	for err == nil {
		validateObject(t, o, BaseComponent)
		o, err = iter.Next()
	}

	if errors.Is(err, io.EOF) != true {
		t.Fatalf("expected '%s' but got '%s'\n", io.EOF, err)
	}
}

//
// end of file
//
