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
	es := testSetup(t)
	defer es.Close()

	// empty id
	_, err := es.ObjectGetByKey(goodNamespace, "", BaseComponent)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// a new object
	o := NewEasyStoreObject(goodNamespace, "")

	// does not already exist
	_, err = es.ObjectGetByKey(goodNamespace, o.Id(), BaseComponent)
	expected = ErrNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// create the object
	_, err = es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we expect to find this one
	obj, err := es.ObjectGetByKey(goodNamespace, o.Id(), BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	validateObject(t, obj, BaseComponent)

	// same thing with a bad namespace
	_, err = es.ObjectGetByKey(badNamespace, o.Id(), BaseComponent)
	expected = ErrNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestGetByIds(t *testing.T) {
	es := testSetup(t)
	defer es.Close()

	// empty ids
	ids := []string{}
	_, err := es.ObjectGetByKeys(goodNamespace, ids, BaseComponent)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// a new object
	o := NewEasyStoreObject(goodNamespace, "")

	// does not already exist
	ids = []string{o.Id()}
	_, err = es.ObjectGetByKeys(goodNamespace, ids, BaseComponent)
	expected = ErrNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// create the object
	_, err = es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we expect to find this one
	ids = []string{o.Id()}
	iter, err := es.ObjectGetByKeys(goodNamespace, ids, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received 1 object
	if iter.Count() == 1 {
		_, err := iter.Next()
		if err != nil {
			t.Fatalf("expected 'OK' but got '%s'\n", err)
		}
	}

	// same thing with a bad namespace
	ids = []string{o.Id()}
	iter, err = es.ObjectGetByKeys(badNamespace, ids, BaseComponent)
	expected = ErrNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// good and bad id
	ids = []string{o.Id(), badId}
	iter, err = es.ObjectGetByKeys(goodNamespace, ids, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received 1 object
	if iter.Count() == 1 {
		o, err := iter.Next()
		if err != nil {
			t.Fatalf("expected 'OK' but got '%s'\n", err)
		}
		validateObject(t, o, BaseComponent)
	}
}

func TestGetByFoundFields(t *testing.T) {
	es := testSetup(t)
	defer es.Close()

	// a new object
	o := NewEasyStoreObject(goodNamespace, "")

	// make some unique fields
	fields := DefaultEasyStoreFields()
	fields["key1"] = o.Id()
	fields["key2"] = o.Id()
	o.SetFields(fields)

	// create the object
	_, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	fieldsSearch := DefaultEasyStoreFields()
	fieldsSearch["key1"] = o.Id()

	// search by specific namespace
	iter, err := es.ObjectGetByFields(goodNamespace, fieldsSearch, Fields)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received some objects
	if iter.Count() == 0 {
		t.Fatalf("expected objects but got none\n")
	}

	// go through the list of objects and validate
	o, err = iter.Next()
	for err == nil {
		validateObject(t, o, Fields)
		ensureObjectHasFields(t, o, fieldsSearch)
		o, err = iter.Next()
	}

	if errors.Is(err, io.EOF) != true {
		t.Fatalf("expected '%s' but got '%s'\n", io.EOF, err)
	}

	// search by empty namespace
	iter, err = es.ObjectGetByFields("", fieldsSearch, Fields)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received some objects
	if iter.Count() == 0 {
		t.Fatalf("expected objects but got none\n")
	}

	// go through the list of objects and validate
	o, err = iter.Next()
	for err == nil {
		validateObject(t, o, Fields)
		ensureObjectHasFields(t, o, fieldsSearch)
		o, err = iter.Next()
	}

	if errors.Is(err, io.EOF) != true {
		t.Fatalf("expected '%s' but got '%s'\n", io.EOF, err)
	}
}

func TestGetByNotFoundFields(t *testing.T) {
	esro := testSetupReadonly(t)
	defer esro.Close()
	fields := DefaultEasyStoreFields()
	fields["key1"] = newObjectId()

	// search by specific namespace
	iter, err := esro.ObjectGetByFields(goodNamespace, fields, Fields)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// ensure we received an empty iterator
	if iter.Count() != 0 {
		t.Fatalf("expected no objects but got some\n")
	}
}

func TestGetByEmptyFields(t *testing.T) {
	esro := testSetupReadonly(t)
	defer esro.Close()
	fields := EasyStoreObjectFields{}

	//empty fields, should be all items
	iter, err := esro.ObjectGetByFields(goodNamespace, fields, BaseComponent)
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
