//
//
//

package uva_easystore

import (
	"errors"
	"testing"
)

func TestSimpleDelete(t *testing.T) {
	es := testSetup(t)
	o := newTestObject("")

	// create the new object
	_, err := es.Create(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// we can get it
	_, err = es.GetById(o.Id(), BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// now delete it
	_, err = es.Delete(o, BaseComponent)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// now we cant
	_, err = es.GetById(o.Id(), BaseComponent)
	if errors.Is(err, ErrObjectNotFound) == false {
		if err != nil {
			t.Fatalf("expected '%s' but got '%s'\n", ErrObjectNotFound, err)
		}
	}
}

//
// end of file
//
