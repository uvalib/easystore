//
//
//

package uva_easystore

import (
	"errors"
	"testing"
)

func TestGetById(t *testing.T) {
	esro := testSetupReadonly(t)
	_, err := esro.GetById(badId, AllComponents)
	expected := ErrNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("Expected: '%s', got '%s'\n", expected, err)
	}
}

//
// end of file
//
