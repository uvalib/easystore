package uva_easystore

import (
	"errors"
	"testing"
)

func TestGetById(t *testing.T) {
	esro := testSetupReadonly(t)
	_, err := esro.GetById(badId, AllComponents)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected: '%s', got '%s'\n", ErrNotFound, err)
	}
}

//
// end of file
//
