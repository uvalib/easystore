//
//
//

package uva_easystore

import (
	"errors"
	"testing"
)

func TestCreate(t *testing.T) {
	es := testSetup(t)
	so := NewEasyStoreObject(uniqueId())
	_, err := es.Create(so)
	if !errors.Is(err, nil) {
		t.Fatalf("Expected: 'success', got '%s'\n", err)
	}
}

//
// end of file
//
