//
//
//

package uva_easystore

import (
	"github.com/google/uuid"
	"log"
	"testing"
)

// test invariants
var goodNamespace = "libraopen"
var badNamespace = "blablabla"
var goodId = "12345"
var badId = "blablabla"

func uniqueId() string {
	return uuid.New().String()
}

func testSetupReadonly(t *testing.T) EasyStoreReadonly {
	config := DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace(goodNamespace)
	config.Logger(log.Default())

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
	config.Logger(log.Default())

	es, err := NewEasyStore(config)
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return es
}

//
// end of file
//
