package uva_easystore

import (
	"log"
	"testing"
)

// test invariants
var namespace = "libraopen"
var badId = "blablabla"
var goodId = "12345"

func testSetupReadonly(t *testing.T) EasyStoreReadonly {
	config := EasyStoreConfig{Namespace: namespace, log: log.Default()}
	esro, err := NewEasyStoreReadonly(config)
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return esro
}

func testSetup(t *testing.T) EasyStore {
	config := EasyStoreConfig{Namespace: namespace, log: log.Default()}
	es, err := NewEasyStore(config)
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return es
}

//
// end of file
//
