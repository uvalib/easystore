//
//
//

package uvaeasystore

import (
	"errors"
	"testing"
)

func TestReadonlyEmptyNamespace(t *testing.T) {
	// configure what we need
	config := DatastoreSqliteConfig{
		filesystem: filesystem,
		namespace:  "",
	}

	_, err := NewEasyStoreReadonly(config)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestReadonlyNotFoundNamespace(t *testing.T) {
	// configure what we need
	config := DatastoreSqliteConfig{
		filesystem: filesystem,
		namespace:  badNamespace,
	}

	_, err := NewEasyStoreReadonly(config)
	expected := ErrNamespaceNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestEmptyNamespace(t *testing.T) {
	// configure what we need
	config := DatastoreSqliteConfig{
		filesystem: filesystem,
		namespace:  "",
	}

	_, err := NewEasyStore(config)
	expected := ErrBadParameter
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

func TestNotFoundNamespace(t *testing.T) {
	// configure what we need
	config := DatastoreSqliteConfig{
		filesystem: filesystem,
		namespace:  badNamespace,
	}

	_, err := NewEasyStore(config)
	expected := ErrNamespaceNotFound
	if !errors.Is(err, expected) {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

//
// end of file
//
