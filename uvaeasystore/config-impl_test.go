//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	"errors"
	"log"
	"testing"
)

// the configurations are used through these interfaces and the setters are declared on
// pointer receivers, so it is the pointer types that satisfy them. Callers must pass
// &Config{} rather than Config{}. The tests below are what catch a setter moving back
// to a value receiver, these assertions just pin the interfaces themselves
var _ EasyStoreImplConfig = (*DatastoreS3Config)(nil)
var _ EasyStoreImplConfig = (*DatastorePostgresConfig)(nil)
var _ EasyStoreProxyConfig = (*ProxyConfigImpl)(nil)

// a setter must actually change the configuration it is called on
func TestImplConfigSetters(t *testing.T) {

	logger := log.Default()

	configs := map[string]EasyStoreImplConfig{
		"DatastoreS3Config":       &DatastoreS3Config{BusName: "original", SourceName: "original"},
		"DatastorePostgresConfig": &DatastorePostgresConfig{BusName: "original", SourceName: "original"},
	}

	for name, config := range configs {

		config.SetMessageBus("changed")
		if config.MessageBus() != "changed" {
			t.Fatalf("%s: expected message bus 'changed' but got '%s'\n", name, config.MessageBus())
		}

		config.SetEventSource("changed")
		if config.EventSource() != "changed" {
			t.Fatalf("%s: expected event source 'changed' but got '%s'\n", name, config.EventSource())
		}

		config.SetLogger(logger)
		if config.Logger() != logger {
			t.Fatalf("%s: expected the logger to be set but it was not\n", name)
		}
	}
}

// a setter must actually change the configuration it is called on
func TestProxyConfigSetters(t *testing.T) {

	logger := log.Default()

	var config EasyStoreProxyConfig = &ProxyConfigImpl{ServiceEndpoint: "original", ServiceTimeout: 1}

	config.SetEndpoint("changed")
	if config.Endpoint() != "changed" {
		t.Fatalf("expected endpoint 'changed' but got '%s'\n", config.Endpoint())
	}

	config.SetTimeout(99)
	if config.Timeout() != 99 {
		t.Fatalf("expected timeout 99 but got %d\n", config.Timeout())
	}

	config.SetLogger(logger)
	if config.Logger() != logger {
		t.Fatalf("expected the logger to be set but it was not\n")
	}
}

// the store factories identify their configuration by type, a mismatch is a bad parameter
func TestStoreConfigMismatch(t *testing.T) {

	_, err := newS3Store(&DatastorePostgresConfig{})
	if errors.Is(err, ErrBadParameter) == false {
		t.Fatalf("expected '%s' but got '%s'\n", ErrBadParameter, err)
	}

	_, err = newPostgresStore(&DatastoreS3Config{})
	if errors.Is(err, ErrBadParameter) == false {
		t.Fatalf("expected '%s' but got '%s'\n", ErrBadParameter, err)
	}
}

// a configuration of the right type but with nothing in it is still a bad parameter,
// this exercises the factory through to validation without needing a live store
func TestStoreConfigEmpty(t *testing.T) {

	_, err := newS3Store(&DatastoreS3Config{})
	if errors.Is(err, ErrBadParameter) == false {
		t.Fatalf("expected '%s' but got '%s'\n", ErrBadParameter, err)
	}

	_, err = newPostgresStore(&DatastorePostgresConfig{})
	if errors.Is(err, ErrBadParameter) == false {
		t.Fatalf("expected '%s' but got '%s'\n", ErrBadParameter, err)
	}
}

//
// end of file
//
