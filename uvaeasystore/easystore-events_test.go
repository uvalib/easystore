//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	"testing"

	"github.com/uvalib/librabus-sdk/uvalibrabus"
)

// a bus that captures the published event instead of sending it
type captureBus struct {
	events []uvalibrabus.UvaBusEvent
}

func (b *captureBus) PublishEvent(ev *uvalibrabus.UvaBusEvent) error {
	b.events = append(b.events, *ev)
	return nil
}

// each publisher must tag its event with the matching event name
func TestPublisherEventNames(t *testing.T) {

	tests := []struct {
		name     string
		publish  func(uvalibrabus.UvaBus, EasyStoreObject) error
		expected string
	}{
		{"pubObjectCreate", pubObjectCreate, uvalibrabus.EventObjectCreate},
		{"pubObjectUpdate", pubObjectUpdate, uvalibrabus.EventObjectUpdate},
		{"pubObjectDelete", pubObjectDelete, uvalibrabus.EventObjectDelete},
		{"pubMetadataUpdate", pubMetadataUpdate, uvalibrabus.EventMetadataUpdate},
		{"pubFileCreate", pubFileCreate, uvalibrabus.EventFileCreate},
		{"pubFileDelete", pubFileDelete, uvalibrabus.EventFileDelete},
		{"pubFileUpdate", pubFileUpdate, uvalibrabus.EventFileUpdate},
	}

	for _, test := range tests {
		bus := &captureBus{}
		o := NewEasyStoreObject(goodNamespace, "")

		if err := test.publish(bus, o); err != nil {
			t.Fatalf("%s: expected 'OK' but got '%s'\n", test.name, err)
		}

		if len(bus.events) != 1 {
			t.Fatalf("%s: expected 1 event but got %d\n", test.name, len(bus.events))
		}

		ev := bus.events[0]
		if ev.EventName != test.expected {
			t.Fatalf("%s: expected event name '%s' but got '%s'\n", test.name, test.expected, ev.EventName)
		}
		if ev.Namespace != o.Namespace() {
			t.Fatalf("%s: expected namespace '%s' but got '%s'\n", test.name, o.Namespace(), ev.Namespace)
		}
		if ev.Identifier != o.Id() {
			t.Fatalf("%s: expected identifier '%s' but got '%s'\n", test.name, o.Id(), ev.Identifier)
		}
	}
}

// an unconfigured bus is not an error, it just means no telemetry
func TestPublishNoBus(t *testing.T) {

	o := NewEasyStoreObject(goodNamespace, "")
	if err := pubFileDelete(nil, o); err != ErrBusNotConfigured {
		t.Fatalf("expected '%s' but got '%s'\n", ErrBusNotConfigured, err)
	}
}

//
// end of file
//
