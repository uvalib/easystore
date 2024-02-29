//
//
//

package uvaeasystore

import (
	"github.com/uvalib/librabus-sdk/uvalibrabus"
	"log"
)

func NewEventBus(eventSource string, eventBus string, logger *log.Logger) (uvalibrabus.UvaBus, error) {
	// we will accept bad config and return nil quietly
	if len(eventBus) == 0 {
		logInfo(logger, "event bus is not configured, no telemetry emitted")
		//return nil, fmt.Errorf( "", "event bus is not configured, no telemetry emitted", ErrBusNotConfigured
		return nil, nil
	}

	cfg := uvalibrabus.UvaBusConfig{eventSource, eventBus, logger}
	return uvalibrabus.NewUvaBus(cfg)
}

func pubObjectCreate(bus uvalibrabus.UvaBus, obj EasyStoreObject) error {
	if bus == nil {
		return ErrBusNotConfigured
	}

	ev := uvalibrabus.NewEasystoreEvent(
		uvalibrabus.EventObjectCreate,
		obj.Namespace(),
		obj.Id())
	return bus.PublishEvent(ev)
}

func pubObjectUpdate(bus uvalibrabus.UvaBus, obj EasyStoreObject) error {
	if bus == nil {
		return ErrBusNotConfigured
	}
	ev := uvalibrabus.NewEasystoreEvent(
		uvalibrabus.EventObjectUpdate,
		obj.Namespace(),
		obj.Id())
	return bus.PublishEvent(ev)
}

func pubObjectDelete(bus uvalibrabus.UvaBus, obj EasyStoreObject) error {
	if bus == nil {
		return ErrBusNotConfigured
	}
	ev := uvalibrabus.NewEasystoreEvent(
		uvalibrabus.EventObjectDelete,
		obj.Namespace(),
		obj.Id())
	return bus.PublishEvent(ev)
}

func pubMetadataUpdate(bus uvalibrabus.UvaBus, obj EasyStoreObject) error {
	if bus == nil {
		return ErrBusNotConfigured
	}
	ev := uvalibrabus.NewEasystoreEvent(
		uvalibrabus.EventMetadataUpdate,
		obj.Namespace(),
		obj.Id())
	return bus.PublishEvent(ev)
}

func pubFileCreate(bus uvalibrabus.UvaBus, obj EasyStoreObject) error {
	if bus == nil {
		return ErrBusNotConfigured
	}
	ev := uvalibrabus.NewEasystoreEvent(
		uvalibrabus.EventFileCreate,
		obj.Namespace(),
		obj.Id())
	return bus.PublishEvent(ev)
}

//
// end of file
//
