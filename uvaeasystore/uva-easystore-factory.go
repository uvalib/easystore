//
// factory/helper methods
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

// NewEasyStore - factory for our EasyStore interface
func NewEasyStore(config EasyStoreImplConfig) (EasyStore, error) {
	return newEasyStore(config)
}

// NewEasyStoreReadonly - factory for our EasyStoreReadonly implementation
func NewEasyStoreReadonly(config EasyStoreImplConfig) (EasyStoreReadonly, error) {
	return newEasyStoreReadonly(config)
}

//
// end of file
//
