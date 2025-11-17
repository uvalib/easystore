package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/uvalib/easystore/uvaeasystore"
)

func getObjectSet(workerId string, namespace string, es uvaeasystore.EasyStoreReadonly) []uvaeasystore.EasyStoreObject {

	start := time.Now()
	fields := uvaeasystore.DefaultEasyStoreFields()
	results, err := es.ObjectGetByFields(namespace, fields, uvaeasystore.BaseComponent)

	if err != nil {
		log.Printf("[%s]: error (%s) getting object set, terminating", workerId, err.Error())
		os.Exit(99)
	}

	if results.Count() == 0 {
		return make([]uvaeasystore.EasyStoreObject, 0, 0)
	}

	res := make([]uvaeasystore.EasyStoreObject, 0, results.Count())
	for {
		o, err := results.Next()
		if err != nil {
			break
		}
		res = append(res, o)
	}

	duration := time.Since(start)
	log.Printf("[%s]: loaded %d objects (elapsed %d ms)", workerId, len(res), duration.Milliseconds())

	return res
}

func deleteElement(objects []uvaeasystore.EasyStoreObject, ix int) []uvaeasystore.EasyStoreObject {

	ln := len(objects)
	if ln == 0 {
		return objects
	}

	if ix >= ln {
		return objects[:ln-1]
	}

	// delete the current item from the set
	return append(objects[:ix], objects[ix+1:]...)

}

func makeFiles() []uvaeasystore.EasyStoreBlob {

	// make files
	f1 := newBinaryBlob("file1.bin")
	f2 := newBinaryBlob("file2.bin")
	f3 := newBinaryBlob("file3.bin")
	return []uvaeasystore.EasyStoreBlob{f1, f2, f3}
}

func makeFields() uvaeasystore.EasyStoreObjectFields {
	fields := uvaeasystore.DefaultEasyStoreFields()
	fields["field1"] = fmt.Sprintf("field1-value-%d", rand.Intn(1000))
	fields["field2"] = fmt.Sprintf("field2-value-%d", rand.Intn(1000))
	fields["field3"] = fmt.Sprintf("field3-value-%d", rand.Intn(1000))
	return fields
}

func newBinaryBlob(filename string) uvaeasystore.EasyStoreBlob {
	buf := make([]byte, 512)
	// then we can call rand.Read.
	_, _ = rand.Read(buf)
	return uvaeasystore.NewEasyStoreBlob(filename, "application/octet-stream", buf)
}

func newMetadataBlob(filename string) uvaeasystore.EasyStoreBlob {
	buf := make([]byte, 512)
	// then we can call rand.Read.
	_, _ = rand.Read(buf)
	return uvaeasystore.NewEasyStoreBlob(filename, "application/octet-stream", buf)
}

func validateObject(workerId string, eso uvaeasystore.EasyStoreObject) {

	if eso.Fields() == nil {
		log.Printf("[%s]: object (%s) has no fields, terminating", workerId, eso.Id())
		os.Exit(99)
	}

	if eso.Metadata() == nil {
		log.Printf("[%s]: object (%s) has no metadata, terminating", workerId, eso.Id())
		os.Exit(99)
	}

	if eso.Files() == nil {
		log.Printf("[%s]: object (%s) has no files, terminating", workerId, eso.Id())
		os.Exit(99)
	}
}

//
// end of file
//
