package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/uvalib/easystore/uvaeasystore"
)

func writer(id int, wg *sync.WaitGroup, es uvaeasystore.EasyStore, namespace string, debug bool, count int) {

	defer wg.Done()

	start := time.Now()

	// main writer loop
	for ix := 0; ix < count; ix++ {

		// make fields
		fields := uvaeasystore.DefaultEasyStoreFields()
		fields["field1"] = strconv.Itoa(ix + rand.Intn(1000))
		fields["field2"] = strconv.Itoa(ix + rand.Intn(1000))
		fields["field3"] = strconv.Itoa(ix + rand.Intn(1000))

		// make files
		f1 := newBinaryBlob("file1.bin")
		f2 := newBinaryBlob("file2.bin")
		f3 := newBinaryBlob("file3.bin")
		files := []uvaeasystore.EasyStoreBlob{f1, f2, f3}

		// make metadata
		metadata := newMetadataBlob("md.bin")

		o := uvaeasystore.NewEasyStoreObject(namespace, "")

		o.SetFields(fields)
		o.SetFiles(files)
		o.SetMetadata(metadata)

		eso, err := es.ObjectCreate(o)
		if err != nil {
			log.Printf("[writer %d]: error (%s) creating object, terminating", id, err.Error())
			os.Exit(99)
		}

		if debug == true {
			log.Printf("[writer %d]: created %s", id, eso.Id())
		}

		if ix > 0 && ix%25 == 0 {
			log.Printf("[writer %d]: completed %d iterations...", id, ix)
		}
	}

	duration := time.Since(start)
	log.Printf("[writer %d]: terminating normally after %d iterations (elapsed %d ms)", id, count, duration.Milliseconds())
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

//
// end of file
//
