package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/uvalib/easystore/uvaeasystore"
)

func writer(id int, wg *sync.WaitGroup, es uvaeasystore.EasyStore, namespace string, debug bool, count int) {

	defer wg.Done()
	workerId := fmt.Sprintf("writer-%d", id)

	start := time.Now()

	// main writer loop
	for ix := 0; ix < count; ix++ {

		o := uvaeasystore.NewEasyStoreObject(namespace, "")

		// populate the object
		o.SetFields(makeFields())
		o.SetFiles(makeFiles())
		o.SetMetadata(newMetadataBlob("md.bin"))

		eso, err := es.ObjectCreate(o)
		if err != nil {
			log.Printf("[%s]: error (%s) creating object, terminating", workerId, err.Error())
			os.Exit(99)
		}

		if debug == true {
			log.Printf("[%s]: created %s", workerId, eso.Id())
		}

		// validate the returned object
		validateObject(workerId, eso)

		if ix > 0 && ix%25 == 0 {
			log.Printf("[%s]: completed %d iterations...", workerId, ix)
		}
	}

	duration := time.Since(start)
	log.Printf("[%sd]: terminating normally after %d iterations (elapsed %d ms)", workerId, count, duration.Milliseconds())
}

//
// end of file
//
