package main

import (
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/uvalib/easystore/uvaeasystore"
)

func reader(id int, wg *sync.WaitGroup, es uvaeasystore.EasyStore, namespace string, debug bool, count int) {

	defer wg.Done()

	start := time.Now()
	fields := uvaeasystore.DefaultEasyStoreFields()
	results, err := es.ObjectGetByFields(namespace, fields, uvaeasystore.BaseComponent)

	if err != nil {
		log.Printf("[reader %d]: error (%s) getting object set, terminating", id, err.Error())
		os.Exit(99)
	}

	if results.Count() == 0 {
		log.Printf("[reader %d]: no objects available, terminating", id)
		os.Exit(99)
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
	log.Printf("[reader %d]: loaded %d objects (elapsed %d ms)", id, len(res), duration.Milliseconds())

	start = time.Now()
	// main reader loop
	for ix := 0; ix < count; ix++ {

		o := res[rand.Intn(len(res))]

		eso, err := es.ObjectGetByKey(namespace, o.Id(), uvaeasystore.AllComponents)
		if err != nil {
			log.Printf("[reader %d]: error (%s) getting object (%s), terminating", id, err.Error(), o.Id())
			os.Exit(99)
		}

		if debug == true {
			log.Printf("[reader %d]: read %s", id, eso.Id())
		}

		// validate the object
		if eso.Fields() == nil {
			log.Printf("[reader %d]: object (%s) has no fields, terminating", id, o.Id())
			os.Exit(99)
		}

		if eso.Metadata() == nil {
			log.Printf("[reader %d]: object (%s) has no metadata, terminating", id, o.Id())
			os.Exit(99)
		}

		if eso.Files() == nil {
			log.Printf("[reader %d]: object (%s) has no files, terminating", id, o.Id())
			os.Exit(99)
		}

		if ix > 0 && ix%25 == 0 {
			log.Printf("[reader %d]: completed %d iterations...", id, ix)
		}
	}

	duration = time.Since(start)
	log.Printf("[reader %d]: terminating normally after %d iterations (elapsed %d ms)", id, count, duration.Milliseconds())
}

//
// end of file
//
