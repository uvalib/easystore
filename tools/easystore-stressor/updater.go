package main

import (
	"errors"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"github.com/uvalib/easystore/uvaeasystore"
)

func updater(id int, wg *sync.WaitGroup, es uvaeasystore.EasyStore, namespace string, debug bool, count int) {

	defer wg.Done()

	fields := uvaeasystore.DefaultEasyStoreFields()
	results, err := es.ObjectGetByFields(namespace, fields, uvaeasystore.BaseComponent)

	if err != nil {
		log.Printf("[updater %d]: error getting object set, terminating", id)
		os.Exit(99)
	}

	if results.Count() == 0 {
		log.Printf("[updater %d]: no objects available, terminating", id)
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

	// main reader loop
	log.Printf("[updater %d]: loaded %d objects", id, len(res))
	for ix := 0; ix < count; ix++ {

		o := res[rand.Intn(len(res))]

		eso, err := es.ObjectGetByKey(namespace, o.Id(), uvaeasystore.AllComponents)
		if err != nil {
			log.Printf("[updater %d]: error getting object (%s), terminating", id, o.Id())
			os.Exit(99)
		}

		if debug == true {
			log.Printf("[updater %d]: read %s", id, eso.Id())
		}

		// validate the object
		if eso.Fields() == nil {
			log.Printf("[updater %d]: object (%s) has no fields, terminating", id, o.Id())
			os.Exit(99)
		}

		if eso.Metadata() == nil {
			log.Printf("[updater %d]: object (%s) has no metadata, terminating", id, o.Id())
			os.Exit(99)
		}

		if eso.Files() == nil {
			log.Printf("[updater %d]: object (%s) has no files, terminating", id, o.Id())
			os.Exit(99)
		}

		// make fields
		fields := uvaeasystore.DefaultEasyStoreFields()
		fields["ufield1"] = strconv.Itoa(ix + rand.Intn(1000))
		fields["ufield2"] = strconv.Itoa(ix + rand.Intn(1000))
		fields["ufield3"] = strconv.Itoa(ix + rand.Intn(1000))

		// make files
		f1 := newBinaryBlob("ufile1.bin")
		f2 := newBinaryBlob("ufile2.bin")
		f3 := newBinaryBlob("ufile3.bin")
		files := []uvaeasystore.EasyStoreBlob{f1, f2, f3}

		// make metadata
		metadata := newMetadataBlob("umd.bin")

		eso.SetFields(fields)
		eso.SetFiles(files)
		eso.SetMetadata(metadata)

		eso, err = es.ObjectUpdate(eso, uvaeasystore.AllComponents)
		if err != nil {
			log.Printf("[updater %d]: ERROR %v", id, err)
			if errors.Is(err, uvaeasystore.ErrStaleObject) == false {
				log.Printf("[updater %d]: error updating object, terminating", id)
				os.Exit(99)
			} else {
				log.Printf("[updater %d]: stale object, continuing", id)
			}
		}

		if debug == true && eso != nil {
			log.Printf("[updater %d]: updated %s", id, eso.Id())
		}

		if ix > 0 && ix%25 == 0 {
			log.Printf("[updater %d]: completed %d iterations...", id, ix)
		}
	}

	log.Printf("[updater %d]: terminating normally after %d iterations", id, count)
}

//
// end of file
//
