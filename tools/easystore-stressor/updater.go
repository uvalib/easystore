package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/uvalib/easystore/uvaeasystore"
)

func updater(id int, wg *sync.WaitGroup, es uvaeasystore.EasyStore, namespace string, debug bool, count int) {

	defer wg.Done()

	workerId := fmt.Sprintf("updater-%d", id)
	res := getObjectSet(workerId, namespace, es)

	if len(res) == 0 {
		log.Printf("[%s]: no objects available, terminating", workerId)
		os.Exit(99)
	}

	start := time.Now()

	// main updater loop
	for ix := 0; ix < count; ix++ {

		// if we have no objects in our local list
		if len(res) == 0 {
			// get a set
			res = getObjectSet(workerId, namespace, es)
			if len(res) == 0 {
				log.Printf("[%s]: no objects available, terminating", workerId)
				os.Exit(99)
			}
		}

		// select an item at random
		itemIx := rand.Intn(len(res))
		o := res[itemIx]

		eso, err := es.ObjectGetByKey(namespace, o.Id(), uvaeasystore.AllComponents)
		if err != nil {
			if errors.Is(err, uvaeasystore.ErrNotFound) == true {
				log.Printf("[%s]: object deleted... continuing", workerId)
				// delete the current item from the set
				res = deleteElement(res, itemIx)
			} else {
				log.Printf("[%s]: error (%s) getting object (%s), terminating", workerId, err.Error(), o.Id())
				os.Exit(99)
			}
		}

		if eso != nil {
			if debug == true {
				log.Printf("[%s]: read %s", workerId, eso.Id())
			}

			// validate the returned object
			validateObject(workerId, eso)

			// update the object
			eso.SetFields(makeFields())
			eso.SetFiles(makeFiles())
			eso.SetMetadata(newMetadataBlob("md.bin"))

			eso, err = es.ObjectUpdate(eso, uvaeasystore.AllComponents)
			if err != nil {
				if errors.Is(err, uvaeasystore.ErrStaleObject) == true {
					log.Printf("[%s]: object is stale... continuing", workerId)
				} else {
					log.Printf("[%s]: error (%s) updating object, terminating", workerId, err.Error())
					os.Exit(99)
				}
			}

			if eso != nil {
				if debug == true {
					log.Printf("[%s]: updated %s", workerId, eso.Id())
				}

				// validate the returned object
				validateObject(workerId, eso)
			}
		}

		if ix > 0 && ix%25 == 0 {
			log.Printf("[%s]: completed %d iterations...", workerId, ix)
		}
	}

	duration := time.Since(start)
	log.Printf("[%s]: terminating normally after %d iterations (elapsed %d ms)", workerId, count, duration.Milliseconds())
}

//
// end of file
//
