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

func deleter(id int, wg *sync.WaitGroup, es uvaeasystore.EasyStore, namespace string, debug bool, count int) {

	defer wg.Done()

	workerId := fmt.Sprintf("deleter-%d", id)

	start := time.Now()

	var res []uvaeasystore.EasyStoreObject

	// main reader loop
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

		// delete the current item from the set
		res = deleteElement(res, itemIx)

		// delete the object
		_, err := es.ObjectDelete(o, uvaeasystore.BaseComponent)
		if err != nil {
			if errors.Is(err, uvaeasystore.ErrNotFound) == true {
				log.Printf("[%s]: object deleted... continuing", workerId)
			} else {
				log.Printf("[%s]: error (%s) deleting object (%s), terminating", workerId, err.Error(), o.Id())
				os.Exit(99)
			}
		}

		if err != nil {
			if debug == true {
				log.Printf("[%s]: deleted %s", workerId, o.Id())
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
