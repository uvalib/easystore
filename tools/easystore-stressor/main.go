package main

import (
	"flag"
	"github.com/uvalib/easystore/uvaeasystore"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// main entry point
func main() {

	var namespace string
	var readers int
	var writers int
	var updaters int
	var count int
	var debug bool
	var logger *log.Logger

	flag.StringVar(&namespace, "namespace", "es-stressor", "Easystore object namespace")
	flag.IntVar(&readers, "readers", 1, "Reader workers")
	flag.IntVar(&writers, "writers", 1, "Writer workers")
	flag.IntVar(&updaters, "updaters", 1, "Updater workers")
	flag.IntVar(&count, "count", 100, "Iteration count")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

	proxyConfig := uvaeasystore.ProxyConfigImpl{
		ServiceEndpoint: os.Getenv("ESENDPOINT"),
		Log:             logger,
	}
	es, err := uvaeasystore.NewEasyStoreProxy(proxyConfig)

	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// important, cleanup properly
	defer es.Close()

	var wg sync.WaitGroup

	// seed the RNG
	rand.Seed(time.Now().UnixNano())

	// start reader and writer workers

	if readers != 0 {
		log.Printf("[main] starting %d reader(s) for %d iterations...", readers, count)
		for r := 1; r <= readers; r++ {
			wg.Add(1)
			go reader(r, &wg, es, namespace, debug, count)
		}
	} else {
		log.Printf("[main] no readers configured...")
	}

	if writers != 0 {
		log.Printf("[main] starting %d writer(s) for %d iterations...", writers, count)
		for w := 1; w <= writers; w++ {
			wg.Add(1)
			go writer(w, &wg, es, namespace, debug, count)
		}
	} else {
		log.Printf("[main] no writers configured...")
	}

	if updaters != 0 {
		log.Printf("[main] starting %d updater(s) for %d iterations...", updaters, count)
		for u := 1; u <= updaters; u++ {
			wg.Add(1)
			go updater(u, &wg, es, namespace, debug, count)
		}
	} else {
		log.Printf("[main] no updaters configured...")
	}

	log.Printf("[main] waiting for worker(s) to complete...")
	wg.Wait()
	log.Printf("[main] terminating normally")
}

//
// end of file
//
