package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/uvalib/easystore/uvaeasystore"
)

// main entry point
func main() {

	var namespace string
	var readers int
	var writers int
	var updaters int
	var deleters int
	var readCount int
	var writeCount int
	var updateCount int
	var deleteCount int
	var debug bool
	var logger *log.Logger

	flag.StringVar(&namespace, "namespace", "es-stressor", "Easystore object namespace")
	flag.IntVar(&readers, "readers", 1, "Reader workers")
	flag.IntVar(&writers, "writers", 1, "Writer workers")
	flag.IntVar(&updaters, "updaters", 1, "Updater workers")
	flag.IntVar(&deleters, "deleters", 1, "Deleter workers")
	flag.IntVar(&readCount, "readcount", 100, "Read iteration count")
	flag.IntVar(&writeCount, "writecount", 100, "Write iteration count")
	flag.IntVar(&updateCount, "updatecount", 100, "Update iteration count")
	flag.IntVar(&deleteCount, "deletecount", 100, "Delete iteration count")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	if readers == 0 && writers == 0 && updaters == 0 && deleters == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

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

	// start reader workers
	if readers != 0 {
		log.Printf("[main] starting %d reader(s) for %d iterations...", readers, readCount)
		for r := 1; r <= readers; r++ {
			wg.Add(1)
			go reader(r, &wg, es, namespace, debug, readCount)
		}
	} else {
		log.Printf("[main] no readers configured...")
	}

	// start writer workers
	if writers != 0 {
		log.Printf("[main] starting %d writer(s) for %d iterations...", writers, writeCount)
		for w := 1; w <= writers; w++ {
			wg.Add(1)
			go writer(w, &wg, es, namespace, debug, writeCount)
		}
	} else {
		log.Printf("[main] no writers configured...")
	}

	// start updater workers
	if updaters != 0 {
		log.Printf("[main] starting %d updater(s) for %d iterations...", updaters, updateCount)
		for u := 1; u <= updaters; u++ {
			wg.Add(1)
			go updater(u, &wg, es, namespace, debug, updateCount)
		}
	} else {
		log.Printf("[main] no updaters configured...")
	}

	// start deleter workers
	if deleters != 0 {
		log.Printf("[main] starting %d deleter(s) for %d iterations...", deleters, updateCount)
		for d := 1; d <= deleters; d++ {
			wg.Add(1)
			go deleter(d, &wg, es, namespace, debug, updateCount)
		}
	} else {
		log.Printf("[main] no deleters configured...")
	}

	log.Printf("[main] waiting for worker(s) to complete...")
	wg.Wait()
	log.Printf("[main] terminating normally")
}

//
// end of file
//
