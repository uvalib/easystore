package main

import (
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"log"
	"os"
	"strconv"
)

// main entry point
func main() {

	if len(os.Args) != 3 {
		log.Fatalf("ERROR: use: %s <import dir> <mode, etd|open>", os.Args[0])
	}

	inputdir := os.Args[1]
	mode := os.Args[2]

	// validate
	_, err := os.Stat(inputdir)
	if err != nil {
		log.Fatalf("ERROR: import dir does not exist or is not readable (%s)", err.Error())
	}

	if mode != "etd" && mode != "open" {
		log.Fatalf("ERROR: mode must be 'etd' or 'open'")
	}

	// configure what we need
	config := uvaeasystore.DatastoreSqliteConfig{
		Filesystem: os.Getenv("SQLITEDIR"),
		Namespace:  os.Getenv("SQLITEFILE"),
		//Log:        log.Default(),
	}

	//config := uvaeasystore.DatastorePostgresConfig{
	//	DbHost:     os.Getenv("DBHOST"),
	//	DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//	DbName:     os.Getenv("DBNAME"),
	//	DbUser:     os.Getenv("DBUSER"),
	//	DbPassword: os.Getenv("DBPASSWD"),
	//	DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//	//  Log:        Log.Default(),
	//}

	es, err := uvaeasystore.NewEasyStore(config)
	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// use the appropriate serializer
	var serializer uvaeasystore.EasyStoreSerializer
	if mode == "etd" {
		serializer = libraEtdSerializer{}
	} else {
		serializer = libraOpenSerializer{}
	}

	okCount := 0
	errCount := 0
	var obj uvaeasystore.EasyStoreObject

	items, err := os.ReadDir(inputdir)
	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	// go through our list
	for _, i := range items {
		if i.IsDir() == true {

			dirname := fmt.Sprintf("%s/%s", inputdir, i.Name())
			log.Printf("DEBUG: importing from %s", dirname)

			if mode == "etd" {
				//obj, err = makeObjectFromEtd(serializer, dirname)
			} else {
				obj, err = makeObjectFromOpen(serializer, dirname)
			}

			if err != nil {
				log.Printf("WARNING: creating object (%s), continuing", err.Error())
				errCount++
				continue
			}

			_, err = es.Create(obj)
			if err != nil {
				log.Printf("WARNING: importing object (%s), continuing", err.Error())
				errCount++
				continue
			} else {
				okCount++
			}
		}
	}

	log.Printf("INFO: terminate normally, imported %d object(s) and %d error(s)", okCount, errCount)
}

func asIntWithDefault(str string, def int) int {
	if len(str) == 0 {
		return def
	}
	i, err := strconv.Atoi(str)
	if err != nil {
		return def
	}
	return i
}

//
// end of file
//
