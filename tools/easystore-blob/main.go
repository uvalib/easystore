package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// main entry point
func main() {

	var mode string
	var id string
	var cmd string
	var name string
	var fname string
	var newName string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3, proxy")
	flag.StringVar(&id, "identifier", "", "Object to change, ns/oid")
	flag.StringVar(&cmd, "cmd", "", "Command, add, del, rename, show, update")
	flag.StringVar(&name, "name", "", "Name (add, del, rename only)")
	flag.StringVar(&fname, "file", "", "New file name (add only)")
	flag.StringVar(&newName, "new", "", "New name (rename only)")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	// check the must haves
	if len(id) == 0 || len(cmd) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	switch cmd {
	case "add":
		if len(name) == 0 || len(fname) == 0 {
			flag.PrintDefaults()
			os.Exit(1)
		}

	case "del":
		if len(name) == 0 {
			flag.PrintDefaults()
			os.Exit(1)
		}

	case "show":

	case "rename":
		if len(name) == 0 || len(newName) == 0 {
			flag.PrintDefaults()
			os.Exit(1)
		}

	case "update":
		if len(name) == 0 || len(fname) == 0 {
			flag.PrintDefaults()
			os.Exit(1)
		}

	default:
		flag.PrintDefaults()
		os.Exit(1)
	}

	parts := strings.Split(id, "/")
	if len(parts) != 2 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	namespace, oid := parts[0], parts[1]

	if debug == true {
		logger = log.Default()
	}

	var implConfig uvaeasystore.EasyStoreImplConfig
	var proxyConfig uvaeasystore.EasyStoreProxyConfig

	// the easystore (or the proxy)
	var es uvaeasystore.EasyStore
	var err error

	switch mode {
	//case "sqlite":
	//	implConfig = uvaeasystore.DatastoreSqliteConfig{
	//		DataSource: os.Getenv("SQLITEFILE"),
	//		Log:        logger,
	//	}
	//	es, err = uvaeasystore.NewEasyStore(implConfig)

	//case "postgres":
	//	implConfig = uvaeasystore.DatastorePostgresConfig{
	//		DbHost:     os.Getenv("DBHOST"),
	//		DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//		DbName:     os.Getenv("DBNAME"),
	//		DbUser:     os.Getenv("DBUSER"),
	//		DbPassword: os.Getenv("DBPASS"),
	//		DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//		Log:        logger,
	//	}
	//	es, err = uvaeasystore.NewEasyStore(implConfig)

	case "s3":
		implConfig = uvaeasystore.DatastoreS3Config{
			Bucket:              os.Getenv("BUCKET"),
			SignerAccessKey:     os.Getenv("SIGNER_ACCESS_KEY"),
			SignerSecretKey:     os.Getenv("SIGNER_SECRET_KEY"),
			SignerExpireMinutes: asIntWithDefault(os.Getenv("SIGNEXPIRE"), 60),
			DbHost:              os.Getenv("DBHOST"),
			DbPort:              asIntWithDefault(os.Getenv("DBPORT"), 0),
			DbName:              os.Getenv("DBNAME"),
			DbUser:              os.Getenv("DBUSER"),
			DbPassword:          os.Getenv("DBPASS"),
			DbTimeout:           asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
			Log:                 logger,
		}
		es, err = uvaeasystore.NewEasyStore(implConfig)

	case "proxy":
		proxyConfig = uvaeasystore.ProxyConfigImpl{
			ServiceEndpoint: os.Getenv("ESENDPOINT"),
			Log:             logger,
		}
		es, err = uvaeasystore.NewEasyStoreProxy(proxyConfig)

	default:
		log.Fatalf("ERROR: unsupported mode (%s)", mode)
	}

	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// important, cleanup properly
	defer es.Close()

	// get the appropriate components
	components := uvaeasystore.BaseComponent
	if cmd == "show" {
		components = uvaeasystore.Files
	}
	eso, err := es.ObjectGetByKey(namespace, oid, components)
	if err == nil {

		switch cmd {
		case "add":
			err = addBlob(es, eso, name, fname)

		case "del":
			err = delBlob(es, eso, name)

		case "rename":
			err = renameBlob(es, eso, name, newName)

		case "show":
			err = show(eso)

		case "update":
			err = updateBlob(es, eso, name, fname)
		}

	} else {
		if errors.Is(err, uvaeasystore.ErrNotFound) == true {
			log.Printf("INFO: not found ns/oid [%s/%s]\n", namespace, oid)
			err = nil
		}
	}

	if err == nil {
		log.Printf("INFO: terminate normally")
	} else {
		log.Printf("ERROR: terminate with '%s'", err.Error())
	}
}

func addBlob(es uvaeasystore.EasyStore, eso uvaeasystore.EasyStoreObject, name string, fname string) error {

	// read the file
	buf, err := os.ReadFile(fname)
	if err != nil {
		log.Printf("INFO: %s not found or not readable\n", fname)
		return nil
	}

	// attempt to determine the content type
	mt := http.DetectContentType(buf)

	// make the new blob
	bl := uvaeasystore.NewEasyStoreBlob(name, mt, buf)
	// and add it
	err = es.FileCreate(eso.Namespace(), eso.Id(), bl)

	return err
}

func delBlob(es uvaeasystore.EasyStore, eso uvaeasystore.EasyStoreObject, name string) error {

	// delete the file
	err := es.FileDelete(eso.Namespace(), eso.Id(), name)

	// handle this case differently
	if errors.Is(err, uvaeasystore.ErrNotFound) == true {
		log.Printf("INFO: not found ns/oid/name [%s/%s/%s]\n", eso.Namespace(), eso.Id(), name)
		err = nil
	}

	return err
}

func renameBlob(es uvaeasystore.EasyStore, eso uvaeasystore.EasyStoreObject, name string, newName string) error {
	err := es.FileRename(eso.Namespace(), eso.Id(), name, newName)

	// handle this case differently
	if errors.Is(err, uvaeasystore.ErrNotFound) == true {
		log.Printf("INFO: not found ns/oid/name [%s/%s/%s]\n", eso.Namespace(), eso.Id(), name)
		err = nil
	}

	return err
}

func show(eso uvaeasystore.EasyStoreObject) error {

	for ix, b := range eso.Files() {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("%02d name:     %s\n", ix+1, b.Name())
		fmt.Printf("%02d url:      %s\n", ix+1, b.Url())
		fmt.Printf("%02d created:  %s\n", ix+1, b.Created())
		fmt.Printf("%02d modified: %s\n", ix+1, b.Modified())
	}
	return nil
}

func updateBlob(es uvaeasystore.EasyStore, eso uvaeasystore.EasyStoreObject, name string, fname string) error {

	// read the file
	buf, err := os.ReadFile(fname)
	if err != nil {
		log.Printf("INFO: %s not found or not readable\n", fname)
		return nil
	}

	// attempt to determine the content type
	mt := http.DetectContentType(buf)

	// make the new blob
	bl := uvaeasystore.NewEasyStoreBlob(name, mt, buf)
	// and update it
	err = es.FileUpdate(eso.Namespace(), eso.Id(), bl)

	// handle this case differently
	if errors.Is(err, uvaeasystore.ErrNotFound) == true {
		log.Printf("INFO: not found ns/oid/name [%s/%s/%s]\n", eso.Namespace(), eso.Id(), name)
		err = nil
	}

	return err
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
