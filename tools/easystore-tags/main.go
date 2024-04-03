package main

import (
	"errors"
	"flag"
	"github.com/uvalib/easystore/uvaeasystore"
	"log"
	"os"
	"strconv"
	"strings"
)

// main entry point
func main() {

	var mode string
	var id string
	var oper string
	var name string
	var value string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3")
	flag.StringVar(&id, "identifier", "", "Object to change, ns/oid")
	flag.StringVar(&oper, "operation", "add", "Tag operation, add|del")
	flag.StringVar(&name, "name", "", "Tag name")
	flag.StringVar(&value, "value", "", "Tag value (add only)")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	if len(id) == 0 || len(name) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if oper != "add" && oper != "del" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if oper == "add" && len(value) == 0 {
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

	var config uvaeasystore.EasyStoreConfig

	switch mode {
	case "sqlite":
		config = uvaeasystore.DatastoreSqliteConfig{
			DataSource: os.Getenv("SQLITEFILE"),
			Log:        logger,
		}
	case "postgres":
		config = uvaeasystore.DatastorePostgresConfig{
			DbHost:     os.Getenv("DBHOST"),
			DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
			DbName:     os.Getenv("DBNAME"),
			DbUser:     os.Getenv("DBUSER"),
			DbPassword: os.Getenv("DBPASS"),
			DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
			Log:        logger,
		}
	case "s3":
		config = uvaeasystore.DatastoreS3Config{
			Bucket:     os.Getenv("BUCKET"),
			DbHost:     os.Getenv("DBHOST"),
			DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
			DbName:     os.Getenv("DBNAME"),
			DbUser:     os.Getenv("DBUSER"),
			DbPassword: os.Getenv("DBPASS"),
			DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
			Log:        logger,
		}
	default:
		log.Fatalf("ERROR: unsupported mode (%s)", mode)
	}

	es, err := uvaeasystore.NewEasyStore(config)
	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// important, cleanup properly
	defer es.Close()

	eso, err := es.GetByKey(namespace, oid, uvaeasystore.Fields)
	if err == nil {
		fields := eso.Fields()
		if oper == "add" {
			log.Printf("INFO: adding tag '%s'='%s'", name, value)
			fields[name] = value
		} else {
			log.Printf("INFO: removing tag '%s'", name)
			delete(fields, name)
		}

		eso.SetFields(fields)
		_, err = es.Update(eso, uvaeasystore.Fields)
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
