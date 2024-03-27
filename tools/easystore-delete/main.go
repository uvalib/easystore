package main

import (
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
	var single string
	var bulk string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3")
	flag.StringVar(&single, "single", "", "Object to delete, ns/oid")
	flag.StringVar(&bulk, "bulk", "", "File containing list of objects to delete, ns/oid")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	if len(single) == 0 && len(bulk) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

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

	lines := make([]string, 0)
	if len(bulk) != 0 {
		buf, err := os.ReadFile(bulk)
		if err != nil {
			log.Fatalf("ERROR: opening file (%s)", err.Error())
		}
		lines = strings.Split(string(buf), "\n")
	} else {
		lines = append(lines, single)
	}

	success := 0
	errors := 0
	for _, l := range lines {
		if len(l) != 0 {
			parts := strings.Split(l, "/")
			if len(parts) == 2 {
				o, err := es.GetByKey(parts[0], parts[1], uvaeasystore.BaseComponent)
				if err != nil {
					log.Printf("WARNING: get %s/%s returns error (%s), continuing", parts[0], parts[1], err.Error())
					errors++
					continue
				}

				_, err = es.Delete(o, uvaeasystore.BaseComponent)
				if err != nil {
					log.Printf("WARNING: delete %s/%s returns error (%s), continuing", parts[0], parts[1], err.Error())
					errors++
					continue
				}
				log.Printf("INFO: delete %s/%s success...", parts[0], parts[1])
				success++
			}
		}
	}

	if err == nil {
		log.Printf("INFO: terminate normally, deleted %d object(s), %d error(s)", success, errors)
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
