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

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3, proxy")
	flag.StringVar(&id, "identifier", "", "Object to change, ns/oid")
	flag.StringVar(&oper, "operation", "add", "Tag operation, add|del")
	flag.StringVar(&name, "name", "", "Field name")
	flag.StringVar(&value, "value", "", "Field value (add only)")
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

	var implConfig uvaeasystore.EasyStoreImplConfig
	var proxyConfig uvaeasystore.EasyStoreProxyConfig

	// the easystore (or the proxy)
	var es uvaeasystore.EasyStore
	var err error

	switch mode {
	//	case "sqlite":
	//		implConfig = uvaeasystore.DatastoreSqliteConfig{
	//			DataSource: os.Getenv("SQLITEFILE"),
	//			Log:        logger,
	//		}
	//		es, err = uvaeasystore.NewEasyStore(implConfig)
	//
	//	case "postgres":
	//		implConfig = uvaeasystore.DatastorePostgresConfig{
	//			DbHost:     os.Getenv("DBHOST"),
	//			DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//			DbName:     os.Getenv("DBNAME"),
	//			DbUser:     os.Getenv("DBUSER"),
	//			DbPassword: os.Getenv("DBPASS"),
	//			DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//			Log:        logger,
	//		}
	//		es, err = uvaeasystore.NewEasyStore(implConfig)

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

	eso, err := es.ObjectGetByKey(namespace, oid, uvaeasystore.Fields)
	if err == nil {
		fields := eso.Fields()
		if oper == "add" {
			log.Printf("INFO: adding field '%s'='%s'", name, value)
			fields[name] = value
		} else {
			log.Printf("INFO: removing field '%s'", name)
			delete(fields, name)
		}

		eso.SetFields(fields)
		_, err = es.ObjectUpdate(eso, uvaeasystore.Fields)
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
