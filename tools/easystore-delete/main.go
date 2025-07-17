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

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3, proxy")
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

	var o uvaeasystore.EasyStoreObject
	success := 0
	errors := 0

	for _, l := range lines {
		if len(l) != 0 {
			parts := strings.Split(l, "/")
			if len(parts) == 2 {
				o, err = es.ObjectGetByKey(parts[0], parts[1], uvaeasystore.BaseComponent)
				if err != nil {
					log.Printf("WARNING: get %s/%s returns error (%s), continuing", parts[0], parts[1], err.Error())
					errors++
					continue
				}

				_, err = es.ObjectDelete(o, uvaeasystore.BaseComponent)
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
