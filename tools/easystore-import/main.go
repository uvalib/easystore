package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"io"
	"log"
	"os"
	"strconv"
)

// main entry point
func main() {

	var mode string
	var namespace string
	var inDir string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3, proxy")
	flag.StringVar(&namespace, "namespace", "", "namespace to import")
	flag.StringVar(&inDir, "importdir", "", "Import directory")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

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

	// use a standard serializer
	serializer := uvaeasystore.DefaultEasyStoreSerializer()

	ix := 0
	var obj uvaeasystore.EasyStoreObject
	for true {
		dirname := fmt.Sprintf("%s/export-%03d", inDir, ix)

		// load the object
		obj, err = makeObject(serializer, dirname, namespace)
		if err != nil {
			//log.Printf("ERROR: %s", err.Error())
			break
		}

		_, err = es.ObjectCreate(obj)
		if err != nil {
			//log.Printf("ERROR: %s", err.Error())
			break
		}

		ix++
	}

	if err == nil || err == io.EOF {
		log.Printf("INFO: terminate normally, imported %d objects", ix)
	} else {
		log.Printf("ERROR: terminate with '%s'", err.Error())
	}
}

func makeObject(serializer uvaeasystore.EasyStoreSerializer, indir string, namespace string) (uvaeasystore.EasyStoreObject, error) {

	log.Printf("INFO: importing from %s", indir)

	// return if the import directory does not exist
	_, err := os.Stat(indir)
	if err != nil {
		return nil, io.EOF
	}

	// import base object
	buf, err := os.ReadFile(fmt.Sprintf("%s/object.json", indir))
	if err != nil {
		log.Fatalf("ERROR: reading file (%s)", err.Error())
	}
	obj, err := serializer.ObjectDeserialize(buf)
	if err != nil {
		log.Fatalf("ERROR: deserializing object (%s)", err.Error())
	}

	// import fields if they exist
	buf, err = os.ReadFile(fmt.Sprintf("%s/fields.json", indir))
	if err == nil {
		fields, err := serializer.FieldsDeserialize(buf)
		if err == nil {
			obj.SetFields(fields)
			log.Printf("DEBUG: ==> imported fields for [%s]", obj.Id())
		} else {
			log.Fatalf("ERROR: deserializing fields (%s)", err.Error())
		}

	} else {
		if errors.Is(err, os.ErrNotExist) == true {
			//log.Printf("DEBUG: no fields for [%s]", obj.Id())
		} else {
			log.Fatalf("ERROR: loading fields file (%s)", err.Error())
		}
	}

	// import metadata if it exists
	buf, err = os.ReadFile(fmt.Sprintf("%s/metadata.json", indir))
	if err == nil {
		metadata, err := serializer.MetadataDeserialize(buf)
		if err == nil {
			obj.SetMetadata(metadata)
			log.Printf("DEBUG: ==> imported metadata for [%s]", obj.Id())
		} else {
			log.Fatalf("ERROR: deserializing metadata (%s)", err.Error())
		}
	} else {
		if errors.Is(err, os.ErrNotExist) == true {
			//log.Printf("DEBUG: no metadata for [%s]", obj.Id())
		} else {
			log.Fatalf("ERROR: loading metadata file (%s)", err.Error())
		}
	}

	// import files if they exist
	buf, err = os.ReadFile(fmt.Sprintf("%s/blob-001.json", indir))
	if err == nil {

		// for each possible blob file
		blobs := make([]uvaeasystore.EasyStoreBlob, 0)
		ix := 0
		var blob uvaeasystore.EasyStoreBlob
		buf, err = os.ReadFile(fmt.Sprintf("%s/blob-%03d.json", indir, ix+1))
		for err == nil {

			blob, err = serializer.BlobDeserialize(buf)
			if err != nil {
				log.Fatalf("ERROR: deserializing blob (%s)", err.Error())
			}

			blobs = append(blobs, blob)
			ix++
			buf, err = os.ReadFile(fmt.Sprintf("%s/blob-%03d.json", indir, ix+1))
		}
		if errors.Is(err, os.ErrNotExist) == true {
			obj.SetFiles(blobs)
			log.Printf("DEBUG: ==> imported %d blob(s) for [%s]", ix, obj.Id())
		} else {
			log.Fatalf("ERROR: loading blob file(s) (%s)", err.Error())
		}
	} else {
		if errors.Is(err, os.ErrNotExist) == true {
			//log.Printf("DEBUG: no blobs for [%s]", obj.Id())
		} else {
			log.Fatalf("ERROR: loading blob file(s) (%s)", err.Error())
		}
	}

	// update the namespace so we import into the correct one
	obj.SetNamespace(namespace)

	return obj, nil
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
