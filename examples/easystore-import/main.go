package main

import (
	"errors"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"io"
	"log"
	"os"
	"strconv"
)

// main entry point
func main() {

	if len(os.Args) != 4 {
		log.Fatalf("ERROR: use: %s <filesystem> <namespace> <import dir>", os.Args[0])
	}

	filesystem := os.Args[1]
	namespace := os.Args[2]
	indir := os.Args[3]

	// configure what we need
	config := uvaeasystore.DatastoreSqliteConfig{
		Filesystem: filesystem,
		Namespace:  namespace,
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

	// use a standard serializer
	serializer := uvaeasystore.DefaultEasyStoreSerializer()

	ix := 0
	var obj uvaeasystore.EasyStoreObject
	for true {
		dirname := fmt.Sprintf("%s/export-%03d", indir, ix)

		// load the object
		obj, err = makeObject(serializer, dirname)
		if err != nil {
			//log.Printf("ERROR: %s", err.Error())
			break
		}

		_, err = es.Create(obj)
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

func makeObject(serializer uvaeasystore.EasyStoreSerializer, indir string) (uvaeasystore.EasyStoreObject, error) {

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
