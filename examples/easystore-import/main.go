package main

import (
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"io"
	"log"
	"os"
)

// main entry point
func main() {

	if len(os.Args) != 3 {
		log.Fatalf("ERROR: use: %s <namespace> <import dir>", os.Args[0])
	}

	namespace := os.Args[1]
	indir := os.Args[2]

	config := uvaeasystore.DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace(namespace)
	//config.Logger(log.Default())

	log.Printf("INFO: creating easystore, namespace: %s", namespace)

	es, err := uvaeasystore.NewEasyStore(config)
	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// use a standard serializer
	serializer := uvaeasystore.DefaultEasyStoreSerializer()

	ix := 0
	for true {
		dirname := fmt.Sprintf("%s/export-%03d", indir, ix)

		// load the object
		obj, err := makeObject(serializer, dirname)
		if err != nil {
			break
		}

		_, err = es.Create(obj)
		if err != nil {
			break
		}

		ix++
	}

	if err == io.EOF {
		log.Printf("INFO: terminate normally, imported %d objects", ix)
	} else {
		log.Printf("ERROR: terminate with %s", err.Error())
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
	if err != nil {
		log.Fatalf("ERROR: reading file (%s)", err.Error())
	} else {
		_, err := serializer.FieldsDeserialize(buf)
		if err != nil {
			log.Fatalf("ERROR: deserializing fields (%s)", err.Error())
		}
	}

	// export metadata if it exists
	//	if obj.Metadata() != nil {
	//		i = serializer.MetadataSerialize(obj.Metadata())
	//		err = os.ReadFile(fmt.Sprintf("%s/metadata.json", outdir), i.(string))
	//		if err != nil {
	//			log.Fatalf("ERROR: writing file (%s)", err.Error())
	//		}
	//	}
	//
	//	// export files of they exist
	//	for ix, f := range obj.Files() {
	//		i = serializer.BlobSerialize(f)
	//		err = os.ReadFile(fmt.Sprintf("%s/blob-%03d.json", outdir, ix+1), i.(string))
	//		if err != nil {
	//			log.Fatalf("ERROR: writing file (%s)", err.Error())
	//		}
	//	}

	return obj, nil
}

//
// end of file
//
