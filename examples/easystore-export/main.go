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
		log.Fatalf("ERROR: use: %s <namespace> <export dir>", os.Args[0])
	}

	namespace := os.Args[1]
	outdir := os.Args[2]

	config := uvaeasystore.DefaultEasyStoreConfig()
	// configure what we need
	config.Namespace(namespace)
	//config.Logger(log.Default())

	log.Printf("INFO: creating easystore, namespace: %s", namespace)

	esro, err := uvaeasystore.NewEasyStoreReadonly(config)
	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// empty fields means all objects
	fields := uvaeasystore.DefaultEasyStoreFields()

	// empty fields, should be all items
	iter, err := esro.GetByFields(fields, uvaeasystore.AllComponents)
	if err != nil {
		log.Fatalf("ERROR: getting objects (%s)", err.Error())
	}

	log.Printf("INFO: received %d object(s)", iter.Count())

	// use a standard serializer
	serializer := uvaeasystore.DefaultEasyStoreSerializer()

	// go through the list of objects and dump each one
	o, err := iter.Next()
	num := 0
	for err == nil {
		// create output directory
		basedir := fmt.Sprintf("%s/export-%03d", outdir, num)
		_ = os.Mkdir(basedir, 0755)

		exportObject(o, serializer, basedir)
		o, err = iter.Next()
		num++
	}

	if err == io.EOF {
		log.Printf("INFO: terminate normally")
	} else {
		log.Printf("ERROR: terminate with %s", err.Error())
	}
}

func exportObject(obj uvaeasystore.EasyStoreObject, serializer uvaeasystore.EasyStoreSerializer, outdir string) {
	log.Printf("INFO: exporting %s", obj.Id())

	// export base object
	i := serializer.ObjectSerialize(obj)
	err := outputFile(fmt.Sprintf("%s/object.json", outdir), i.(string))
	if err != nil {
		log.Fatalf("ERROR: writing file (%s)", err.Error())
	}

	// export fields if they exist
	i = serializer.FieldsSerialize(obj.Fields())
	err = outputFile(fmt.Sprintf("%s/fields.json", outdir), i.(string))
	if err != nil {
		log.Fatalf("ERROR: writing file (%s)", err.Error())
	}

	// export metadata if it exists
	if obj.Metadata() != nil {
		i = serializer.MetadataSerialize(obj.Metadata())
		err = outputFile(fmt.Sprintf("%s/metadata.json", outdir), i.(string))
		if err != nil {
			log.Fatalf("ERROR: writing file (%s)", err.Error())
		}
	}

	// export files of they exist
	for ix, f := range obj.Files() {
		i = serializer.BlobSerialize(f)
		err = outputFile(fmt.Sprintf("%s/blob-%03d.json", outdir, ix+1), i.(string))
		if err != nil {
			log.Fatalf("ERROR: writing file (%s)", err.Error())
		}
	}
}

func outputFile(name string, contents string) error {

	payloadFile, err := os.Create(name)
	if err != nil {
		return err
	}
	defer payloadFile.Close()

	// write the payload
	_, err = payloadFile.Write([]byte(contents))
	if err != nil {
		return err
	}

	return nil
}

//
// end of file
//