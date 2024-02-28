package main

import (
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

	var namespace string
	var outDir string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&namespace, "namespace", "", "Namespace to export")
	flag.StringVar(&outDir, "exportdir", "", "Export directory")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

	// configure what we need
	config := uvaeasystore.DatastoreSqliteConfig{
		DataSource: os.Getenv("SQLITEFILE"),
		Log:        logger,
	}

	//config := uvaeasystore.DatastorePostgresConfig{
	//	DbHost:     os.Getenv("DBHOST"),
	//	DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//	DbName:     os.Getenv("DBNAME"),
	//	DbUser:     os.Getenv("DBUSER"),
	//	DbPassword: os.Getenv("DBPASSWD"),
	//	DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//	//  Log:        logger,
	//}

	esro, err := uvaeasystore.NewEasyStoreReadonly(config)
	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// empty fields means all objects
	fields := uvaeasystore.DefaultEasyStoreFields()

	// empty fields, should be all items
	iter, err := esro.GetByFields(namespace, fields, uvaeasystore.AllComponents)
	if err != nil {
		log.Fatalf("ERROR: getting objects (%s)", err.Error())
	}

	log.Printf("INFO: received %d object(s)", iter.Count())

	// use a standard serializer
	serializer := uvaeasystore.DefaultEasyStoreSerializer(namespace)

	// go through the list of objects and dump each one
	o, err := iter.Next()
	num := 0
	for err == nil {
		// create output directory
		basedir := fmt.Sprintf("%s/export-%03d", outDir, num)
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
	err := outputFile(fmt.Sprintf("%s/object.json", outdir), i.([]byte))
	if err != nil {
		log.Fatalf("ERROR: writing file (%s)", err.Error())
	}

	// export fields if they exist
	i = serializer.FieldsSerialize(obj.Fields())
	err = outputFile(fmt.Sprintf("%s/fields.json", outdir), i.([]byte))
	if err != nil {
		log.Fatalf("ERROR: writing file (%s)", err.Error())
	}

	// export metadata if it exists
	if obj.Metadata() != nil {
		i = serializer.MetadataSerialize(obj.Metadata())
		err = outputFile(fmt.Sprintf("%s/metadata.json", outdir), i.([]byte))
		if err != nil {
			log.Fatalf("ERROR: writing file (%s)", err.Error())
		}
	}

	// export files of they exist
	for ix, f := range obj.Files() {
		i = serializer.BlobSerialize(f)
		err = outputFile(fmt.Sprintf("%s/blob-%03d.json", outdir, ix+1), i.([]byte))
		if err != nil {
			log.Fatalf("ERROR: writing file (%s)", err.Error())
		}
	}
}

func outputFile(name string, contents []byte) error {

	payloadFile, err := os.Create(name)
	if err != nil {
		return err
	}
	defer payloadFile.Close()

	// write the payload
	_, err = payloadFile.Write(contents)
	if err != nil {
		return err
	}

	return nil
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
