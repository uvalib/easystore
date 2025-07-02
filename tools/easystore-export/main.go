package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// main entry point
func main() {

	var mode string
	var namespace string
	var whatCmd string
	var outDir string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3, proxy")
	flag.StringVar(&namespace, "namespace", "", "namespace to export")
	flag.StringVar(&whatCmd, "what", "id", "What to export, can be 1 or more of id,fields,metadata,files")
	flag.StringVar(&outDir, "exportdir", "", "Export directory")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

	//var implConfig uvaeasystore.EasyStoreImplConfig
	var proxyConfig uvaeasystore.EasyStoreProxyConfig

	// the easystore (or the proxy)
	var esro uvaeasystore.EasyStoreReadonly
	var err error

	switch mode {
	//	case "sqlite":
	//		implConfig = uvaeasystore.DatastoreSqliteConfig{
	//			DataSource: os.Getenv("SQLITEFILE"),
	//			Log:        logger,
	//		}
	//		esro, err = uvaeasystore.NewEasyStoreReadonly(implConfig)
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
	//		esro, err = uvaeasystore.NewEasyStoreReadonly(implConfig)
	//
	//	case "s3":
	//		implConfig = uvaeasystore.DatastoreS3Config{
	//			Bucket:     os.Getenv("BUCKET"),
	//			DbHost:     os.Getenv("DBHOST"),
	//			DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//			DbName:     os.Getenv("DBNAME"),
	//			DbUser:     os.Getenv("DBUSER"),
	//			DbPassword: os.Getenv("DBPASS"),
	//			DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//			Log:        logger,
	//		}
	//		esro, err = uvaeasystore.NewEasyStoreReadonly(implConfig)

	case "proxy":
		proxyConfig = uvaeasystore.ProxyConfigImpl{
			ServiceEndpoint: os.Getenv("ESENDPOINT"),
			Log:             logger,
		}
		esro, err = uvaeasystore.NewEasyStoreProxyReadonly(proxyConfig)

	default:
		log.Fatalf("ERROR: unsupported mode (%s)", mode)
	}

	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// important, cleanup properly
	defer esro.Close()

	// what are we exporting
	what := uvaeasystore.BaseComponent
	if strings.Contains(whatCmd, "fields") {
		what += uvaeasystore.Fields
	}
	if strings.Contains(whatCmd, "metadata") {
		what += uvaeasystore.Metadata
	}
	if strings.Contains(whatCmd, "files") {
		what += uvaeasystore.Files
	}

	// empty fields means all objects
	fields := uvaeasystore.DefaultEasyStoreFields()

	// empty fields should return all items
	iter, err := esro.GetByFields(namespace, fields, what)
	if err != nil {
		log.Fatalf("ERROR: getting objects (%s)", err.Error())
	}

	log.Printf("INFO: received %d object(s)", iter.Count())

	// use a standard serializer
	serializer := uvaeasystore.DefaultEasyStoreSerializer()

	// go through the list of objects and dump each one
	o, err := iter.Next()
	count := iter.Count()
	num := 0
	errors := 0
	for err == nil {
		// create output directory
		basedir := fmt.Sprintf("%s/export-%03d", outDir, num)
		_ = os.Mkdir(basedir, 0755)

		log.Printf("INFO: exporting %s (%d of %d)", o.Id(), num+1, count)
		err = exportObject(o, serializer, basedir)
		if err != nil {
			log.Printf("ERROR: during export, continuing (%s)", err.Error())
			errors++
		}
		o, err = iter.Next()
		num++
	}

	log.Printf("INFO: terminate normally, processed %d object(s), %d error(s)", num, errors)
}

func exportObject(obj uvaeasystore.EasyStoreObject, serializer uvaeasystore.EasyStoreSerializer, outdir string) error {

	// export base object
	i := serializer.ObjectSerialize(obj)
	err := outputFile(fmt.Sprintf("%s/object.json", outdir), i.([]byte))
	if err != nil {
		//log.Printf("ERROR: writing file (%s)", err.Error())
		return err
	}

	// export fields if they exist
	i = serializer.FieldsSerialize(obj.Fields())
	err = outputFile(fmt.Sprintf("%s/fields.json", outdir), i.([]byte))
	if err != nil {
		//log.Printf("ERROR: writing file (%s)", err.Error())
		return err
	}

	// export metadata if it exists
	if obj.Metadata() != nil {
		i = serializer.MetadataSerialize(obj.Metadata())
		err = outputFile(fmt.Sprintf("%s/metadata.json", outdir), i.([]byte))
		if err != nil {
			//log.Printf("ERROR: writing file (%s)", err.Error())
			return err
		}
	}

	// export files of they exist
	for ix, f := range obj.Files() {

		// serialize the blob object
		i = serializer.BlobSerialize(f)
		err = outputFile(fmt.Sprintf("%s/blob-%03d.json", outdir, ix+1), i.([]byte))
		if err != nil {
			return err
		}

		// and stream the file locally if appropriate
		if len(f.Url()) != 0 {
			err = streamFile(fmt.Sprintf("%s/%s", outdir, f.Name()), f.Url())
			if err != nil {
				return err
			}
		}
	}

	return nil
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

func streamFile(name string, url string) error {

	start := time.Now()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// stream
	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		return err
	}

	// and write
	buf := b.Bytes()
	err = os.WriteFile(name, buf, 0644)
	if err != nil {
		return err
	}

	duration := time.Since(start)
	log.Printf("INFO: stream/written %s (elapsed %d ms)", name, duration.Milliseconds())
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
