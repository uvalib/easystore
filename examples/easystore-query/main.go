package main

import (
	"flag"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"log"
	"os"
	"strconv"
	"strings"
)

// main entry point
func main() {

	var whatCmd string
	var whereCmd string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&whatCmd, "what", "id", "What to query for, can be 1 or more of id,fields,metadata,files")
	flag.StringVar(&whereCmd, "where", "", "How to specify, either by object id (oid=nnnnn) or by field (field:name=value)")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

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

	// what are we querying for
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

	// create the easystore
	esro, err := uvaeasystore.NewEasyStoreReadonly(config)
	if err != nil {
		log.Fatalf("ERROR: creating easystore (%s)", err.Error())
	}

	// issue the query
	results, err := queryEasyStore(esro, what, whereCmd)
	if err != nil {
		log.Fatalf("ERROR: querying easystore (%s)", err.Error())
	}

	// process results as appropriate
	if results.Count() != 0 {
		total := results.Count()
		current := 1
		log.Printf("INFO: located %d object(s)...", total)
		var obj uvaeasystore.EasyStoreObject
		obj, err = results.Next()
		for err == nil {
			fmt.Printf("  ===> Id: %s (%d of %d)\n", obj.Id(), current, total)
			err = outputObject(obj, what)
			if err != nil {
				log.Fatalf("ERROR: outputting result object (%s)", err.Error())
			}
			obj, err = results.Next()
			current++
		}
	} else {
		log.Printf("INFO: no objects found, terminating")
	}
}

func queryEasyStore(esro uvaeasystore.EasyStoreReadonly, what uvaeasystore.EasyStoreComponents, whereCmd string) (uvaeasystore.EasyStoreObjectSet, error) {

	// query by id
	if strings.Contains(whereCmd, "oid=") {
		oid := whereCmd[4:]
		fmt.Printf("Querying by OID: %s\n", oid)
		oids := []string{oid}
		return esro.GetByIds(oids, what)
	}

	// query by fields
	fields := uvaeasystore.DefaultEasyStoreFields()
	if strings.Contains(whereCmd, "field:") {
		nv := whereCmd[6:]
		name := strings.Split(nv, "=")[0]
		value := strings.Split(nv, "=")[1]
		fmt.Printf("Querying by Field: %s=%s\n", name, value)
		fields[name] = value
	}

	// return query by fields
	return esro.GetByFields(fields, what)
}

func outputObject(obj uvaeasystore.EasyStoreObject, what uvaeasystore.EasyStoreComponents) error {

	fmt.Printf("       created: %s\n", obj.Created())
	fmt.Printf("       updated: %s\n", obj.Modified())

	if what&uvaeasystore.Fields == uvaeasystore.Fields {
		if len(obj.Fields()) != 0 {
			for n, v := range obj.Fields() {
				fmt.Printf("       field: %s=%s\n", n, v)
			}
		} else {
			fmt.Printf("       no fields\n")
		}
	}
	if what&uvaeasystore.Metadata == uvaeasystore.Metadata {
		if obj.Metadata() != nil {
			fmt.Printf("       metadata: %d bytes (%s)\n", len(obj.Metadata().Payload()), obj.Metadata().MimeType())
		} else {
			fmt.Printf("       no metadata\n")
		}
	}
	if what&uvaeasystore.Files == uvaeasystore.Files {
		if len(obj.Files()) != 0 {
			for ix, f := range obj.Files() {
				fmt.Printf("       file %d: %s, %d bytes (%s)\n", ix+1, f.Name(), len(f.Payload()), f.MimeType())
			}
		} else {
			fmt.Printf("       no files\n")
		}
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
