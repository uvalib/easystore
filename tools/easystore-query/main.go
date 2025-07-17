package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// main entry point
func main() {

	var mode string
	var namespace string
	var whatCmd string
	var whereCmd string
	var dumpDir string
	var debug bool
	var quiet bool
	var limit int
	var logger *log.Logger

	flag.StringVar(&mode, "mode", "postgres", "Mode, sqlite, postgres, s3, proxy")
	flag.StringVar(&namespace, "namespace", "", "namespace to query")
	flag.StringVar(&whatCmd, "what", "id", "What to query for, can be 1 or more of id,fields,metadata,files")
	flag.StringVar(&whereCmd, "where", "", "How to specify, either by object id (oid=nnnnn) or by field (field:name=value)")
	flag.StringVar(&dumpDir, "dumpdir", "", "Directory to dump files and/or metadata")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.BoolVar(&quiet, "quiet", false, "Quiet mode")
	flag.IntVar(&limit, "limit", 0, "Query count limit, 0 is no limit")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

	var implConfig uvaeasystore.EasyStoreImplConfig
	var proxyConfig uvaeasystore.EasyStoreProxyConfig

	// the easystore (or the proxy)
	var esro uvaeasystore.EasyStoreReadonly
	var err error

	switch mode {
	//case "sqlite":
	//	implConfig = uvaeasystore.DatastoreSqliteConfig{
	//		DataSource: os.Getenv("SQLITEFILE"),
	//		Log:        logger,
	//	}
	//	esro, err = uvaeasystore.NewEasyStoreReadonly(implConfig)

	//case "postgres":
	//	implConfig = uvaeasystore.DatastorePostgresConfig{
	//		DbHost:     os.Getenv("DBHOST"),
	//		DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//		DbName:     os.Getenv("DBNAME"),
	//		DbUser:     os.Getenv("DBUSER"),
	//		DbPassword: os.Getenv("DBPASS"),
	//		DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//		Log:        logger,
	//	}
	//	esro, err = uvaeasystore.NewEasyStoreReadonly(implConfig)

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
		esro, err = uvaeasystore.NewEasyStoreReadonly(implConfig)

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

	// issue the query
	start := time.Now()
	results, err := queryEasyStore(namespace, esro, what, whereCmd)
	if err != nil {
		log.Fatalf("ERROR: querying easystore (%s)", err.Error())
	}
	queryDuration := time.Since(start)

	// process results as appropriate
	if results.Count() != 0 {
		total := results.Count()
		current := 1
		log.Printf("INFO: located %d object(s)...", total)
		var obj uvaeasystore.EasyStoreObject
		obj, err = results.Next()
		for err == nil {
			if quiet == false {
				fmt.Printf("  ===> ns/id: %s/%s (%d of %d)\n", obj.Namespace(), obj.Id(), current, total)
				err = outputObject(obj, what)
				if err != nil {
					log.Fatalf("ERROR: outputting result object (%s)", err.Error())
				}
			}
			err = dumpObject(obj, dumpDir)
			if err != nil {
				log.Fatalf("ERROR: dumping result object (%s)", err.Error())
			}

			obj, err = results.Next()
			current++
			if limit > 0 && current > limit {
				log.Printf("INFO: terminating at %d object(s)...", limit)
				break
			}
		}
		totalDuration := time.Since(start)

		log.Printf("INFO: query time %0.2f seconds", queryDuration.Seconds())
		log.Printf("INFO: %d results in %0.2f seconds", results.Count(), totalDuration.Seconds())
	} else {
		log.Printf("INFO: no objects found, terminating")
	}
}

func queryEasyStore(namespace string, esro uvaeasystore.EasyStoreReadonly, what uvaeasystore.EasyStoreComponents, whereCmd string) (uvaeasystore.EasyStoreObjectSet, error) {

	// query by id
	if strings.Contains(whereCmd, "oid=") {
		oid := whereCmd[4:]
		fmt.Printf("Querying by OID: %s\n", oid)
		oids := []string{oid}
		return esro.ObjectGetByKeys(namespace, oids, what)
	}

	// query by fields
	fields := uvaeasystore.DefaultEasyStoreFields()
	if strings.Contains(whereCmd, "fields:") {
		split := strings.Split(whereCmd[7:], ",")
		for _, s := range split {
			name := strings.Split(s, "=")[0]
			value := strings.Split(s, "=")[1]
			fields[name] = value
			fmt.Printf("Querying by Field: %s=%s\n", name, value)
		}
	}

	// return query by fields
	return esro.ObjectGetByFields(namespace, fields, what)
}

func outputObject(obj uvaeasystore.EasyStoreObject, what uvaeasystore.EasyStoreComponents) error {

	fmt.Printf("       vtag:    %s\n", obj.VTag())
	fmt.Printf("       created: %s\n", obj.Created())
	fmt.Printf("       updated: %s\n", obj.Modified())

	if what&uvaeasystore.Fields == uvaeasystore.Fields {
		if len(obj.Fields()) != 0 {
			// output our fields in sorted order
			keys := make([]string, 0, len(obj.Fields()))
			for k := range obj.Fields() {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("       field: %s=%s\n", k, obj.Fields()[k])
			}
		} else {
			fmt.Printf("       no fields\n")
		}
	}
	if what&uvaeasystore.Metadata == uvaeasystore.Metadata {
		if obj.Metadata() != nil {
			b, err := obj.Metadata().Payload()
			if err != nil {
				fmt.Printf("       metadata: payload access error (%s)\n", err)
			} else {
				fmt.Printf("       metadata: %d bytes (%s)\n", len(b), obj.Metadata().MimeType())
			}
		} else {
			fmt.Printf("       no metadata\n")
		}
	}
	if what&uvaeasystore.Files == uvaeasystore.Files {
		if len(obj.Files()) != 0 {
			for ix, f := range obj.Files() {
				// check for a streaming URL
				if len(f.Url()) != 0 {
					fmt.Printf("       file %d: %s, url %s (%s)\n", ix+1, f.Name(), f.Url(), f.MimeType())
				} else {
					b, err := f.Payload()
					if err != nil {
						fmt.Printf("       file: payload access error (%s)\n", err)
					} else {
						fmt.Printf("       file %d: %s, %d bytes (%s)\n", ix+1, f.Name(), len(b), f.MimeType())
					}
				}
			}
		} else {
			fmt.Printf("       no files\n")
		}
	}

	return nil
}

func dumpObject(obj uvaeasystore.EasyStoreObject, outdir string) error {

	if len(outdir) == 0 {
		return nil
	}

	// dump metadata if it exists
	if obj.Metadata() != nil {
		buf, err := obj.Metadata().Payload()
		if err != nil {
			return err
		}
		fname := fmt.Sprintf("%s/%s-%s-metadata.bin", outdir, obj.Namespace(), obj.Id())
		fmt.Printf("       ==> writing %s...\n", fname)
		err = os.WriteFile(fname, buf, 0644)
		if err != nil {
			return err
		}
	}

	// dump files if they exist
	if obj.Files() != nil {
		for _, f := range obj.Files() {
			var buf []byte
			var err error

			// check for a streaming URL
			if len(f.Url()) != 0 {
				fmt.Printf("       ==> streaming %s...\n", f.Url())

				resp, err := http.Get(f.Url())
				if err != nil {
					return err
				}
				defer resp.Body.Close()

				// Check response
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("bad status: %s", resp.Status)
				}

				// Write
				var b bytes.Buffer
				writer := bufio.NewWriter(&b)
				_, err = io.Copy(writer, resp.Body)
				if err != nil {
					return err
				}
				buf = b.Bytes()
			} else {
				buf, err = f.Payload()
				if err != nil {
					return err
				}
			}
			fname := fmt.Sprintf("%s/%s-%s-%s", outdir, obj.Namespace(), obj.Id(), f.Name())
			fmt.Printf("       ==> writing %s...\n", fname)
			err = os.WriteFile(fname, buf, 0644)
			if err != nil {
				return err
			}
		}
	}

	if obj.Fields() != nil {

		b, err := json.Marshal(obj.Fields())
		if err != nil {
			return err
		}

		fname := fmt.Sprintf("%s/%s-%s-fields.bin", outdir, obj.Namespace(), obj.Id())
		fmt.Printf("       ==> writing %s...\n", fname)
		err = os.WriteFile(fname, b, 0644)
		if err != nil {
			return err
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
