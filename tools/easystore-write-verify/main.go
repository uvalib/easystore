package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/uvalib/easystore/uvaeasystore"
)

// main entry point
func main() {

	var mode string
	var namespace string
	var whereCmd string
	var workDir string
	var debug bool
	var logger *log.Logger

	flag.StringVar(&mode, "mode", "postgres", "Mode, s3 or proxy")
	flag.StringVar(&namespace, "namespace", "", "namespace to verify")
	flag.StringVar(&whereCmd, "where", "", "Query by field (fields:name=value)")
	flag.StringVar(&workDir, "workdir", "", "Work directory")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
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
			ServiceTimeout:  60,
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

	// query by fields
	fields := uvaeasystore.DefaultEasyStoreFields()
	if strings.Contains(whereCmd, "fields:") {
		split := strings.Split(whereCmd[7:], ",")
		for _, s := range split {
			name := strings.Split(s, "=")[0]
			value := strings.Split(s, "=")[1]
			fields[name] = value
			fmt.Printf("INFO: querying by field: %s=%s\n", name, value)
		}
	}

	// empty fields should return all items
	iter, err := esro.ObjectGetByFields(namespace, fields, uvaeasystore.AllComponents)
	if err != nil {
		log.Fatalf("ERROR: getting objects (%s)", err.Error())
	}

	log.Printf("INFO: received %d object(s)", iter.Count())

	// go through the list of objects and dump each one
	o, err := iter.Next()
	//count := iter.Count()
	num := 0
	errors := 0
	for err == nil {

		// process files if they exist
		for _, f := range o.Files() {

			// stream the file locally if appropriate
			if len(f.Url()) != 0 {
				fname := fmt.Sprintf("%s/%s-original-%s", workDir, o.Id(), f.Name())
				err = streamFile(fname, f.Url())
				if err != nil {
					log.Printf("ERROR: streaming/writing %s, continuing (%s)", fname, err.Error())
					errors++
				}
			}

			// do more stuff
		}
		o, err = iter.Next()
		num++
	}

	log.Printf("INFO: terminate normally, processed %d object(s), %d error(s)", num, errors)
}

func outputFile(name string, contents []byte) error {
	err := os.WriteFile(name, contents, 0644)
	return err
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
	err = outputFile(name, b.Bytes())
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
