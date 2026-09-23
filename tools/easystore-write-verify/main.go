package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

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
	var es uvaeasystore.EasyStore
	var err error

	switch mode {
	case "s3":
		implConfig = &uvaeasystore.DatastoreS3Config{
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
		proxyConfig = &uvaeasystore.ProxyConfigImpl{
			ServiceEndpoint: os.Getenv("ESENDPOINT"),
			ServiceTimeout:  60,
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
	iter, err := es.ObjectGetByFields(namespace, fields, uvaeasystore.AllComponents)
	if err != nil {
		log.Fatalf("ERROR: getting objects (%s)", err.Error())
	}

	log.Printf("INFO: received %d object(s)", iter.Count())

	// go through the list of objects and process each one
	o, err := iter.Next()
	count := iter.Count()
	num := 0
	errors := 0
	for err == nil {

		// download the files
		originalNames := make([]string, 0)
		originalFiles := make([]string, 0)
		for _, f := range o.Files() {

			// stream the file locally if appropriate
			if len(f.Url()) != 0 {
				fname := fmt.Sprintf("%s/%s-original-%s", workDir, o.Id(), f.Name())
				err = streamFile(fname, f.Url())
				if err != nil {
					log.Printf("ERROR: streaming/writing %s/%s (original), continuing (%s)", fname, o.Id(), err.Error())
					errors++
					continue
				}
				originalNames = append(originalNames, f.Name())
				originalFiles = append(originalFiles, fname)
			}
		}

		// create our new work object
		// we really don't want to use the namespace that then lambda's trigger on
		oNew := uvaeasystore.NewEasyStoreObject("test-namespace", "")

		// populate it
		oNew.SetFiles(fileStreams(originalNames, originalFiles))

		// and commit it
		_, err = es.ObjectCreate(oNew)
		if err != nil {
			log.Printf("ERROR: creating copy object, continuing (%s)", err.Error())
			errors++
			continue
		}

		// get the new object
		oNew, err = es.ObjectGetByKey(namespace, oNew.Id(), uvaeasystore.Files)
		if err != nil {
			log.Printf("ERROR: getting copy object, continuing (%s)", err.Error())
			errors++
			continue
		}

		// download the copy files
		copyFiles := make([]string, 0)
		for _, f := range oNew.Files() {

			// stream the file locally if appropriate
			if len(f.Url()) != 0 {
				fname := fmt.Sprintf("%s/%s-copy-%s", workDir, oNew.Id(), f.Name())
				err = streamFile(fname, f.Url())
				if err != nil {
					log.Printf("ERROR: streaming/writing %s/%s (copy), continuing (%s)", fname, err.Error())
					errors++
					continue
				}
				copyFiles = append(copyFiles, fname)
			}
		}

		// verify the original and copy files are identical
		err = verifyFiles(originalFiles, copyFiles)
		if err != nil {
			errors++
			continue
		}

		// remove the copy object
		_, err = es.ObjectDelete(oNew, uvaeasystore.AllComponents)
		if err != nil {
			log.Printf("ERROR: deleting sample object, continuing (%s)", err.Error())
			errors++
			continue
		}

		// delete the files if the operation was successful
		deleteFiles(originalFiles)
		deleteFiles(copyFiles)

		log.Printf("INFO: processed %d of %d objects successfully", num+1, count)
		o, err = iter.Next()
		num++
	}

	log.Printf("INFO: terminate normally, processed %d object(s), %d error(s)", num, errors)
}

func fileStreams(names []string, files []string) []uvaeasystore.EasyStoreBlob {
	var fs []uvaeasystore.EasyStoreBlob
	for ix, f := range files {
		b, err := uvaeasystore.NewEasyStoreBlobFromFile(names[ix], "", f)
		if err == nil {
			fs = append(fs, b)
		}
	}
	return fs
}

func verifyFiles(originalFiles []string, copyFiles []string) error {

	if len(originalFiles) != len(copyFiles) {
		return fmt.Errorf("number of original and copy files not equal")
	}

	for ix, f := range originalFiles {

		hashOriginal, err := sha1sum(f)
		if err != nil {
			return err
		}

		hashCopy, err := sha1sum(copyFiles[ix])
		if err != nil {
			return err
		}

		if hashOriginal != hashCopy {
			return fmt.Errorf("hashes not equal for %s and %s", f, copyFiles[ix])
		}
	}

	return nil
}

func streamFile(name string, url string) error {

	//start := time.Now()

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

	//duration := time.Since(start)
	//log.Printf("INFO: stream/written %s (elapsed %d ms)", name, duration.Milliseconds())
	return nil
}

func outputFile(name string, contents []byte) error {
	err := os.WriteFile(name, contents, 0644)
	return err
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

func sha1sum(fileName string) (string, error) {

	file, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha1.New()

	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	//log.Printf("DEBUG: sha1sum %s = %s", fileName, hashString)
	return hashString, nil
}

func deleteFiles(files []string) {

	for _, f := range files {
		_ = os.Remove(f)
	}
}

//
// end of file
//
