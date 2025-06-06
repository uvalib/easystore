package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/uvalib/easystore/uvaeasystore"
	"log"
	"os"
	"strconv"
	"strings"
)

// main entry point
func main() {

	var namespace string
	var debug bool
	var delBefore bool
	var limit int
	var logger *log.Logger

	flag.StringVar(&namespace, "namespace", "", "namespace to rebuild")
	flag.BoolVar(&delBefore, "delete", false, "Delete before adding")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.IntVar(&limit, "limit", 0, "Rebuild count limit, 0 is no limit")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

	// create S3 store configuration
	cfg := uvaeasystore.DatastoreS3Config{
		Bucket:     os.Getenv("BUCKET"),
		DbHost:     os.Getenv("DBHOST"),
		DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
		DbName:     os.Getenv("DBNAME"),
		DbUser:     os.Getenv("DBUSER"),
		DbPassword: os.Getenv("DBPASS"),
		DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
		Log:        logger,
	}

	// the S3 datastore
	ds, err := uvaeasystore.NewDatastore(cfg)
	if err != nil {
		log.Fatalf("ERROR: creating S3 datastore (%s)", err.Error())
	}

	// important, cleanup properly
	defer ds.Close()

	// we need access to the actual implementation
	s3store, ok := ds.(*uvaeasystore.S3Storage)
	if ok == false {
		log.Fatalf("ERROR: cast failed, not an s3Storage")
	}

	ids, err := getIds(namespace, s3store)
	if err != nil {
		log.Fatalf("ERROR: enumerating objects in S3 datastore (%s)", err.Error())
	}

	for _, id := range ids {
		fmt.Printf("OBJ: [%s]\n", id)
	}
}

func getIds(namespace string, s3Store *uvaeasystore.S3Storage) ([]string, error) {

	res, err := s3Store.S3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Store.Bucket),
		Prefix: aws.String(namespace),
		//Key:       aws.String("/"),
	})
	if err != nil {
		return nil, err
	}

	// make the result set
	result := make([]string, 0)
	for _, o := range res.Contents {
		if strings.HasSuffix(*o.Key, uvaeasystore.S3ObjectFileName) {
			bits := strings.Split(*o.Key, "/")
			result = append(result, bits[1])
		}
	}

	return result, nil
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
