package main

import (
	"context"
	"errors"
	"flag"
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
	var limit int
	var logger *log.Logger

	flag.StringVar(&namespace, "namespace", "", "namespace to rebuild")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.IntVar(&limit, "limit", 0, "Check count limit, 0 is no limit")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

	// create the S3 store configuration
	s3Config := uvaeasystore.DatastoreS3Config{
		Bucket:              os.Getenv("BUCKET"),
		SignerExpireMinutes: asIntWithDefault(os.Getenv("SIGNEXPIRE"), 60),
		SignerAccessKey:     os.Getenv("SIGNER_ACCESS_KEY"),
		SignerSecretKey:     os.Getenv("SIGNER_SECRET_KEY"),
		DbHost:              os.Getenv("DBHOST"),
		DbPort:              asIntWithDefault(os.Getenv("DBPORT"), 0),
		DbName:              os.Getenv("DBNAME"),
		DbUser:              os.Getenv("DBUSER"),
		DbPassword:          os.Getenv("DBPASS"),
		DbTimeout:           asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
		Log:                 logger,
	}

	// create the S3 datastore
	s3ds, err := uvaeasystore.NewDatastore(s3Config)
	if err != nil {
		log.Fatalf("ERROR: creating S3 datastore (%s)", err.Error())
	}

	// important, cleanup properly
	defer s3ds.Close()

	// we need access to the actual S3 implementation
	s3store, ok := s3ds.(*uvaeasystore.S3Storage)
	if ok == false {
		log.Fatalf("ERROR: cast failed, not an s3Storage")
	}

	// get the ID's that exist in the S3 datastore
	ids, err := getIds(namespace, s3store)
	if err != nil {
		log.Fatalf("ERROR: enumerating objects in S3 datastore (%s)", err.Error())
	}

	// for each of the objects we located
	okCount := 0
	errorCount := 0
	count := len(ids)
	for ix, id := range ids {

		if limit != 0 && ((okCount + errorCount) >= limit) {
			log.Printf("INFO: terminating after %d item(s)", limit)
			break
		}

		log.Printf("INFO: checking ns/oid [%s/%s] (%d of %d)\n", namespace, id, ix+1, count)

		key := uvaeasystore.DataStoreKey{Namespace: namespace, ObjectId: id}

		// get the object
		_, err := s3ds.GetObjectByKey(key, uvaeasystore.NOCACHE)
		if err != nil {
			log.Printf("ERROR: getting object from S3 datastore (%s), continuing\n", err.Error())
			errorCount++
			continue
		}

		// get the fields
		_, err = s3ds.GetFieldsByKey(key, uvaeasystore.NOCACHE)
		if err != nil {
			if errors.Is(err, uvaeasystore.ErrNotFound) == true {
				//log.Printf("INFO: no fields located for this object\n")
			} else {
				log.Printf("ERROR: getting fields from S3 datastore (%s), continuing\n", err.Error())
				errorCount++
				continue
			}
		} else {
			//log.Printf("INFO: %d fields located for this object\n", len(*fields))
		}

		// get the metadata
		md, err := s3ds.GetMetadataByKey(key, uvaeasystore.NOCACHE)
		if err != nil {
			if errors.Is(err, uvaeasystore.ErrNotFound) == true {
				//log.Printf("INFO: no metadata located for this object\n")
			} else {
				log.Printf("ERROR: getting metadata from S3 datastore (%s), continuing\n", err.Error())
				errorCount++
				continue
			}
		} else {
			_, err := md.Payload()
			if err != nil {
				log.Printf("ERROR: getting metadata payload (%s), continuing\n", err.Error())
			} else {
				//log.Printf("INFO: %d bytes of metadata located for this object\n", len(pl))
			}
		}

		// get the blobs
		_, err = s3ds.GetBlobsByKey(key, uvaeasystore.NOCACHE)
		if err != nil {
			if errors.Is(err, uvaeasystore.ErrNotFound) == true {
				//log.Printf("INFO: no blobs located for this object\n")
			} else {
				log.Printf("ERROR: getting blobs from S3 datastore (%s), continuing\n", err.Error())
				errorCount++
				continue
			}
		} else {
			//log.Printf("INFO: %d blobs located for this object\n", len(blobs))
		}

		log.Printf("INFO: ok\n")
		okCount++
	}

	log.Printf("INFO: checked %d object(s), %d ok, %d error(s)", okCount+errorCount, okCount, errorCount)
}

func getIds(namespace string, s3Store *uvaeasystore.S3Storage) ([]string, error) {

	log.Printf("INFO: getting list of stored objects (this may take a while)...\n")

	// query parameters
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Store.Bucket),
		Prefix: aws.String(namespace),
	}

	// create a paginator
	var limit int32 = 1000
	paginate := s3.NewListObjectsV2Paginator(s3Store.S3Client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = limit
	})

	// make the result set
	result := make([]string, 0)

	// iterate through the pages
	for paginate.HasMorePages() {

		// get the next page
		page, err := paginate.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		// Log the objects found
		//log.Printf("INFO: evaluating %d objects...\n", len(page.Contents))

		for _, o := range page.Contents {
			if strings.HasSuffix(*o.Key, uvaeasystore.S3ObjectFileName) {
				bits := strings.Split(*o.Key, "/")
				result = append(result, bits[1])
			}
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
