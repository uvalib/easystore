package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/uvalib/easystore/uvaeasystore"
)

// main entry point
func main() {

	var namespace string
	var debug bool
	var verifyCache bool
	var limit int
	var logger *log.Logger

	flag.StringVar(&namespace, "namespace", "", "namespace to check")
	flag.BoolVar(&debug, "debug", false, "Log debug information")
	flag.IntVar(&limit, "limit", 0, "Check count limit, 0 is no limit")
	flag.BoolVar(&verifyCache, "verify", false, "Verify against cache")
	flag.Parse()

	if debug == true {
		logger = log.Default()
	}

	if verifyCache == true {
		log.Printf("INFO: enabled cache verify")
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

		log.Printf("INFO: checking ns/oid [%s/%s] (%d of %d)", namespace, id, ix+1, count)

		key := uvaeasystore.DataStoreKey{Namespace: namespace, ObjectId: id}

		// get the object
		eso, err := s3ds.GetObjectByKey(key, uvaeasystore.NOCACHE)
		if err != nil {
			log.Printf("ERROR: getting object from S3 datastore (%s), continuing", err.Error())
			errorCount++
			continue
		}

		// get the fields
		var fields *uvaeasystore.EasyStoreObjectFields
		fields, err = s3ds.GetFieldsByKey(key, uvaeasystore.NOCACHE)
		if err != nil {
			if errors.Is(err, uvaeasystore.ErrNotFound) == true {
				//log.Printf("INFO: no fields located for this object\n")
			} else {
				log.Printf("ERROR: getting fields from S3 datastore (%s), continuing", err.Error())
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
				log.Printf("ERROR: getting metadata from S3 datastore (%s), continuing", err.Error())
				errorCount++
				continue
			}
		} else {
			_, err := md.Payload()
			if err != nil {
				log.Printf("ERROR: getting metadata payload (%s), continuing", err.Error())
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
				log.Printf("ERROR: getting blobs from S3 datastore (%s), continuing", err.Error())
				errorCount++
				continue
			}
		} else {
			//log.Printf("INFO: %d blobs located for this object\n", len(blobs))
		}

		// do we verify the cache
		if verifyCache == true {

			//log.Printf("INFO: checking cache for this object\n")

			var esoCache uvaeasystore.EasyStoreObject
			esoCache, err = s3ds.GetObjectByKey(key, uvaeasystore.FROMCACHE)
			if err != nil {
				log.Printf("ERROR: getting cached object from S3 datastore (%s), continuing\n", err.Error())
				errorCount++
				continue
			}

			if verifyObject(eso, esoCache) == false {
				log.Printf("ERROR: cached object and S3 datastore OUT OF SYNC, continuing\n")
				errorCount++
				continue
			}

			// get the fields
			if fields != nil {
				var fieldsCache *uvaeasystore.EasyStoreObjectFields
				fieldsCache, err = s3ds.GetFieldsByKey(key, uvaeasystore.FROMCACHE)
				if err != nil {
					if errors.Is(err, uvaeasystore.ErrNotFound) == true {
						//log.Printf("INFO: no fields located for this object\n")
					} else {
						log.Printf("ERROR: getting cached fields from S3 datastore (%s), continuing", err.Error())
						errorCount++
						continue
					}
				} else {
					//log.Printf("INFO: %d fields located for this object\n", len(*fields))
				}

				if verifyFields(*fields, *fieldsCache) == false {
					log.Printf("ERROR: cached fields and S3 datastore OUT OF SYNC, continuing\n")
					errorCount++
					continue
				}
			}
		}

		log.Printf("INFO: ok")
		okCount++
	}

	log.Printf("INFO: checked %d object(s), %d ok, %d error(s)", okCount+errorCount, okCount, errorCount)
}

func getIds(namespace string, s3Store *uvaeasystore.S3Storage) ([]string, error) {

	log.Printf("INFO: getting list of stored objects (this may take a while)...")

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

func verifyObject(esoSource uvaeasystore.EasyStoreObject, esoCache uvaeasystore.EasyStoreObject) bool {

	same := true
	// silly I know
	if esoSource.Namespace() != esoCache.Namespace() {
		log.Printf("ERROR: namespace out of sync, s3 [%s], cache [%s]", esoSource.Namespace(), esoCache.Namespace())
		same = false
	}
	if esoSource.Id() != esoCache.Id() {
		log.Printf("ERROR: id out of sync, s3 [%s], cache [%s]", esoSource.Id(), esoCache.Id())
		same = false
	}

	if esoSource.VTag() != esoCache.VTag() {
		log.Printf("ERROR: vtag out of sync, s3 [%s], cache [%s]", esoSource.VTag(), esoCache.VTag())
		same = false
	}

	// FIXME
	//if esoSource.Created() != esoCache.Created() {
	//	log.Printf("ERROR: created out of sync, s3 [%s], cache [%s]\n", esoSource.Created(), esoCache.Created())
	//	same = false
	//}

	//if esoSource.Modified() != esoCache.Modified() {
	//	log.Printf("ERROR: modified out of sync, s3 [%s], cache [%s]\n", esoSource.Modified(), esoCache.Modified())
	//	same = false
	//}

	return same
}

func verifyFields(fsSource uvaeasystore.EasyStoreObjectFields, fsCache uvaeasystore.EasyStoreObjectFields) bool {
	if len(fsSource) != len(fsCache) {
		log.Printf("ERROR: field counts out of sync, s3 [%d], cache [%d]", len(fsSource), len(fsCache))
		return false
	}

	for key, value := range fsSource {
		val, ok := fsCache[key]
		if ok == false {
			log.Printf("ERROR: field in s3 does not exist in cache [%s]", key)
			return false
		}
		if val != value {
			log.Printf("ERROR: field [%s] values out of sync, s3 [%s], cache [%s]", key, value, val)
			return false
		}
	}
	return true
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
