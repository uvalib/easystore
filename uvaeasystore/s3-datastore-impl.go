//
//
//

package uvaeasystore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/exp/maps"
	"log"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

var S3ObjectFileName = "object.json"
var S3FieldsFileName = "fields.json"
var S3MetadataFileName = "metadata.json"
var S3BlobFileNameSuffix = "-es.json"

// this is our S3 implementation
type S3Storage struct {
	Bucket          string              // Bucket name
	signerAccessKey string              // signing key
	signerSecretKey string              // signing secret
	serialize       EasyStoreSerializer // standard serializer
	S3Client        *s3.Client          // the s3 client
	s3SignClient    *s3.PresignClient   // the signing client (creates signed access urls)
	log             *log.Logger         // logger
	*sql.DB                             // database connection
}

// Check -- check our database health
func (s *S3Storage) Check() error {

	// perhaps check Bucket access too?

	return s.Ping()
}

// UpdateObject -- update a couple of object fields
func (s *S3Storage) UpdateObject(key DataStoreKey) error {
	obj, err := s.GetObjectByKey(key)
	if err != nil {
		return err
	}

	impl, ok := obj.(*easyStoreObjectImpl)
	if ok == false {
		return fmt.Errorf("%q: %w", "cast failed, not an easyStoreObjectImpl", ErrBadParameter)
	}

	impl.Vtag_ = newVtag()
	impl.Modified_ = time.Now()

	stmt, err := s.Prepare("UPDATE objects set vtag = $1, updated_at = NOW() WHERE namespace = $2 AND oid = $3")
	if err != nil {
		return err
	}
	err = execPreparedBy3(stmt, impl.Vtag_, key.Namespace, key.ObjectId)
	if err != nil {
		return err
	}

	return s.addObject(impl.Namespace(), impl.Id(), impl)
}

// AddBlob -- add a new blob object
func (s *S3Storage) AddBlob(key DataStoreKey, blob EasyStoreBlob) error {
	// check asset does not exist
	if s.checkExists(key.Namespace, key.ObjectId, fmt.Sprintf("%s%s", blob.Name(), S3BlobFileNameSuffix)) == true {
		return ErrAlreadyExists
	}
	return s.addBlob(key.Namespace, key.ObjectId, blob)
}

// AddFields -- add a new fields object
func (s *S3Storage) AddFields(key DataStoreKey, fields EasyStoreObjectFields) error {
	// check asset does not exist
	//if s.checkExists(key.Namespace_, key.objectId, S3FieldsFileName) == true {
	//	return ErrAlreadyExists
	//}

	stmt, err := s.Prepare("INSERT INTO fields( namespace, oid, name, value ) VALUES( $1,$2,$3,$4 )")
	if err != nil {
		return err
	}

	for n, v := range fields {
		_, err = stmt.Exec(key.Namespace, key.ObjectId, n, v)
		if err != nil {
			return errorMapper(err)
		}
	}

	return s.addFields(key.Namespace, key.ObjectId, fields)
}

// AddMetadata -- add a new metadata object
func (s *S3Storage) AddMetadata(key DataStoreKey, metadata EasyStoreMetadata) error {
	// check asset does not exist
	if s.checkExists(key.Namespace, key.ObjectId, S3MetadataFileName) == true {
		return ErrAlreadyExists
	}
	return s.addMetadata(key.Namespace, key.ObjectId, metadata)
}

// AddObject -- add a new object
func (s *S3Storage) AddObject(obj EasyStoreObject) error {
	// check asset does not exist
	//if s.checkExists(obj.Namespace_(), obj.Id(), S3ObjectFileName) == true {
	//	return ErrAlreadyExists
	//}

	// update the database
	stmt, err := s.Prepare("INSERT INTO objects( namespace, oid, vtag ) VALUES( $1,$2,$3 )")
	if err != nil {
		return err
	}
	err = execPreparedBy3(stmt, obj.Namespace(), obj.Id(), obj.VTag())
	if err != nil {
		return err
	}

	return s.addObject(obj.Namespace(), obj.Id(), obj)
}

// GetBlobsByKey -- get all blob data associated with the specified object
func (s *S3Storage) GetBlobsByKey(key DataStoreKey) ([]EasyStoreBlob, error) {
	fset, err := s.s3List(s.Bucket, fmt.Sprintf("%s/%s", key.Namespace, key.ObjectId))
	if err != nil {
		return nil, err
	}
	res := make([]EasyStoreBlob, 0)
	for _, fname := range fset {
		bname := filepath.Base(fname)
		if s.isBlobName(bname) == true {
			blob, err := s.getBlob(fname)
			if err != nil {
				return nil, err
			}
			res = append(res, *blob)
		}
	}

	// no blobs
	if len(res) == 0 {
		return nil, ErrNotFound
	}
	return res, nil
}

// GetFieldsByKey -- get all field data associated with the specified object
func (s *S3Storage) GetFieldsByKey(key DataStoreKey) (*EasyStoreObjectFields, error) {
	// check asset exists
	if s.checkExists(key.Namespace, key.ObjectId, S3FieldsFileName) == false {
		return nil, ErrNotFound
	}
	return s.getFields(key.Namespace, key.ObjectId)
}

// GetMetadataByKey -- get all field data associated with the specified object
func (s *S3Storage) GetMetadataByKey(key DataStoreKey) (EasyStoreMetadata, error) {
	// check asset exists
	if s.checkExists(key.Namespace, key.ObjectId, S3MetadataFileName) == false {
		return nil, ErrNotFound
	}
	return s.getMetadata(key.Namespace, key.ObjectId)
}

// GetObjectByKey -- get all field data associated with the specified object
func (s *S3Storage) GetObjectByKey(key DataStoreKey) (EasyStoreObject, error) {
	// check asset exists
	if s.checkExists(key.Namespace, key.ObjectId, S3ObjectFileName) == false {
		return nil, ErrNotFound
	}
	return s.getObject(key.Namespace, key.ObjectId)
}

// GetObjectsByKey -- get all field data associated with the specified object
func (s *S3Storage) GetObjectsByKey(keys []DataStoreKey) ([]EasyStoreObject, error) {

	results := make([]EasyStoreObject, 0, len(keys))
	for _, key := range keys {
		if s.checkExists(key.Namespace, key.ObjectId, S3ObjectFileName) == true {
			obj, err := s.getObject(key.Namespace, key.ObjectId)
			if err != nil {
				return nil, err
			}
			results = append(results, obj)
		}
	}
	if len(results) == 0 {
		return nil, ErrNotFound
	}
	return results, nil
}

// DeleteBlobsByKey -- delete all blob data associated with the specified object
func (s *S3Storage) DeleteBlobsByKey(key DataStoreKey) error {
	fset, err := s.s3List(s.Bucket, fmt.Sprintf("%s/%s", key.Namespace, key.ObjectId))
	if err != nil {
		return err
	}
	for _, fname := range fset {
		bname := filepath.Base(fname)
		if s.isBlobName(bname) == true {
			err = s.s3Remove(s.Bucket, fname)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteFieldsByKey -- delete all field data associated with the specified object
func (s *S3Storage) DeleteFieldsByKey(key DataStoreKey) error {

	stmt, err := s.Prepare("DELETE FROM fields WHERE namespace = $1 AND oid = $2")
	if err != nil {
		return err
	}
	err = execPreparedBy2(stmt, key.Namespace, key.ObjectId)
	if err != nil {
		return err
	}

	return s.removeAsset(key.Namespace, key.ObjectId, S3FieldsFileName)
}

// DeleteMetadataByKey -- delete all field data associated with the specified object
func (s *S3Storage) DeleteMetadataByKey(key DataStoreKey) error {
	return s.removeAsset(key.Namespace, key.ObjectId, S3MetadataFileName)
}

// DeleteObjectByKey -- delete all field data associated with the specified object
func (s *S3Storage) DeleteObjectByKey(key DataStoreKey) error {

	stmt, err := s.Prepare("DELETE FROM objects WHERE namespace = $1 AND oid = $2")
	if err != nil {
		return err
	}
	err = execPreparedBy2(stmt, key.Namespace, key.ObjectId)
	if err != nil {
		return err
	}

	return s.removeAsset(key.Namespace, key.ObjectId, S3ObjectFileName)
}

// GetKeysByFields -- get a list of keys that have the supplied fields/values
func (s *S3Storage) GetKeysByFields(namespace string, fields EasyStoreObjectFields) ([]DataStoreKey, error) {
	var err error
	var rows *sql.Rows
	var query string
	//
	// support the following cases:
	// empty namespace (all namespaces) or specified namespace
	// no fields (all objects) or variable set of fields
	//
	if len(fields) == 0 {
		if len(namespace) == 0 {
			query = "SELECT namespace, oid, 0 FROM objects ORDER BY namespace, oid"
			rows, err = s.Query(query)
		} else {
			query = "SELECT namespace, oid, 0 FROM objects where namespace = $1 ORDER BY namespace, oid"
			rows, err = s.Query(query, namespace)
		}
	} else {
		// dynamically build the query because we have a variable number of fields
		args := make([]any, 0)
		query = "SELECT namespace, oid, count(*) FROM fields WHERE "
		variableIx := 1
		if len(namespace) != 0 {
			query += fmt.Sprintf("namespace = $%d AND ", variableIx)
			args = append(args, namespace)
			variableIx++
		}

		for ix, k := range maps.Keys(fields) {
			query += fmt.Sprintf("(name = $%d AND value = $%d) ", variableIx, variableIx+1)
			variableIx += 2
			args = append(args, k, fields[k])
			if ix != (len(fields) - 1) {
				query += "OR "
			}
		}

		query += fmt.Sprintf("GROUP BY namespace, oid HAVING count(*) = %d", len(fields))
		rows, err = s.Query(query, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return keyQueryResults(rows, s.log)
}

//
// private implementation methods
//

func (s *S3Storage) checkExists(namespace string, identifier string, assetName string) bool {
	key := s.assetKey(namespace, identifier, assetName)
	return s.s3Exists(s.Bucket, key)
}

func (s *S3Storage) removeAsset(namespace string, identifier string, assetName string) error {
	key := s.assetKey(namespace, identifier, assetName)
	return s.s3Remove(s.Bucket, key)
}

func (s *S3Storage) addBlob(namespace string, identifier string, blob EasyStoreBlob) error {

	// we add the serialized blob and create the original file
	blobKey := s.assetKey(namespace, identifier, fmt.Sprintf("%s%s", blob.Name(), S3BlobFileNameSuffix))
	fileKey := s.assetKey(namespace, identifier, blob.Name())

	// for setting the timestamps
	impl, ok := blob.(*easyStoreBlobImpl)
	if ok == false {
		return fmt.Errorf("%q: %w", "cast failed, not an easyStoreBlobImpl", ErrBadParameter)
	}
	impl.Created_, impl.Modified_ = time.Now(), time.Now()

	// we want to store as the original file rather than a serialized byte stream...
	fBytes := impl.Payload_
	// upload to S3
	err := s.s3UploadFromBuffer(s.Bucket, fileKey, fBytes)
	if err != nil {
		return err
	}

	// dont want to serialize the payload
	impl.Payload_ = nil
	bBytes := s.serialize.BlobSerialize(impl).([]byte)

	// upload to S3
	return s.s3UploadFromBuffer(s.Bucket, blobKey, bBytes)
}

func (s *S3Storage) addFields(namespace string, identifier string, fields EasyStoreObjectFields) error {
	key := s.assetKey(namespace, identifier, S3FieldsFileName)
	b := s.serialize.FieldsSerialize(fields).([]byte)
	// upload to S3
	return s.s3UploadFromBuffer(s.Bucket, key, b)
}

func (s *S3Storage) addMetadata(namespace string, identifier string, metadata EasyStoreMetadata) error {
	key := s.assetKey(namespace, identifier, S3MetadataFileName)

	// for setting the timestamps
	impl, ok := metadata.(*easyStoreMetadataImpl)
	if ok == false {
		return fmt.Errorf("%q: %w", "cast failed, not an easyStoreMetadataImpl", ErrBadParameter)
	}
	impl.Created_, impl.Modified_ = time.Now(), time.Now()

	b := s.serialize.MetadataSerialize(impl).([]byte)
	// upload to S3
	return s.s3UploadFromBuffer(s.Bucket, key, b)
}

func (s *S3Storage) addObject(namespace string, identifier string, obj EasyStoreObject) error {
	key := s.assetKey(namespace, identifier, S3ObjectFileName)

	// for setting the timestamps
	impl, ok := obj.(*easyStoreObjectImpl)
	if ok == false {
		return fmt.Errorf("%q: %w", "cast failed, not an easyStoreObjectImpl", ErrBadParameter)
	}
	impl.Created_, impl.Modified_ = time.Now(), time.Now()

	b := s.serialize.ObjectSerialize(impl).([]byte)
	// upload to S3
	return s.s3UploadFromBuffer(s.Bucket, key, b)
}

func (s *S3Storage) getBlob(key string) (*EasyStoreBlob, error) {
	// download from S3
	b, err := s.s3DownloadToBuffer(s.Bucket, key)
	if err != nil {
		return nil, err
	}
	blob, err := s.serialize.BlobDeserialize(b)
	if err != nil {
		return nil, err
	}

	// if the payload is empty, we check for the original file
	pl, err := blob.Payload()
	if pl == nil || len(pl) == 0 {
		// for setting the timestamps
		impl, ok := blob.(*easyStoreBlobImpl)
		if ok == false {
			return nil, fmt.Errorf("%q: %w", "cast failed, not an easyStoreBlobImpl", ErrBadParameter)
		}
		impl.Url_, err = s.signedUrl(s.Bucket, strings.TrimSuffix(key, S3BlobFileNameSuffix))
	}

	return &blob, nil
}

func (s *S3Storage) getFields(namespace string, identifier string) (*EasyStoreObjectFields, error) {
	key := s.assetKey(namespace, identifier, S3FieldsFileName)

	// download from S3
	b, err := s.s3DownloadToBuffer(s.Bucket, key)
	if err != nil {
		return nil, err
	}
	fields, err := s.serialize.FieldsDeserialize(b)
	if err != nil {
		return nil, err
	}
	return &fields, nil
}

func (s *S3Storage) getMetadata(namespace string, identifier string) (EasyStoreMetadata, error) {
	key := s.assetKey(namespace, identifier, S3MetadataFileName)

	// download from S3
	b, err := s.s3DownloadToBuffer(s.Bucket, key)
	if err != nil {
		return nil, err
	}
	metadata, err := s.serialize.MetadataDeserialize(b)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (s *S3Storage) getObject(namespace string, identifier string) (EasyStoreObject, error) {
	key := s.assetKey(namespace, identifier, S3ObjectFileName)

	// download from S3
	b, err := s.s3DownloadToBuffer(s.Bucket, key)
	if err != nil {
		return nil, err
	}
	obj, err := s.serialize.ObjectDeserialize(b)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *S3Storage) isBlobName(name string) bool {
	if strings.HasSuffix(name, S3BlobFileNameSuffix) {
		return true
	}
	return false
}

//
// S3 helpers
//

func (s *S3Storage) s3UploadFromBuffer(bucket string, key string, buf []byte) error {

	logDebug(s.log, fmt.Sprintf("uploading [%s/%s]", bucket, key))
	start := time.Now()

	// upload in 5 MB blocks
	var partMiBs int64 = 5
	uploader := manager.NewUploader(s.S3Client, func(u *manager.Uploader) {
		u.PartSize = partMiBs * 1024 * 1024
	})
	_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf),
	})

	duration := time.Since(start)
	logDebug(s.log, fmt.Sprintf("upload [%s/%s] complete in %0.2f seconds (%s)", bucket, key, duration.Seconds(), s.statusText(err)))
	return err
}

func (s *S3Storage) s3DownloadToBuffer(bucket string, key string) ([]byte, error) {

	logDebug(s.log, fmt.Sprintf("downloading [%s/%s]", bucket, key))
	start := time.Now()

	// download in 5 MB blocks
	var partMiBs int64 = 5
	downloader := manager.NewDownloader(s.S3Client, func(d *manager.Downloader) {
		d.PartSize = partMiBs * 1024 * 1024
	})
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	duration := time.Since(start)
	logDebug(s.log, fmt.Sprintf("download [%s/%s] complete in %0.2f seconds (%s)", bucket, key, duration.Seconds(), s.statusText(err)))
	return buffer.Bytes(), err
}

func (s *S3Storage) s3Remove(bucket string, key string) error {

	logDebug(s.log, fmt.Sprintf("deleting [%s/%s]", bucket, key))
	start := time.Now()

	var objectIds []types.ObjectIdentifier
	objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
	_, err := s.S3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: objectIds},
	})

	duration := time.Since(start)
	logDebug(s.log, fmt.Sprintf("delete [%s/%s] complete in %0.2f seconds (%s)", bucket, key, duration.Seconds(), s.statusText(err)))
	return err
}

func (s *S3Storage) s3Exists(bucket string, key string) bool {

	logDebug(s.log, fmt.Sprintf("head [%s/%s]", bucket, key))
	start := time.Now()

	_, err := s.S3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	duration := time.Since(start)
	logDebug(s.log, fmt.Sprintf("head [%s/%s] complete in %0.2f seconds (%s)", bucket, key, duration.Seconds(), s.statusText(err)))
	return err == nil
}

func (s *S3Storage) s3List(bucket string, key string) ([]string, error) {

	logDebug(s.log, fmt.Sprintf("list [%s/%s]", bucket, key))
	start := time.Now()

	res, err := s.S3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	// make the result set
	result := make([]string, 0)
	for _, o := range res.Contents {
		result = append(result, *o.Key)
	}

	duration := time.Since(start)
	logDebug(s.log, fmt.Sprintf("list [%s/%s] complete in %0.2f seconds (%s)", bucket, key, duration.Seconds(), s.statusText(nil)))
	return result, nil
}

// assetName -- S3 assets are named as follows:
// s3://Bucket-name/namespace/object-identifier/asset-name
func (s *S3Storage) assetKey(namespace string, identifier string, assetName string) string {
	return fmt.Sprintf("%s/%s/%s", namespace, identifier, assetName)
}

// create a signed access URL for this blob
func (s *S3Storage) signedUrl(bucket string, key string) (string, error) {

	ps, err := s.s3SignClient.PresignGetObject(context.Background(),
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}, s3.WithPresignExpires(time.Hour*1)) // FIXME

	if err != nil {
		return "", err
	}
	decode, err := url.QueryUnescape(ps.URL)
	if err != nil {
		return "", err
	}
	return decode, nil
}

func (s *S3Storage) statusText(err error) string {
	if err == nil {
		return "ok"
	}
	return "ERR"
}

//
// end of file
//
