//
// S3 implementation of the datastore interface (which also uses Postgres)
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	"context"
	"database/sql"
	"fmt"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	// postgres
	_ "github.com/lib/pq"
)

// DatastoreS3Config -- this is our S3 configuration implementation
type DatastoreS3Config struct {
	Bucket              string      // storage Bucket name
	SignerAccessKey     string      // the signer access key
	SignerSecretKey     string      // the signer secret key
	SignerExpireMinutes int         // signed link expire time in minutes
	DbHost              string      // host endpoint
	DbPort              int         // port
	DbName              string      // database name
	DbUser              string      // database user
	DbPassword          string      // database password
	DbTimeout           int         // timeout
	BusName             string      // the message bus name
	SourceName          string      // the event source name
	Log                 *log.Logger // the logger
}

func (impl DatastoreS3Config) Logger() *log.Logger {
	return impl.Log
}

func (impl DatastoreS3Config) SetLogger(log *log.Logger) {
	impl.Log = log
}

func (impl DatastoreS3Config) MessageBus() string {
	return impl.BusName
}

func (impl DatastoreS3Config) SetMessageBus(busName string) {
	impl.BusName = busName
}

func (impl DatastoreS3Config) EventSource() string {
	return impl.SourceName
}

func (impl DatastoreS3Config) SetEventSource(sourceName string) {
	impl.SourceName = sourceName
}

// newS3Store -- create an S3 version of the DataStore
func newS3Store(config EasyStoreImplConfig) (DataStore, error) {

	// make sure its one of these
	c, ok := config.(DatastoreS3Config)
	if ok == false {
		return nil, fmt.Errorf("%q: %w", "bad configuration, not a DatastoreS3Config", ErrBadParameter)
	}

	// validate our configuration
	err := validateS3Config(c)
	if err != nil {
		return nil, err
	}

	logDebug(config.Logger(), fmt.Sprintf("using [s3://%s] for storage", c.Bucket))

	cfg, err := awsconfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	// our role based S3 client
	client := s3.NewFromConfig(cfg)

	// our signer S3 client
	s3Cfg, err := awsconfig.LoadDefaultConfig(
		context.TODO(),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				c.SignerAccessKey,
				c.SignerSecretKey,
				""),
		),
	)
	if err != nil {
		return nil, err
	}
	s3Client := s3.NewFromConfig(s3Cfg)
	signer := s3.NewPresignClient(s3Client)

	// connect to database (postgres)
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d connect_timeout=%d",
		c.DbUser, c.DbPassword, c.DbName, c.DbHost, c.DbPort, c.DbTimeout)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return &S3Storage{
		serialize:           newEasyStoreSerializer(),
		Bucket:              c.Bucket,
		S3Client:            client,
		s3SignClient:        signer,
		s3SignExpireMinutes: c.SignerExpireMinutes,
		log:                 c.Log,
		DB:                  db,
	}, nil
}

func validateS3Config(config DatastoreS3Config) error {

	if len(config.Bucket) == 0 {
		return fmt.Errorf("%q: %w", "config.Bucket is blank", ErrBadParameter)
	}

	if len(config.DbHost) == 0 {
		return fmt.Errorf("%q: %w", "config.DbHost is blank", ErrBadParameter)
	}

	if len(config.DbName) == 0 {
		return fmt.Errorf("%q: %w", "config.DbName is blank", ErrBadParameter)
	}

	if len(config.DbUser) == 0 {
		return fmt.Errorf("%q: %w", "config.DbUser is blank", ErrBadParameter)
	}

	if len(config.DbPassword) == 0 {
		return fmt.Errorf("%q: %w", "config.DbPassword is blank", ErrBadParameter)
	}

	if config.DbPort == 0 {
		return fmt.Errorf("%q: %w", "config.DbPort is 0", ErrBadParameter)
	}

	if config.DbTimeout == 0 {
		return fmt.Errorf("%q: %w", "config.DbTimeout is 0", ErrBadParameter)
	}

	if config.SignerExpireMinutes == 0 {
		return fmt.Errorf("%q: %w", "config.SignerExpireMinutes is 0", ErrBadParameter)
	}

	if len(config.BusName) != 0 && len(config.SourceName) == 0 {
		return fmt.Errorf("%q: %w", "config.SourceName is blank", ErrBadParameter)
	}

	return nil
}

//
// end of file
//
