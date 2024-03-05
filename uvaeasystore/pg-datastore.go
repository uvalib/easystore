//
// Postgres implementation of the datastore interface
//

package uvaeasystore

import (
	"database/sql"
	"fmt"
	"log"

	// postgres
	_ "github.com/lib/pq"
)

// DatastorePostgresConfig -- this is our Postgres configuration implementation
type DatastorePostgresConfig struct {
	DbHost     string      // host endpoint
	DbPort     int         // port
	DbName     string      // database name
	DbUser     string      // database user
	DbPassword string      // database password
	DbTimeout  int         // timeout
	BusName    string      // the message bus name
	SourceName string      // the event source name
	Log        *log.Logger // the logger
}

func (impl DatastorePostgresConfig) Logger() *log.Logger {
	return impl.Log
}

func (impl DatastorePostgresConfig) SetLogger(log *log.Logger) {
	impl.Log = log
}

func (impl DatastorePostgresConfig) MessageBus() string {
	return impl.BusName
}

func (impl DatastorePostgresConfig) SetMessageBus(busName string) {
	impl.BusName = busName
}

func (impl DatastorePostgresConfig) EventSource() string {
	return impl.SourceName
}

func (impl DatastorePostgresConfig) SetEventSource(sourceName string) {
	impl.SourceName = sourceName
}

// newPostgresStore -- create a postgres version of the DataStore
func newPostgresStore(config EasyStoreConfig) (DataStore, error) {

	// make sure its one of these
	c, ok := config.(DatastorePostgresConfig)
	if ok == false {
		return nil, fmt.Errorf("%q: %w", "bad configuration, not a DatastorePostgresConfig", ErrBadParameter)
	}

	// validate our configuration
	err := validatePostgresConfig(c)
	if err != nil {
		return nil, err
	}

	logDebug(config.Logger(), fmt.Sprintf("using [postgres:%s/%s] for dbStorage", c.DbHost, c.DbName))

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

	return &dbStorage{
		dbCurrentTimeFn: "NOW()",
		log:             c.Log,
		DB:              db,
	}, nil
}

func validatePostgresConfig(config DatastorePostgresConfig) error {

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

	if len(config.BusName) != 0 && len(config.SourceName) == 0 {
		return fmt.Errorf("%q: %w", "config.SourceName is blank", ErrBadParameter)
	}

	return nil
}

//
// end of file
//
