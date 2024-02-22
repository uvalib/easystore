//
// Postgress implementation of the datastore interface
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
	Log        *log.Logger // the logger
}

func (impl DatastorePostgresConfig) Logger() *log.Logger {
	return impl.Log
}

func (impl DatastorePostgresConfig) SetLogger(log *log.Logger) {
	impl.Log = log
}

// newPostgresStore -- create a postgres version of the DataStore
func newPostgresStore(config EasyStoreConfig) (DataStore, error) {

	// make sure its one of these
	c, ok := config.(DatastorePostgresConfig)
	if ok == false {
		return nil, fmt.Errorf("%q: %w", "bad configuration, not a DatastorePostgresConfig", ErrBadParameter)
	}

	logDebug(config.Logger(), fmt.Sprintf("using [postgres:%s/%s] for storage", c.DbHost, c.DbName))

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

	return &storage{c.Log, db}, nil
}

//
// end of file
//
