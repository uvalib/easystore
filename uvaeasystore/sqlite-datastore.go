//
// sqlite implementation of the datastore interface
//

package uvaeasystore

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
)

// DatastoreSqliteConfig -- this is our sqlite configuration implementation
type DatastoreSqliteConfig struct {
	DataSource string      // the storage file name
	Log        *log.Logger // the logger
}

func (impl DatastoreSqliteConfig) Logger() *log.Logger {
	return impl.Log
}

func (impl DatastoreSqliteConfig) SetLogger(log *log.Logger) {
	impl.Log = log
}

// newSqliteStore -- create a sqlite version of the DataStore
func newSqliteStore(config EasyStoreConfig) (DataStore, error) {

	// make sure its one of these
	c, ok := config.(DatastoreSqliteConfig)
	if ok == false {
		return nil, fmt.Errorf("%q: %w", "bad configuration, not a DatastoreSqliteConfig", ErrBadParameter)
	}

	// validate our configuration
	err := validateSqliteConfig(c)
	if err != nil {
		return nil, err
	}

	logDebug(config.Logger(), fmt.Sprintf("using [sqlite:%s] for storage", c.DataSource))

	db, err := sql.Open("sqlite3", c.DataSource)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return &storage{c.Log, db}, nil
}

func validateSqliteConfig(config DatastoreSqliteConfig) error {

	if len(config.DataSource) == 0 {
		return fmt.Errorf("%q: %w", "config.DataSource is blank", ErrBadParameter)
	}

	// make sure it exists
	_, err := os.Stat(config.DataSource)
	if err != nil {
		return fmt.Errorf("%q: %w", err, ErrFileNotFound)
	}

	return nil
}

//
// end of file
//
