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
	Namespace  string      // source file name
	Filesystem string      // the storage Filesystem
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
	if len(c.Filesystem) == 0 || len(c.Namespace) == 0 {
		return nil, ErrBadParameter
	}

	dataSourceName := fmt.Sprintf("%s/%s.db", c.Filesystem, c.Namespace)
	logDebug(config.Logger(), fmt.Sprintf("using [sqlite:%s] for storage", dataSourceName))

	// make sure it exists so we do not create an empty schema
	_, err := os.Stat(dataSourceName)
	if err != nil {
		return nil, ErrNamespaceNotFound
	}

	db, err := sql.Open("sqlite3", dataSourceName)
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
