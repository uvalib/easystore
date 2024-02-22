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
	namespace  string      // source file name
	filesystem string      // the storage filesystem
	log        *log.Logger // the logger
}

func (impl DatastoreSqliteConfig) Logger() *log.Logger {
	return impl.log
}

func (impl DatastoreSqliteConfig) SetLogger(log *log.Logger) {
	impl.log = log
}

// newSqliteStore -- create a sqlite version of the DataStore
func newSqliteStore(config EasyStoreConfig) (DataStore, error) {

	// make sure its one of these
	c, ok := config.(DatastoreSqliteConfig)
	if ok == false {
		return nil, fmt.Errorf("%q: %w", "bad configuration, not a datastoreSqliteConfig", ErrBadParameter)
	}

	// validate our configuration
	if len(c.filesystem) == 0 || len(c.namespace) == 0 {
		return nil, ErrBadParameter
	}

	dataSourceName := fmt.Sprintf("%s/%s.db", c.filesystem, c.namespace)
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

	return &storage{c.log, db}, nil
}

//
// end of file
//
