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

// newSqliteStore -- create a sqlite version of the DataStore
func newSqliteStore(namespace string, log *log.Logger) (DataStore, error) {

	// temp location for now
	dataSourceName := fmt.Sprintf("/tmp/%s.db", namespace)

	logDebug(log, fmt.Sprintf("using [sqlite:%s] for storage", dataSourceName))

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

	return &storage{log, db}, nil
}

//
// end of file
//
