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

// newPostgresStore -- create a postgres version of the DataStore
func newPostgresStore(namespace string, log *log.Logger) (DataStore, error) {

	// for now
	dbUser := "easystore"
	dbPass := "Iojaiviuhee7toh7Ohni6ho2eoj3iesh"
	dbName := "easystore"
	dbHost := "rds-postgres15-staging.internal.lib.virginia.edu"
	dbPort := 5432
	dbTimeout := 30

	logDebug(log, fmt.Sprintf("using [postgres:%s/%s] for storage", dbHost, dbName))

	// connect to database (postgres)
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d connect_timeout=%d",
		dbUser, dbPass, dbName, dbHost, dbPort, dbTimeout)

	db, err := sql.Open("postgres", connStr)
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
