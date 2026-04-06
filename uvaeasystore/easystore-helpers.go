//
//
//

package uvaeasystore

import (
	"fmt"
	"log"

	"github.com/rs/xid"
)

//
// private helpers
//

func logDebug(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("DEBUG: %s", msg)
	} else {
		//fmt.Printf("DEBUG: %s\n", msg)
	}
}

func logWarning(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("WARNING: %s", msg)
	} else {
		fmt.Printf("WARNING: %s\n", msg)
	}
}

func logError(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("ERROR: %s", msg)
	} else {
		fmt.Printf("ERROR: %s\n", msg)
	}
}

func logInfo(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("INFO: %s", msg)
	} else {
		//fmt.Printf("INFO: %s\n", msg)
	}
}

func newVtag() string {
	return fmt.Sprintf("vtag-%s", xid.New().String())
}

func newObjectId() string {
	return fmt.Sprintf("oid-%s", xid.New().String())
}

//
// end of file
//
