//
//
//

package uvaeasystore

import (
	"log"
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

func logError(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("ERROR: %s", msg)
	} else {
		//fmt.Printf("ERROR: %s\n", msg)
	}
}

func logInfo(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("INFO: %s", msg)
	} else {
		//fmt.Printf("INFO: %s\n", msg)
	}
}

//
// end of file
//
