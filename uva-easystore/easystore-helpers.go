//
//
//

package uva_easystore

import "log"

func logDebug(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("DEBUG: %s", msg)
	}
}

func logError(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("ERROR: %s", msg)
	}
}

func logInfo(log *log.Logger, msg string) {
	if log != nil {
		log.Printf("INFO: %s", msg)
	}
}

//
// end of file
//
