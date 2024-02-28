//
//
//

package uvaeasystore

import (
	"strconv"
	"testing"
)

// test invariants
var goodSqliteFilename = "/tmp/sqlite.db"
var badSqliteFilename = "/tmp/blablabla.db"

var goodNamespace = "libraopen"
var goodId = "oid:cnfivf6dfnu1a2a5l3fg"
var badId = "oid:blablabla"
var jsonPayload = []byte("{\"id\":123,\"name\":\"the name\"}")

func newTestObject(id string) EasyStoreObject {
	return NewEasyStoreObject(goodNamespace, id)
}

func testSetupReadonly(t *testing.T) EasyStoreReadonly {
	// configure what we need
	config := DatastoreSqliteConfig{
		DataSource: goodSqliteFilename,
		//Log:        log.Default(),
	}

	//config := DatastorePostgresConfig{
	//	DbHost:     os.Getenv("DBHOST"),
	//	DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//	DbName:     os.Getenv("DBNAME"),
	//	DbUser:     os.Getenv("DBUSER"),
	//	DbPassword: os.Getenv("DBPASSWD"),
	//	DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//  Log:        log.Default(),
	//}

	esro, err := NewEasyStoreReadonly(config)
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return esro
}

func testSetup(t *testing.T) EasyStore {
	// configure what we need
	config := DatastoreSqliteConfig{
		DataSource: goodSqliteFilename,
		//Log:        Log.Default(),
	}

	//config := DatastorePostgresConfig{
	//	DbHost:     os.Getenv("DBHOST"),
	//	DbPort:     asIntWithDefault(os.Getenv("DBPORT"), 0),
	//	DbName:     os.Getenv("DBNAME"),
	//	DbUser:     os.Getenv("DBUSER"),
	//	DbPassword: os.Getenv("DBPASSWD"),
	//	DbTimeout:  asIntWithDefault(os.Getenv("DBTIMEOUT"), 0),
	//  Log:        Log.Default(),
	//}

	es, err := NewEasyStore(config)
	if err != nil {
		t.Fatalf("%t\n", err)
	}
	return es
}

func validateObject(t *testing.T, obj EasyStoreObject, which EasyStoreComponents) {

	// test the contents of the object
	if len(obj.Id()) == 0 {
		t.Fatalf("object id is empty\n")
	}

	// should it have fields
	fieldCount := len(obj.Fields())
	if (which & Fields) == Fields {
		if fieldCount != 0 {
			for n, v := range obj.Fields() {
				if len(n) == 0 {
					t.Fatalf("object field key is empty\n")
				}
				if len(v) == 0 {
					t.Fatalf("object field value is empty\n")
				}
			}
		} else {
			t.Fatalf("expected object fields but got none\n")
		}
	} else {
		if fieldCount != 0 {
			t.Fatalf("unexpected object fields\n")
		}
	}

	// should it have files
	fileCount := len(obj.Files())
	if (which & Files) == Files {
		if fileCount != 0 {
			for _, f := range obj.Files() {
				if len(f.Name()) == 0 {
					t.Fatalf("file name is empty\n")
				}
				if len(f.MimeType()) == 0 {
					t.Fatalf("file mime type is empty\n")
				}
				if len(f.Payload()) == 0 {
					t.Fatalf("file payload is empty\n")
				}
				if len(f.Url()) == 0 {
					t.Fatalf("file url is empty\n")
				}
				if f.Created().IsZero() == true {
					t.Fatalf("file create time is empty\n")
				}
				if f.Modified().IsZero() == true {
					t.Fatalf("file modified time is empty\n")
				}
			}
		} else {
			t.Fatalf("expected object files but got none\n")
		}
	} else {
		if fileCount != 0 {
			t.Fatalf("unexpected object files\n")
		}
	}

	// should it have metadata
	md := obj.Metadata()
	if (which & Metadata) == Metadata {
		if md != nil {
			if len(md.MimeType()) == 0 {
				t.Fatalf("metadata mime type is empty\n")
			}
			if len(md.Payload()) == 0 {
				t.Fatalf("metadata payload is empty\n")
			}
			if md.Created().IsZero() == true {
				t.Fatalf("metadata create time is empty\n")
			}
			if md.Modified().IsZero() == true {
				t.Fatalf("metadata modified time is empty\n")
			}
		} else {
			t.Fatalf("expected object metadata but got none\n")
		}
	} else {
		if md != nil {
			t.Fatalf("unexpected object metadata\n")
		}
	}
}

func ensureObjectHasFields(t *testing.T, obj EasyStoreObject, fields EasyStoreObjectFields) {

	if len(obj.Fields()) == 0 {
		t.Fatalf("expected object fields but got none\n")
	}

	if len(fields) == 0 {
		t.Fatalf("expected reference fields but got none\n")
	}

	for n, v := range fields {
		if obj.Fields()[n] != v {
			t.Fatalf("expected field/value %s=%s but did not get it\n", n, v)
		}
	}
}

func testEqual(t *testing.T, expected string, actual string) {
	if expected != actual {
		t.Fatalf("expected '%s' but got '%s'\n", expected, actual)
	}
}

func asIntWithDefault(str string, def int) int {
	if len(str) == 0 {
		return def
	}
	i, err := strconv.Atoi(str)
	if err != nil {
		return def
	}
	return i
}

//
// end of file
//
