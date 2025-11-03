//
//
//

package uvaeasystore

import (
	"testing"
)

func TestObjectBlobsUpdate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if o.Files() != nil {
		t.Fatalf("expected empty but got non-empty\n")
	}

	// add some files
	file1Name := "file1.bin"
	file2Name := "file2.bin"
	f1 := newBinaryBlob(file1Name)
	f2 := newBinaryBlob(file2Name)
	blobs := []EasyStoreBlob{f1, f2}

	//file1Contents, _ := f1.Payload()
	file1Type := f1.MimeType()
	//file2Contents, _ := f2.Payload()
	file2Type := f2.MimeType()

	// update the object
	o.SetFiles(blobs)
	o, err = es.ObjectUpdate(o, Files)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// test we got back what we expect
	if o.Files() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	files := o.Files()
	if len(files) != 2 {
		t.Fatalf("expected 2 but got %d\n", len(files))
	}

	b1 := files[0]
	b2 := files[1]
	testEqual(t, file1Name, b1.Name())
	testEqual(t, file2Name, b2.Name())
	testEqual(t, file1Type, b1.MimeType())
	testEqual(t, file2Type, b2.MimeType())
	buf1, _ := b1.Payload()
	buf2, _ := b2.Payload()
	url1 := b1.Url()
	url2 := b2.Url()

	if (buf1 == nil || len(buf1) == 0) && len(url1) == 0 {
		t.Fatalf("file payload AND url are empty\n")
	}
	if (buf2 == nil || len(buf2) == 0) && len(url2) == 0 {
		t.Fatalf("file payload AND url are empty\n")
	}

	//fmt.Printf("SIGNED URL: %s\n", url1)
	//fmt.Printf("SIGNED URL: %s\n", url2)

	//if bytes.Equal(file1Contents, buf1) == false {
	//	t.Fatalf("byte slices are not equal\n")
	//}
	//if bytes.Equal(file2Contents, buf2) == false {
	//	t.Fatalf("byte slices are not equal\n")
	//}
}

func TestObjectFieldsUpdate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// create a couple of field sets
	fieldSet1 := DefaultEasyStoreFields()
	fieldSet2 := DefaultEasyStoreFields()

	fieldSet1["field-one"] = "s1v1"
	fieldSet1["field-two"] = "s1v2"
	fieldSet1["field-three"] = "s1v3"

	fieldSet2["field-one"] = "s2v1"
	fieldSet2["another"] = "s2v2"

	o.SetFields(fieldSet1)

	// update the object
	n, err := es.ObjectUpdate(o, AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if n.Fields() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	if fieldsEqual(fieldSet1, n.Fields()) == false {
		t.Fatalf("expected fields to be equal\n")
	}

	n.SetFields(fieldSet2)

	// update the object again
	n, err = es.ObjectUpdate(n, AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if n.Fields() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	if fieldsEqual(fieldSet2, n.Fields()) == false {
		t.Fatalf("expected fields to be equal\n")
	}
}

func TestObjectMetadataUpdate(t *testing.T) {
	es := testSetup(t)
	defer es.Close()
	o := NewEasyStoreObject(goodNamespace, "")

	// create the new object
	o, err := es.ObjectCreate(o)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// add some metadata
	mimeType := "application/json"
	m := newEasyStoreMetadata(mimeType, jsonPayload)
	o.SetMetadata(m)

	// update the object
	n, err := es.ObjectUpdate(o, AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if n.Metadata() == nil {
		t.Fatalf("expected non-empty but got empty\n")
	}

	testEqual(t, mimeType, n.Metadata().MimeType())
	buf, err := n.Metadata().Payload()
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	testEqual(t, string(jsonPayload), string(buf))

	// update the object again
	_, err = es.ObjectUpdate(n, AllComponents)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
}

//
// end of file
//
