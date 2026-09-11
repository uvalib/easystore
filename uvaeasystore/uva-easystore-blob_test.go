//
//
//

package uvaeasystore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
)

func TestBlobFromBuffer(t *testing.T) {

	payload := []byte("the payload contents")
	b := NewEasyStoreBlob("file1.bin", "application/octet-stream", payload)

	// a buffered blob provides its payload as a buffer
	buf, err := b.Payload()
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if bytes.Equal(payload, buf) == false {
		t.Fatalf("payloads are unequal but should be\n")
	}

	// and also as a stream
	reader, err := b.PayloadReader()
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	defer reader.Close()

	buf, err = io.ReadAll(reader)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if bytes.Equal(payload, buf) == false {
		t.Fatalf("payloads are unequal but should be\n")
	}
}

func TestBlobFromReader(t *testing.T) {

	payload := []byte("the payload contents")
	b := NewEasyStoreBlobFromReader("file1.bin", "application/octet-stream", io.NopCloser(bytes.NewReader(payload)))

	testEqual(t, "file1.bin", b.Name())
	testEqual(t, "application/octet-stream", b.MimeType())

	// a streamed blob does not provide its payload as a buffer
	expected := ErrPayloadNotBuffered
	_, err := b.Payload()
	if errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}

	// it is only available as a stream
	reader, err := b.PayloadReader()
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	defer reader.Close()

	buf, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if bytes.Equal(payload, buf) == false {
		t.Fatalf("payloads are unequal but should be\n")
	}
}

func TestBlobFromFile(t *testing.T) {

	payload := []byte("{\"id\":123,\"name\":\"the name\"}")
	fname := tempFile(t, payload)
	defer os.Remove(fname)

	// no mime type supplied so it is determined from the contents
	b, err := NewEasyStoreBlobFromFile("file1.json", "", fname)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	testEqual(t, "file1.json", b.Name())
	if len(b.MimeType()) == 0 {
		t.Fatalf("mime type is empty\n")
	}

	// the payload is streamed from the start of the file, even though we have already
	// read from it to determine the mime type
	reader, err := b.PayloadReader()
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	defer reader.Close()

	buf, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if bytes.Equal(payload, buf) == false {
		t.Fatalf("payloads are unequal but should be\n")
	}

	// and a supplied mime type is used as is
	b, err = NewEasyStoreBlobFromFile("file1.json", "application/json", fname)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	defer func() {
		r, _ := b.PayloadReader()
		r.Close()
	}()
	testEqual(t, "application/json", b.MimeType())

	// a non-existent file is an error
	_, err = NewEasyStoreBlobFromFile("file1.json", "", badFilename)
	if err == nil {
		t.Fatalf("expected an error but got 'OK'\n")
	}
}

func TestBlobPreflight(t *testing.T) {

	// a streamed blob passes preflight without its payload being consumed
	b := NewEasyStoreBlobFromReader("file1.bin", "application/octet-stream", io.NopCloser(bytes.NewReader([]byte("payload"))))
	if err := FileCreatePreflight(goodNamespace, "oid-123", b); err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if err := FileUpdatePreflight(goodNamespace, "oid-123", b); err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	// while an empty buffered blob does not
	expected := ErrBadParameter
	b = NewEasyStoreBlob("file1.bin", "application/octet-stream", nil)
	if err := FileCreatePreflight(goodNamespace, "oid-123", b); errors.Is(err, expected) == false {
		t.Fatalf("expected '%s' but got '%s'\n", expected, err)
	}
}

//
// end of file
//
