//
//
//

package uvaeasystore

import (
	"errors"
	"testing"
)

// a payload that deserializes cleanly, used as the basis for the damaged ones
var goodObjectJson = `{"ns":"test-namespace","id":"oid-1","vtag":"vtag-1",` +
	`"created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:06 +0000 UTC"}`

var goodBlobJson = `{"name":"\"file.bin\"","mimetype":"application/octet-stream","payload":"aGVsbG8=",` +
	`"created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:06 +0000 UTC"}`

var goodMetadataJson = `{"mimetype":"application/json","payload":"e30=",` +
	`"created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:06 +0000 UTC"}`

// damaged stored data must be an error, never a panic. There is a tool that exists to hunt
// for damaged assets so reading one must not take the caller down
func TestObjectDeserializeDamaged(t *testing.T) {

	s := DefaultEasyStoreSerializer()

	tests := []struct {
		name    string
		payload string
	}{
		{"object: empty", `{}`},
		{"object: null", `null`},
		{"object: no ns", `{"id":"i","vtag":"v","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"object: no id", `{"ns":"n","vtag":"v","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"object: no vtag", `{"ns":"n","id":"i","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"object: no created", `{"ns":"n","id":"i","vtag":"v","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"object: no modified", `{"ns":"n","id":"i","vtag":"v","created":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"object: numeric ns", `{"ns":1,"id":"i","vtag":"v","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"object: null vtag", `{"ns":"n","id":"i","vtag":null,"created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"object: bad timestamp", `{"ns":"n","id":"i","vtag":"v","created":"not a time","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
	}

	for _, test := range tests {
		_, err := s.ObjectDeserialize([]byte(test.payload))
		if err == nil {
			t.Fatalf("%s: expected an error but got 'OK'\n", test.name)
		}
		if errors.Is(err, ErrDeserialize) == false {
			t.Fatalf("%s: expected '%s' but got '%s'\n", test.name, ErrDeserialize, err)
		}
	}
}

// as above, for a damaged file descriptor
func TestBlobDeserializeDamaged(t *testing.T) {

	s := DefaultEasyStoreSerializer()

	tests := []struct {
		name    string
		payload string
	}{
		{"blob: empty", `{}`},
		{"blob: null", `null`},
		{"blob: no payload", `{"name":"\"f\"","mimetype":"m","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"blob: no name", `{"mimetype":"m","payload":"aGVsbG8=","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"blob: no mimetype", `{"name":"\"f\"","payload":"aGVsbG8=","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"blob: no created", `{"name":"\"f\"","mimetype":"m","payload":"aGVsbG8=","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"blob: numeric name", `{"name":1,"mimetype":"m","payload":"aGVsbG8=","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"blob: bad base64", `{"name":"\"f\"","mimetype":"m","payload":"not base64!!","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
	}

	for _, test := range tests {
		_, err := s.BlobDeserialize([]byte(test.payload))
		if err == nil {
			t.Fatalf("%s: expected an error but got 'OK'\n", test.name)
		}
		if errors.Is(err, ErrDeserialize) == false {
			t.Fatalf("%s: expected '%s' but got '%s'\n", test.name, ErrDeserialize, err)
		}
	}
}

// as above, for damaged object metadata
func TestMetadataDeserializeDamaged(t *testing.T) {

	s := DefaultEasyStoreSerializer()

	tests := []struct {
		name    string
		payload string
	}{
		{"metadata: empty", `{}`},
		{"metadata: null", `null`},
		{"metadata: no payload", `{"mimetype":"m","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"metadata: no mimetype", `{"payload":"e30=","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"metadata: no modified", `{"mimetype":"m","payload":"e30=","created":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"metadata: numeric mimetype", `{"mimetype":1,"payload":"e30=","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
		{"metadata: bad base64", `{"mimetype":"m","payload":"not base64!!","created":"2024-01-02 03:04:05 +0000 UTC","modified":"2024-01-02 03:04:05 +0000 UTC"}`},
	}

	for _, test := range tests {
		_, err := s.MetadataDeserialize([]byte(test.payload))
		if err == nil {
			t.Fatalf("%s: expected an error but got 'OK'\n", test.name)
		}
		if errors.Is(err, ErrDeserialize) == false {
			t.Fatalf("%s: expected '%s' but got '%s'\n", test.name, ErrDeserialize, err)
		}
	}
}

// well formed stored data must still deserialize, the checks above must not be over strict
func TestDeserializeGoodData(t *testing.T) {

	s := DefaultEasyStoreSerializer()

	obj, err := s.ObjectDeserialize([]byte(goodObjectJson))
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if obj.Namespace() != goodNamespace {
		t.Fatalf("expected namespace '%s' but got '%s'\n", goodNamespace, obj.Namespace())
	}
	if obj.Id() != "oid-1" {
		t.Fatalf("expected id 'oid-1' but got '%s'\n", obj.Id())
	}
	if obj.VTag() != "vtag-1" {
		t.Fatalf("expected vtag 'vtag-1' but got '%s'\n", obj.VTag())
	}
	if obj.Created().IsZero() == true || obj.Modified().IsZero() == true {
		t.Fatalf("expected created/modified times but got zero values\n")
	}

	blob, err := s.BlobDeserialize([]byte(goodBlobJson))
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if blob.Name() != "file.bin" {
		t.Fatalf("expected name 'file.bin' but got '%s'\n", blob.Name())
	}
	pl, err := blob.Payload()
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if string(pl) != "hello" {
		t.Fatalf("expected payload 'hello' but got '%s'\n", string(pl))
	}

	meta, err := s.MetadataDeserialize([]byte(goodMetadataJson))
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	if meta.MimeType() != "application/json" {
		t.Fatalf("expected mimetype 'application/json' but got '%s'\n", meta.MimeType())
	}
}

// anything we serialize must deserialize back again
func TestSerializeRoundTrip(t *testing.T) {

	s := DefaultEasyStoreSerializer()

	o := NewEasyStoreObject(goodNamespace, "oid-roundtrip")
	obj, err := s.ObjectDeserialize(s.ObjectSerialize(o))
	if err != nil {
		t.Fatalf("object: expected 'OK' but got '%s'\n", err)
	}
	if obj.Id() != o.Id() || obj.Namespace() != o.Namespace() || obj.VTag() != o.VTag() {
		t.Fatalf("object: round trip did not preserve ns/id/vtag\n")
	}

	b := NewEasyStoreBlobFromBuffer("file.bin", "application/octet-stream", jsonPayload)
	blob, err := s.BlobDeserialize(s.BlobSerialize(b))
	if err != nil {
		t.Fatalf("blob: expected 'OK' but got '%s'\n", err)
	}
	if blob.Name() != b.Name() || blob.MimeType() != b.MimeType() {
		t.Fatalf("blob: round trip did not preserve name/mimetype\n")
	}

	m := NewEasyStoreMetadata("application/json", jsonPayload)
	meta, err := s.MetadataDeserialize(s.MetadataSerialize(m))
	if err != nil {
		t.Fatalf("metadata: expected 'OK' but got '%s'\n", err)
	}
	if meta.MimeType() != m.MimeType() {
		t.Fatalf("metadata: round trip did not preserve mimetype\n")
	}
}

// a non []byte interface is already handled, make sure it stays that way
func TestDeserializeWrongInterface(t *testing.T) {

	s := DefaultEasyStoreSerializer()

	for _, i := range []interface{}{"a string", 42, nil} {
		if _, err := s.ObjectDeserialize(i); errors.Is(err, ErrDeserialize) == false {
			t.Fatalf("expected '%s' but got '%s'\n", ErrDeserialize, err)
		}
		if _, err := s.BlobDeserialize(i); errors.Is(err, ErrDeserialize) == false {
			t.Fatalf("expected '%s' but got '%s'\n", ErrDeserialize, err)
		}
		if _, err := s.MetadataDeserialize(i); errors.Is(err, ErrDeserialize) == false {
			t.Fatalf("expected '%s' but got '%s'\n", ErrDeserialize, err)
		}
	}
}

//
// end of file
//
