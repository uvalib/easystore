//
//
//

package uvaeasystore

import (
	"encoding/json"
	"errors"
	"testing"
)

// a complete object payload, the shape the service actually sends
var fullObjectPayload = `{
	"namespace": "test-namespace",
	"id": "oid-unmarshal",
	"vtag": "vtag-unmarshal",
	"created": "2024-01-02T03:04:05Z",
	"modified": "2024-01-02T03:04:06Z",
	"fields": {"author": "the author"},
	"metadata": {"mimetype": "application/json", "payload": "e30="},
	"files": [{"name": "file.bin", "mimetype": "application/octet-stream"}]
}`

// an unexpected response shape must be an error, never a panic
func TestObjectUnmarshalBadPayload(t *testing.T) {

	tests := []struct {
		name    string
		payload string
	}{
		// none of the required members are present
		{"empty object", `{}`},
		// each required member missing in turn
		{"no namespace", `{"id":"i","vtag":"v","created":"2024-01-02T03:04:05Z","modified":"2024-01-02T03:04:05Z"}`},
		{"no id", `{"namespace":"n","vtag":"v","created":"2024-01-02T03:04:05Z","modified":"2024-01-02T03:04:05Z"}`},
		{"no vtag", `{"namespace":"n","id":"i","created":"2024-01-02T03:04:05Z","modified":"2024-01-02T03:04:05Z"}`},
		{"no created", `{"namespace":"n","id":"i","vtag":"v","modified":"2024-01-02T03:04:05Z"}`},
		{"no modified", `{"namespace":"n","id":"i","vtag":"v","created":"2024-01-02T03:04:05Z"}`},
		// present but explicitly null, which also yields a nil raw message
		{"null namespace", `{"namespace":null,"id":"i","vtag":"v","created":"2024-01-02T03:04:05Z","modified":"2024-01-02T03:04:05Z"}`},
		// the whole payload is null
		{"null payload", `null`},
		// a required member of the wrong type
		{"numeric namespace", `{"namespace":1,"id":"i","vtag":"v","created":"2024-01-02T03:04:05Z","modified":"2024-01-02T03:04:05Z"}`},
		// an optional member of the wrong type
		{"bad fields", `{"namespace":"n","id":"i","vtag":"v","created":"2024-01-02T03:04:05Z","modified":"2024-01-02T03:04:05Z","fields":"not a map"}`},
	}

	for _, test := range tests {
		var obj easyStoreObjectImpl
		err := json.Unmarshal([]byte(test.payload), &obj)
		if err == nil {
			t.Fatalf("%s: expected an error but got 'OK'\n", test.name)
		}
		if errors.Is(err, ErrDeserialize) == false {
			t.Fatalf("%s: expected '%s' but got '%s'\n", test.name, ErrDeserialize, err)
		}
	}
}

// explicitly null optional members are absent, not an error
func TestObjectUnmarshalNullOptionals(t *testing.T) {

	payload := `{
		"namespace": "test-namespace",
		"id": "oid-unmarshal",
		"vtag": "vtag-unmarshal",
		"created": "2024-01-02T03:04:05Z",
		"modified": "2024-01-02T03:04:05Z",
		"fields": null,
		"metadata": null,
		"files": null
	}`

	var obj easyStoreObjectImpl
	if err := json.Unmarshal([]byte(payload), &obj); err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if obj.Fields() != nil {
		t.Fatalf("expected no fields but got %d\n", len(obj.Fields()))
	}
	if obj.Metadata() != nil {
		t.Fatalf("expected no metadata but got some\n")
	}
	if obj.Files() != nil {
		t.Fatalf("expected no files but got %d\n", len(obj.Files()))
	}
}

// a well formed payload must still deserialize completely
func TestObjectUnmarshalGoodPayload(t *testing.T) {

	var obj easyStoreObjectImpl
	if err := json.Unmarshal([]byte(fullObjectPayload), &obj); err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if obj.Namespace() != "test-namespace" {
		t.Fatalf("expected namespace 'test-namespace' but got '%s'\n", obj.Namespace())
	}
	if obj.Id() != "oid-unmarshal" {
		t.Fatalf("expected id 'oid-unmarshal' but got '%s'\n", obj.Id())
	}
	if obj.VTag() != "vtag-unmarshal" {
		t.Fatalf("expected vtag 'vtag-unmarshal' but got '%s'\n", obj.VTag())
	}
	if obj.Created().IsZero() == true || obj.Modified().IsZero() == true {
		t.Fatalf("expected created/modified times but got zero values\n")
	}
	if obj.Fields()["author"] != "the author" {
		t.Fatalf("expected field 'the author' but got '%s'\n", obj.Fields()["author"])
	}
	if obj.Metadata() == nil {
		t.Fatalf("expected metadata but got none\n")
	}
	if len(obj.Files()) != 1 {
		t.Fatalf("expected 1 file but got %d\n", len(obj.Files()))
	}
	if obj.Files()[0].Name() != "file.bin" {
		t.Fatalf("expected file 'file.bin' but got '%s'\n", obj.Files()[0].Name())
	}
}

// a bad element in a result list must fail the enclosing response, not panic
func TestObjectsResponseBadElement(t *testing.T) {

	payload := `{"results":[` + fullObjectPayload + `,{}]}`

	var resp GetObjectsResponse
	err := json.Unmarshal([]byte(payload), &resp)
	if err == nil {
		t.Fatalf("expected an error but got 'OK'\n")
	}
	if errors.Is(err, ErrDeserialize) == false {
		t.Fatalf("expected '%s' but got '%s'\n", ErrDeserialize, err)
	}
}

//
// end of file
//
