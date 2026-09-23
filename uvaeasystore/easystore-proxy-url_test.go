//
//
//

package uvaeasystore

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// what the server actually received
type capturedRequest struct {
	method string
	path   string // the decoded path, so we compare against the values we sent
	query  map[string]string
	raw    string // the request target as it went over the wire
}

// a stand in for the easystore service that records the request it is given
func urlCaptureServer(t *testing.T, captured *[]capturedRequest) *httptest.Server {

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		query := make(map[string]string)
		for k, v := range r.URL.Query() {
			query[k] = v[0]
		}

		*captured = append(*captured, capturedRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  query,
			raw:    r.URL.RequestURI(),
		})

		// the healthcheck runs when the proxy is constructed, everything else is
		// happy with an empty success
		w.WriteHeader(http.StatusOK)
	}))
}

// a proxy pointed at the capture server. The first captured request is the healthcheck
// the factory performs, so it is discarded
func testProxy(t *testing.T, captured *[]capturedRequest) EasyStore {

	srv := urlCaptureServer(t, captured)
	t.Cleanup(srv.Close)

	es, err := NewEasyStoreProxy(&ProxyConfigImpl{ServiceEndpoint: srv.URL, ServiceTimeout: 5})
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}
	t.Cleanup(func() { _ = es.Close() })

	// drop the healthcheck
	*captured = (*captured)[:0]
	return es
}

// names that need escaping must survive the trip to the service intact
func TestFileDeleteUrlEscaping(t *testing.T) {

	tests := []struct {
		name      string
		namespace string
		oid       string
		file      string
	}{
		{"a space", goodNamespace, "oid-1", "my file.bin"},
		{"an ampersand", goodNamespace, "oid-1", "rock&roll.bin"},
		{"a hash", goodNamespace, "oid-1", "draft#2.bin"},
		{"a question mark", goodNamespace, "oid-1", "what?.bin"},
		{"a slash", goodNamespace, "oid-1", "a/b.bin"},
		{"a percent", goodNamespace, "oid-1", "100%.bin"},
		{"a plus", goodNamespace, "oid-1", "a+b.bin"},
		{"an awkward oid", goodNamespace, "oid one/two", "file.bin"},
		{"an awkward namespace", "name space", "oid-1", "file.bin"},
	}

	for _, test := range tests {

		var captured []capturedRequest
		es := testProxy(t, &captured)

		// the server returns success so this must not error
		if err := es.FileDelete(test.namespace, test.oid, test.file); err != nil {
			t.Fatalf("%s: expected 'OK' but got '%s'\n", test.name, err)
		}

		if len(captured) != 1 {
			t.Fatalf("%s: expected 1 request but got %d\n", test.name, len(captured))
		}

		req := captured[0]
		if req.method != "DELETE" {
			t.Fatalf("%s: expected method 'DELETE' but got '%s'\n", test.name, req.method)
		}

		expected := fmt.Sprintf("/%s/%s/file/%s", test.namespace, test.oid, test.file)
		if req.path != expected {
			t.Fatalf("%s: expected path '%s' but got '%s' (wire [%s])\n", test.name, expected, req.path, req.raw)
		}
	}
}

// the rename target travels as a query value so it needs escaping too
func TestFileRenameUrlEscaping(t *testing.T) {

	tests := []struct {
		name    string
		file    string
		newName string
	}{
		{"spaces both ends", "old file.bin", "new file.bin"},
		{"ampersand in target", "old.bin", "rock&roll.bin"},
		{"hash in target", "old.bin", "draft#2.bin"},
		{"equals in target", "old.bin", "a=b.bin"},
		{"plus in target", "old.bin", "a+b.bin"},
		{"slash in target", "old.bin", "a/b.bin"},
	}

	for _, test := range tests {

		var captured []capturedRequest
		es := testProxy(t, &captured)

		if err := es.FileRename(goodNamespace, "oid-1", test.file, test.newName); err != nil {
			t.Fatalf("%s: expected 'OK' but got '%s'\n", test.name, err)
		}

		if len(captured) != 1 {
			t.Fatalf("%s: expected 1 request but got %d\n", test.name, len(captured))
		}

		req := captured[0]
		expectedPath := fmt.Sprintf("/%s/oid-1/file/%s", goodNamespace, test.file)
		if req.path != expectedPath {
			t.Fatalf("%s: expected path '%s' but got '%s' (wire [%s])\n", test.name, expectedPath, req.path, req.raw)
		}
		if req.query["new"] != test.newName {
			t.Fatalf("%s: expected new name '%s' but got '%s' (wire [%s])\n", test.name, test.newName, req.query["new"], req.raw)
		}
	}
}

// the vtag is caller supplied and travels as a query value
func TestObjectDeleteUrlEscaping(t *testing.T) {

	var captured []capturedRequest
	es := testProxy(t, &captured)

	obj := ProxyEasyStoreObject("name space", "oid one", "vtag one&two")

	// the server returns an empty body which cannot be deserialized into an object,
	// we only care about the request that was made
	_, _ = es.ObjectDelete(obj, BaseComponent)

	if len(captured) != 1 {
		t.Fatalf("expected 1 request but got %d\n", len(captured))
	}

	req := captured[0]
	if req.path != "/name space/oid one" {
		t.Fatalf("expected path '/name space/oid one' but got '%s' (wire [%s])\n", req.path, req.raw)
	}
	if req.query["vtag"] != "vtag one&two" {
		t.Fatalf("expected vtag 'vtag one&two' but got '%s' (wire [%s])\n", req.query["vtag"], req.raw)
	}
}

// the component attributes must still arrive alongside an escaped path
func TestObjectGetUrlEscaping(t *testing.T) {

	var captured []capturedRequest
	es := testProxy(t, &captured)

	_, _ = es.ObjectGetByKey("name space", "oid one", AllComponents)

	if len(captured) != 1 {
		t.Fatalf("expected 1 request but got %d\n", len(captured))
	}

	req := captured[0]
	if req.path != "/name space/oid one" {
		t.Fatalf("expected path '/name space/oid one' but got '%s' (wire [%s])\n", req.path, req.raw)
	}
	if req.query["attribs"] != "all" {
		t.Fatalf("expected attribs 'all' but got '%s' (wire [%s])\n", req.query["attribs"], req.raw)
	}
}

//
// end of file
//
