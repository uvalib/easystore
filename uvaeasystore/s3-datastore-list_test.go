//
//
//

// only include this file for service builds

//go:build service
// +build service

package uvaeasystore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var listOid = "oid-listtest"

// a stand in for S3 that serves a truncated first page followed by a final one
func listPageServer(t *testing.T, pages [][]string) *httptest.Server {

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// which page is being asked for, the token is the index of the next one
		page := 0
		if tok := r.URL.Query().Get("continuation-token"); len(tok) != 0 {
			if _, err := fmt.Sscanf(tok, "page-%d", &page); err != nil {
				t.Errorf("unexpected continuation token '%s'\n", tok)
			}
		}
		if page >= len(pages) {
			t.Errorf("asked for page %d but only %d exist\n", page, len(pages))
			return
		}

		var body strings.Builder
		body.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
		body.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, key := range pages[page] {
			body.WriteString(fmt.Sprintf("<Contents><Key>%s</Key></Contents>", key))
		}
		if page+1 < len(pages) {
			body.WriteString("<IsTruncated>true</IsTruncated>")
			body.WriteString(fmt.Sprintf("<NextContinuationToken>page-%d</NextContinuationToken>", page+1))
		} else {
			body.WriteString("<IsTruncated>false</IsTruncated>")
		}
		body.WriteString("</ListBucketResult>")

		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body.String()))
	}))
}

// a storage instance pointed at the supplied endpoint instead of real S3
func testS3Storage(t *testing.T, endpoint string) *S3Storage {

	cfg, err := awsconfig.LoadDefaultConfig(
		context.TODO(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""),
		),
	)
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	return &S3Storage{
		serialize: newEasyStoreSerializer(),
		Bucket:    "test-bucket",
		S3Client:  client,
	}
}

// a listing larger than one page must return every key, not just the first page
func TestS3ListPaginates(t *testing.T) {

	// a full page plus a partial one
	first := make([]string, 0, 1000)
	for ix := 0; ix < 1000; ix++ {
		first = append(first, fmt.Sprintf("%s/%s/file-%04d", goodNamespace, listOid, ix))
	}
	second := []string{
		fmt.Sprintf("%s/%s/file-1000", goodNamespace, listOid),
		fmt.Sprintf("%s/%s/file-1001", goodNamespace, listOid),
	}

	srv := listPageServer(t, [][]string{first, second})
	defer srv.Close()

	s := testS3Storage(t, srv.URL)
	keys, err := s.s3List(s.Bucket, fmt.Sprintf("%s/%s", goodNamespace, listOid))
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	expected := len(first) + len(second)
	if len(keys) != expected {
		t.Fatalf("expected %d keys but got %d\n", expected, len(keys))
	}

	// the last key of the last page must be present
	last := second[len(second)-1]
	if keys[len(keys)-1] != last {
		t.Fatalf("expected last key '%s' but got '%s'\n", last, keys[len(keys)-1])
	}
}

// the single page case must still behave
func TestS3ListSinglePage(t *testing.T) {

	only := []string{
		fmt.Sprintf("%s/%s/%s", goodNamespace, listOid, S3ObjectFileName),
		fmt.Sprintf("%s/%s/%s", goodNamespace, listOid, S3FieldsFileName),
	}

	srv := listPageServer(t, [][]string{only})
	defer srv.Close()

	s := testS3Storage(t, srv.URL)
	keys, err := s.s3List(s.Bucket, fmt.Sprintf("%s/%s", goodNamespace, listOid))
	if err != nil {
		t.Fatalf("expected 'OK' but got '%s'\n", err)
	}

	if len(keys) != len(only) {
		t.Fatalf("expected %d keys but got %d\n", len(only), len(keys))
	}
}

//
// end of file
//
