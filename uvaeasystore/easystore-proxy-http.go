package uvaeasystore

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

var maxHttpRetries = 3
var retrySleepTime = 100 * time.Millisecond
var jsonContentType = "application/json"

func newHTTPClient(timeout int) *http.Client {
	return &http.Client{
		Transport: newHTTPTransport(),
		Timeout:   time.Duration(timeout) * time.Second,
	}
}

// newStreamingHTTPClient -- a client for streaming (potentially large) payloads. We do not
// impose an overall request timeout because the time taken depends on the payload size;
// establishing the connection is still bounded by the transport
func newStreamingHTTPClient() *http.Client {
	return &http.Client{
		Transport: newHTTPTransport(),
	}
}

func newHTTPTransport() *http.Transport {
	return &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 15 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 2 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
	}
}

func httpGet(client *http.Client, url string) ([]byte, error) {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("ERROR: GET %s failed with error (%s)\n", url, err)
		return nil, err
	}

	return httpSend(client, req)
}

func httpDelete(client *http.Client, url string) ([]byte, error) {

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		fmt.Printf("ERROR: DELETE %s failed with error (%s)\n", url, err)
		return nil, err
	}

	return httpSend(client, req)
}

func httpPost(client *http.Client, url string, payload []byte, contentType string) ([]byte, error) {

	reader := bytes.NewReader(payload)
	req, err := http.NewRequest("POST", url, reader)
	if err != nil {
		fmt.Printf("ERROR: POST %s failed with error (%s)\n", url, err)
		return nil, err
	}

	// if we specify the content type, add the content type header
	if len(contentType) != 0 {
		req.Header.Add("content-type", contentType)
	}

	return httpSend(client, req)
}

func httpPut(client *http.Client, url string, payload []byte, contentType string) ([]byte, error) {

	reader := bytes.NewReader(payload)
	req, err := http.NewRequest("PUT", url, reader)
	if err != nil {
		fmt.Printf("ERROR: PUT %s failed with error (%s)\n", url, err)
		return nil, err
	}

	// if we specify the content type, add the content type header
	if len(contentType) != 0 {
		req.Header.Add("content-type", contentType)
	}

	return httpSend(client, req)
}

func httpPostStream(client *http.Client, url string, payload io.Reader, contentType string) ([]byte, error) {
	return httpSendStream(client, "POST", url, payload, contentType)
}

func httpPutStream(client *http.Client, url string, payload io.Reader, contentType string) ([]byte, error) {
	return httpSendStream(client, "PUT", url, payload, contentType)
}

// httpSendStream -- send the payload as a stream rather than a buffer. Note that we cannot
// retry these requests because the payload has (at least partially) been consumed
func httpSendStream(client *http.Client, method string, url string, payload io.Reader, contentType string) ([]byte, error) {

	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		fmt.Printf("ERROR: %s %s failed with error (%s)\n", method, url, err)
		return nil, err
	}

	// if we specify the content type, add the content type header
	if len(contentType) != 0 {
		req.Header.Add("content-type", contentType)
	}

	response, err := client.Do(req)
	if err != nil {
		fmt.Printf("ERROR: %s %s failed with error (%s)\n", method, url, err)
		return nil, err
	}

	return httpResponse(req, response)
}

func httpSend(client *http.Client, req *http.Request) ([]byte, error) {

	var response *http.Response
	var err error
	url := req.URL.String()
	count := 0
	for {
		//start := time.Now()
		response, err = client.Do(req)
		//duration := time.Since(start)
		//fmt.Printf("INFO: %s %s (elapsed %d ms)\n", req.Method, url, duration.Milliseconds())

		count++
		if err != nil {
			if canRetry(err) == false {
				fmt.Printf("ERROR: %s %s failed with error (%s)\n", req.Method, url, err)
				return nil, err
			}

			// break when tried too many times
			if count >= maxHttpRetries {
				return nil, err
			}

			fmt.Printf("ERROR: %s %s failed with error, retrying (%s)\n", req.Method, url, err)

			// sleep for a bit before retrying
			time.Sleep(retrySleepTime)
		} else {
			return httpResponse(req, response)
		}
	}
}

// httpResponse -- process the response payload, the response body is closed here
func httpResponse(req *http.Request, response *http.Response) ([]byte, error) {

	defer response.Body.Close()
	url := req.URL.String()

	if response.StatusCode >= 300 {
		logLevel := "ERROR"
		// log StatusNotFound as informational instead of as an error
		switch response.StatusCode {
		case http.StatusNotFound: // object/file not found, not really an error
			logLevel = "INFO"
		case http.StatusConflict: // stale object, not really an error
			logLevel = "WARNING"
		}
		fmt.Printf("%s: %s %s failed with status %d\n", logLevel, req.Method, url, response.StatusCode)

		body, _ := io.ReadAll(response.Body)
		//fmt.Printf("DEBUG: RESP: [%s]\n", string(body))

		return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("DEBUG: RESP: [%s]\n", string(body))
	return body, nil
}

// examines the error and decides if it can be retried
func canRetry(err error) bool {

	if strings.Contains(err.Error(), "operation timed out") == true {
		return true
	}

	if strings.Contains(err.Error(), "Client.Timeout exceeded") == true {
		return true
	}

	if strings.Contains(err.Error(), "write: broken pipe") == true {
		return true
	}

	if strings.Contains(err.Error(), "no such host") == true {
		return true
	}

	if strings.Contains(err.Error(), "network is down") == true {
		return true
	}

	return false
}

//
// end of file
//
