//
//
//

package uvaeasystore

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	neturl "net/url"
	"strings"
)

// ProxyConfigImpl -- this is our proxy configuration implementation
type ProxyConfigImpl struct {
	ServiceEndpoint string      // service endpoint
	ServiceTimeout  int         // service call timeout
	Log             *log.Logger // the logger
}

func (impl ProxyConfigImpl) Logger() *log.Logger {
	return impl.Log
}

func (impl ProxyConfigImpl) SetLogger(log *log.Logger) {
	impl.Log = log
}

func (impl ProxyConfigImpl) Endpoint() string {
	return impl.ServiceEndpoint
}

func (impl ProxyConfigImpl) SetEndpoint(endpoint string) {
	impl.ServiceEndpoint = endpoint
}

func (impl ProxyConfigImpl) Timeout() int {
	return impl.ServiceTimeout
}

func (impl ProxyConfigImpl) SetTimeout(timeout int) {
	impl.ServiceTimeout = timeout
}

// these are our proxy implementations
type easyStoreProxyImpl struct {
	StreamClient *http.Client // used when we stream a file payload
	easyStoreProxyReadonlyImpl
}

type easyStoreProxyReadonlyImpl struct {
	config     EasyStoreProxyConfig
	HTTPClient *http.Client
}

// this is our object set implementation (different from the native implementation
type easyStoreProxyObjectSetImpl struct {
	current int                        // current object index
	which   EasyStoreComponents        // which components are we requesting
	objects []easyStoreObjectImpl      // object list
	proxy   easyStoreProxyReadonlyImpl // ourself so we can get the next item
}

func (impl *easyStoreProxyObjectSetImpl) Count() uint {
	return uint(len(impl.objects))
}

func (impl *easyStoreProxyObjectSetImpl) Next() (EasyStoreObject, error) {
	if impl.current == len(impl.objects) {
		return nil, io.EOF
	}

	obj := impl.objects[impl.current]
	impl.current++

	// do we need to get any more bits?
	if impl.which > BaseComponent {
		return impl.proxy.ObjectGetByKey(obj.Namespace(), obj.Id(), impl.which)
	}

	return &obj, nil
}

// factory for our easystore interface
func newEasyStoreProxy(config EasyStoreProxyConfig) (EasyStore, error) {
	logInfo(config.Logger(), fmt.Sprintf("new easystore proxy"))
	i := easyStoreProxyImpl{
		StreamClient:               newStreamingHTTPClient(),
		easyStoreProxyReadonlyImpl: easyStoreProxyReadonlyImpl{config: config, HTTPClient: newHTTPClient(config.Timeout())},
	}
	return i, i.Check()
}

func newEasyStoreProxyReadonly(config EasyStoreProxyConfig) (EasyStoreReadonly, error) {
	logInfo(config.Logger(), fmt.Sprintf("new easystore readonly proxy"))
	i := easyStoreProxyReadonlyImpl{config: config, HTTPClient: newHTTPClient(config.Timeout())}
	return i, i.Check()
}

func (impl easyStoreProxyImpl) ObjectCreate(obj EasyStoreObject) (EasyStoreObject, error) {

	// preflight validation
	if err := ObjectCreatePreflight(obj); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("creating new object ns/oid [%s/%s]", obj.Namespace(), obj.Id()))

	// streamed files cannot be included in the request payload, they are sent separately
	sendObj, streamed, err := extractStreamedFiles(obj)
	if err != nil {
		return nil, err
	}

	// create the request payload
	reqBytes, err := json.Marshal(sendObj)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return nil, ErrSerialize
	}

	//log.Printf("REQ: [%s]", string(reqBytes))

	// issue the request
	url := fmt.Sprintf("%s/%s", impl.config.Endpoint(), obj.Namespace())
	respBytes, err := httpPost(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	//log.Printf("RESP: [%s]", string(respBytes))

	// process the response payload
	var resp easyStoreObjectImpl
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, ErrDeserialize
	}

	// now the object exists, stream any remaining files into it
	if len(streamed) != 0 {
		if err = impl.createStreamedFiles(resp.Namespace(), resp.Id(), streamed); err != nil {
			return nil, err
		}

		// the object has changed since it was created (vtag and files), get it again
		return impl.ObjectGetByKey(resp.Namespace(), resp.Id(), AllComponents)
	}

	return &resp, nil
}

func (impl easyStoreProxyImpl) ObjectUpdate(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// preflight validation
	if err := ObjectUpdatePreflight(obj, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list (this is optional)
	attribs := impl.componentHelper(which)

	// build the query parameters
	query := ""
	if len(attribs) != 0 {
		query = fmt.Sprintf("?%s", attribs)
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("updating object ns/oid [%s/%s]", obj.Namespace(), obj.Id()))

	// streamed files cannot be included in the request payload, they are sent separately
	sendObj, streamed, err := extractStreamedFiles(obj)
	if err != nil {
		return nil, err
	}

	// we only send the files when they are being updated, discard any we are not sending
	if (which & Files) != Files {
		closeStreamedFiles(streamed)
		streamed = nil
	}

	// create the request payload
	reqBytes, err := json.Marshal(sendObj)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return nil, ErrSerialize
	}

	//log.Printf("REQ: [%s]", string(reqBytes))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s", impl.config.Endpoint(), obj.Namespace(), obj.Id(), query)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	// process the response payload
	var resp easyStoreObjectImpl
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, ErrDeserialize
	}

	// the update has replaced the files so we stream the remainder in as new ones
	if len(streamed) != 0 {
		if err = impl.createStreamedFiles(resp.Namespace(), resp.Id(), streamed); err != nil {
			return nil, err
		}

		// the object has changed since it was updated (vtag and files), get it again
		return impl.ObjectGetByKey(resp.Namespace(), resp.Id(), AllComponents)
	}

	return &resp, nil
}

func (impl easyStoreProxyImpl) ObjectDelete(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// preflight validation
	if err := ObjectDeletePreflight(obj, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// create the vtag parameter
	vtag := fmt.Sprintf("vtag=%s", obj.VTag())

	// build the attributes list (this is optional)
	attribs := impl.componentHelper(which)

	// build the query parameters
	query := fmt.Sprintf("?%s", vtag)

	// if we have the optional attribute component
	if len(attribs) != 0 {
		query = fmt.Sprintf("%s&%s", query, attribs)
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("deleting object ns/oid [%s/%s]", obj.Namespace(), obj.Id()))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s", impl.config.Endpoint(), obj.Namespace(), obj.Id(), query)
	respBytes, err := httpDelete(impl.HTTPClient, url)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	return nil, nil
}

// create a file
func (impl easyStoreProxyImpl) FileCreate(namespace string, oid string, file EasyStoreBlob) error {

	// preflight validation
	if err := FileCreatePreflight(namespace, oid, file); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return err
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("creating new file for ns/oid [%s/%s]", namespace, oid))

	// stream the payload if that is how it is supplied
	if blobIsStreaming(file) == true {
		return impl.fileStream("POST", namespace, oid, file)
	}

	// create the request payload
	reqBytes, err := json.Marshal(file)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return ErrSerialize
	}

	//log.Printf("REQ: [%s]", string(reqBytes))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s/file", impl.config.Endpoint(), namespace, oid)
	respBytes, err := httpPost(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return mapResponseToError(string(respBytes))
		}
		return err
	}

	return nil
}

// delete a file
func (impl easyStoreProxyImpl) FileDelete(namespace string, oid string, name string) error {

	// preflight validation
	if err := FileDeletePreflight(namespace, oid, name); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return err
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("deleting file ns/oid/name [%s/%s/%s]", namespace, oid, name))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s/file/%s", impl.config.Endpoint(), namespace, oid, name)
	respBytes, err := httpDelete(impl.HTTPClient, url)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return mapResponseToError(string(respBytes))
		}
		return err
	}

	return nil
}

// rename a file, old name, new name
func (impl easyStoreProxyImpl) FileRename(namespace string, oid string, name string, newName string) error {

	// preflight validation
	if err := FileRenamePreflight(namespace, oid, name, newName); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return err
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("renaming file ns/oid/name [%s/%s/%s] -> [%s]", namespace, oid, name, newName))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s/file/%s?new=%s", impl.config.Endpoint(), namespace, oid, name, newName)
	respBytes, err := httpPost(impl.HTTPClient, url, nil, "")
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return mapResponseToError(string(respBytes))
		}
		return err
	}

	return nil
}

// update a file
func (impl easyStoreProxyImpl) FileUpdate(namespace string, oid string, file EasyStoreBlob) error {

	// preflight validation
	if err := FileUpdatePreflight(namespace, oid, file); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return err
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("updating file ns/oid [%s/%s/%s]", namespace, oid, file.Name()))

	// stream the payload if that is how it is supplied
	if blobIsStreaming(file) == true {
		return impl.fileStream("PUT", namespace, oid, file)
	}

	// create the request payload
	reqBytes, err := json.Marshal(file)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return ErrSerialize
	}

	//log.Printf("REQ: [%s]", string(reqBytes))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s/file", impl.config.Endpoint(), namespace, oid)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return mapResponseToError(string(respBytes))
		}
		return err
	}

	return nil
}

// fileStream -- create or update a file, streaming the payload as the request body so that
// it is never held in memory in its entirety
func (impl easyStoreProxyImpl) fileStream(method string, namespace string, oid string, file EasyStoreBlob) error {

	reader, err := file.PayloadReader()
	if err != nil {
		return err
	}
	defer reader.Close()

	logDebug(impl.config.Logger(), fmt.Sprintf("streaming file ns/oid/name [%s/%s/%s]", namespace, oid, file.Name()))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s/file/%s/content", impl.config.Endpoint(), namespace, oid, neturl.PathEscape(file.Name()))
	var respBytes []byte
	if method == "POST" {
		respBytes, err = httpPostStream(impl.StreamClient, url, reader, file.MimeType())
	} else {
		respBytes, err = httpPutStream(impl.StreamClient, url, reader, file.MimeType())
	}

	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return mapResponseToError(string(respBytes))
		}
		return err
	}

	return nil
}

// createStreamedFiles -- add each of the streamed files to an existing object
func (impl easyStoreProxyImpl) createStreamedFiles(namespace string, oid string, files []EasyStoreBlob) error {

	for _, f := range files {
		if err := impl.FileCreate(namespace, oid, f); err != nil {
			logError(impl.config.Logger(), fmt.Sprintf("streaming file [%s] for ns/oid [%s/%s] (%s)", f.Name(), namespace, oid, err.Error()))
			return err
		}
	}
	return nil
}

func (impl easyStoreProxyImpl) Close() error {

	// need to do this
	impl.StreamClient.CloseIdleConnections()
	return impl.easyStoreProxyReadonlyImpl.Close()
}

func (impl easyStoreProxyReadonlyImpl) Close() error {

	// need to do this
	impl.HTTPClient.CloseIdleConnections()
	return nil
}

func (impl easyStoreProxyReadonlyImpl) Check() error {
	url := fmt.Sprintf("%s/healthcheck", impl.config.Endpoint())
	respBytes, err := httpGet(impl.HTTPClient, url)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return mapResponseToError(string(respBytes))
		}
		return err
	}
	return nil
}

func (impl easyStoreProxyReadonlyImpl) ObjectGetByKey(namespace string, id string, which EasyStoreComponents) (EasyStoreObject, error) {

	// preflight validation
	if err := GetByKeyPreflight(namespace, id, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list (this is optional)
	attribs := impl.componentHelper(which)

	// build the query parameters
	query := ""
	if len(attribs) != 0 {
		query = fmt.Sprintf("?%s", attribs)
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("getting ns/oid [%s/%s]", namespace, id))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s", impl.config.Endpoint(), namespace, id, query)
	respBytes, err := httpGet(impl.HTTPClient, url)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	// process the response payload
	var resp easyStoreObjectImpl
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, ErrDeserialize
	}

	return &resp, nil
}

func (impl easyStoreProxyReadonlyImpl) ObjectGetByKeys(namespace string, ids []string, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// preflight validation
	if err := GetByKeysPreflight(namespace, ids, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list. We get these items lazily so just request the base component for now
	//attribs := impl.componentHelper(BaseComponent)
	//if len(attribs) != 0 {
	//	attribs = fmt.Sprintf("?%s", attribs)
	//}

	logInfo(impl.config.Logger(), fmt.Sprintf("getting ns/oid's [%s/%s]", namespace, strings.Join(ids, ",")))

	// create the request payload
	var req GetObjectsRequest
	req.Ids = ids
	reqBytes, err := json.Marshal(req)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return nil, ErrSerialize
	}

	// issue the request
	url := fmt.Sprintf("%s/%s", impl.config.Endpoint(), namespace)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	// process the response payload
	var resp GetObjectsResponse
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, ErrDeserialize
	}

	// return results in an iterator object
	return &easyStoreProxyObjectSetImpl{
		current: 0,
		which:   which,
		objects: resp.Results,
		proxy:   impl}, nil
}

func (impl easyStoreProxyReadonlyImpl) ObjectGetByFields(namespace string, fields EasyStoreObjectFields, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// preflight validation
	if err := GetByFieldsPreflight(namespace, fields, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list. We get these items lazily so just request the base component for now
	//attribs := impl.componentHelper(BaseComponent)
	//if len(attribs) != 0 {
	//	attribs = fmt.Sprintf("?%s", attribs)
	//}

	logDebug(impl.config.Logger(), fmt.Sprintf("getting by fields ns/fields [%s/%v]", namespace, fields))

	// create the request payload
	reqBytes, err := json.Marshal(fields)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return nil, ErrSerialize
	}

	// issue the request
	url := fmt.Sprintf("%s/%s/search", impl.config.Endpoint(), namespace)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	// process the response payload
	var resp SearchObjectsResponse
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, ErrDeserialize
	}

	// return results in an iterator object
	return &easyStoreProxyObjectSetImpl{
		current: 0,
		which:   which,
		objects: resp.Results,
		proxy:   impl}, nil
}

func (impl easyStoreProxyReadonlyImpl) FileGetByKey(namespace string, oid string, name string) (EasyStoreBlob, error) {
	return nil, ErrNotImplemented
}

func (impl easyStoreProxyReadonlyImpl) componentHelper(which EasyStoreComponents) string {

	if which == AllComponents {
		return "attribs=all"
	}
	if which == BaseComponent {
		return ""
	}

	components := "attribs="
	if which&Fields == Fields {
		components = components + "fields,"
	}
	if which&Files == Files {
		components = components + "files,"
	}
	if which&Metadata == Metadata {
		components = components + "metadata,"
	}
	return strings.TrimSuffix(components, ",")
}

// extractStreamedFiles -- a streamed payload cannot be represented in the request payload so
// we remove any streamed files from the object (a copy, we do not modify the callers object)
// and return them so they can be sent separately
func extractStreamedFiles(obj EasyStoreObject) (EasyStoreObject, []EasyStoreBlob, error) {

	streamed := make([]EasyStoreBlob, 0)
	buffered := make([]EasyStoreBlob, 0)
	for _, f := range obj.Files() {
		if blobIsStreaming(f) == true {
			streamed = append(streamed, f)
		} else {
			buffered = append(buffered, f)
		}
	}

	// nothing streamed, use the object as it is
	if len(streamed) == 0 {
		return obj, streamed, nil
	}

	impl, ok := obj.(*easyStoreObjectImpl)
	if ok == false {
		return nil, nil, fmt.Errorf("%q: %w", "cast failed, not an easyStoreObjectImpl", ErrBadParameter)
	}

	// interfaces are pointers so clone before we change the file list
	implClone := *impl
	implClone.Files_ = buffered
	return &implClone, streamed, nil
}

// closeStreamedFiles -- close the payload of each of these files, used when we are not
// going to send them
func closeStreamedFiles(files []EasyStoreBlob) {

	for _, f := range files {
		reader, err := f.PayloadReader()
		if err == nil {
			reader.Close()
		}
	}
}

//
// end of file
//
