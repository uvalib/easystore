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
		return impl.proxy.GetByKey(obj.Namespace(), obj.Id(), impl.which)
	}

	return &obj, nil
}

// factory for our easystore interface
func newEasyStoreProxy(config EasyStoreProxyConfig) (EasyStore, error) {
	logInfo(config.Logger(), fmt.Sprintf("new easystore proxy"))
	i := easyStoreProxyImpl{easyStoreProxyReadonlyImpl{config: config, HTTPClient: newHTTPClient(config.Timeout())}}
	return i, i.Check()
}

func newEasyStoreProxyReadonly(config EasyStoreProxyConfig) (EasyStoreReadonly, error) {
	logInfo(config.Logger(), fmt.Sprintf("new easystore readonly proxy"))
	i := easyStoreProxyReadonlyImpl{config: config, HTTPClient: newHTTPClient(config.Timeout())}
	return i, i.Check()
}

func (impl easyStoreProxyImpl) Create(obj EasyStoreObject) (EasyStoreObject, error) {

	// preflight validation
	if err := CreatePreflight(obj); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("creating new ns/oid [%s/%s]", obj.Namespace(), obj.Id()))

	// create the request payload
	reqBytes, err := json.Marshal(obj)
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

	return &resp, nil
}

func (impl easyStoreProxyImpl) Update(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// preflight validation
	if err := UpdatePreflight(obj, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list
	attribs := impl.componentHelper(which)
	if len(attribs) != 0 {
		attribs = fmt.Sprintf("?%s", attribs)
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("updating ns/oid [%s/%s]", obj.Namespace(), obj.Id()))

	// create the request payload
	reqBytes, err := json.Marshal(obj)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return nil, ErrSerialize
	}

	//log.Printf("REQ: [%s]", string(reqBytes))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s", impl.config.Endpoint(), obj.Namespace(), obj.Id(), attribs)
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

	return &resp, nil
}

func (impl easyStoreProxyImpl) Delete(obj EasyStoreObject, which EasyStoreComponents) (EasyStoreObject, error) {

	// preflight validation
	if err := DeletePreflight(obj, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list
	attribs := impl.componentHelper(which)
	if len(attribs) != 0 {
		attribs = fmt.Sprintf("?%s", attribs)
	}

	// add the vtag parameter
	vtag := fmt.Sprintf("vtag=%s", obj.VTag())
	if len(attribs) == 0 {
		vtag = fmt.Sprintf("?%s", vtag)
	} else {
		vtag = fmt.Sprintf("&%s", vtag)
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("deleting ns/oid [%s/%s]", obj.Namespace(), obj.Id()))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s%s", impl.config.Endpoint(), obj.Namespace(), obj.Id(), attribs, vtag)
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

func (impl easyStoreProxyImpl) Rename(obj EasyStoreObject, name string, newName string) (EasyStoreObject, error) {

	// preflight validation
	if err := RenamePreflight(obj, name, newName); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	return nil, ErrNotImplemented
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

func (impl easyStoreProxyReadonlyImpl) GetByKey(namespace string, id string, which EasyStoreComponents) (EasyStoreObject, error) {

	// preflight validation
	if err := GetByKeyPreflight(namespace, id, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list
	attribs := impl.componentHelper(which)
	if len(attribs) != 0 {
		attribs = fmt.Sprintf("?%s", attribs)
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("getting ns/oid [%s/%s]", namespace, id))

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s", impl.config.Endpoint(), namespace, id, attribs)
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

func (impl easyStoreProxyReadonlyImpl) GetByKeys(namespace string, ids []string, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// preflight validation
	if err := GetByKeysPreflight(namespace, ids, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list. We get these items lazily so just request the base component for now
	attribs := impl.componentHelper(BaseComponent)
	if len(attribs) != 0 {
		attribs = fmt.Sprintf("?%s", attribs)
	}

	logInfo(impl.config.Logger(), fmt.Sprintf("getting ns/oid's [%s/%s]", namespace, strings.Join(ids, ",")))

	// create the request payload
	var req getObjectsRequest
	req.Ids = ids
	reqBytes, err := json.Marshal(req)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return nil, ErrSerialize
	}

	// issue the request
	url := fmt.Sprintf("%s/%s%s", impl.config.Endpoint(), namespace, attribs)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	// process the response payload
	var resp getObjectsResponse
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

func (impl easyStoreProxyReadonlyImpl) GetByFields(namespace string, fields EasyStoreObjectFields, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// preflight validation
	if err := GetByFieldsPreflight(namespace, fields, which); err != nil {
		logError(impl.config.Logger(), "preflight failure")
		return nil, err
	}

	// build the attributes list. We get these items lazily so just request the base component for now
	attribs := impl.componentHelper(BaseComponent)
	if len(attribs) != 0 {
		attribs = fmt.Sprintf("?%s", attribs)
	}

	logDebug(impl.config.Logger(), fmt.Sprintf("getting by fields ns/fields [%s/%v]", namespace, fields))

	// create the request payload
	reqBytes, err := json.Marshal(fields)
	if err != nil {
		log.Printf("ERROR: Unable to marshal request (%s)", err.Error())
		return nil, ErrSerialize
	}

	// issue the request
	url := fmt.Sprintf("%s/%s/search%s", impl.config.Endpoint(), namespace, attribs)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		if len(respBytes) > 0 {
			//log.Printf("RESP: [%s]", string(respBytes))
			return nil, mapResponseToError(string(respBytes))
		}
		return nil, err
	}

	// process the response payload
	var resp searchObjectsResponse
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

//
// end of file
//
