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
	Index     int
	ObjectSet []easyStoreObjectImpl
}

func (impl *easyStoreProxyObjectSetImpl) Count() uint {
	return uint(len(impl.ObjectSet))
}

func (impl *easyStoreProxyObjectSetImpl) Next() (EasyStoreObject, error) {
	if impl.Index == len(impl.ObjectSet) {
		return nil, io.EOF
	}
	obj := impl.ObjectSet[impl.Index]
	impl.Index++
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

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
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

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object Namespace_/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
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

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object Namespace_/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
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

	// validate the object
	if obj == nil {
		return nil, ErrBadParameter
	}

	// validate the object namespace/id
	if len(obj.Namespace()) == 0 {
		return nil, ErrBadParameter
	}
	if len(obj.Id()) == 0 {
		return nil, ErrBadParameter
	}

	// validate the vtag is included
	if len(obj.VTag()) == 0 {
		return nil, ErrBadParameter
	}

	// ensure our inputs are good
	if len(name) == 0 {
		return nil, ErrBadParameter
	}
	if len(newName) == 0 {
		return nil, ErrBadParameter
	}

	// ensure we actually have files
	files := obj.Files()
	if files == nil {
		return nil, ErrBadParameter
	}
	// and we have one named as specified and not one named as its replacement
	found := false
	duplicate := false
	for _, file := range files {
		if file.Name() == name {
			found = true
		}
		if file.Name() == newName {
			duplicate = true
		}
	}
	if found == false || duplicate == true {
		return nil, ErrBadParameter
	}

	return nil, ErrNotImplemented
}

func (impl easyStoreProxyReadonlyImpl) Close() error {
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

	// validate the id
	if len(id) == 0 {
		return nil, ErrBadParameter
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
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

	// validate the id list
	if len(ids) == 0 {
		return nil, ErrBadParameter
	}

	// validate each member
	for _, id := range ids {
		if len(id) == 0 {
			return nil, ErrBadParameter
		}
	}

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	// build the attributes list
	attribs := impl.componentHelper(which)
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
	return &easyStoreProxyObjectSetImpl{0, resp.Results}, nil
}

func (impl easyStoreProxyReadonlyImpl) GetByFields(namespace string, fields EasyStoreObjectFields, which EasyStoreComponents) (EasyStoreObjectSet, error) {

	// validate the component request
	if which > AllComponents {
		return nil, ErrBadParameter
	}

	// build the attributes list
	attribs := impl.componentHelper(which)
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
	return &easyStoreProxyObjectSetImpl{0, resp.Results}, nil
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
