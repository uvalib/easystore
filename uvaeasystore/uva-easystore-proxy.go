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

	return nil, ErrNotImplemented

	//	// add the object
	//	err := impl.store.AddObject(obj)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	// do we add metadata
	//	if obj.Metadata() != nil {
	//		logDebug(impl.config.Logger(), fmt.Sprintf("adding metadata for ns/oid [%s/%s]", obj.Namespace_(), obj.Id()))
	//		err = impl.store.AddMetadata(DataStoreKey{obj.Namespace_(), obj.Id()}, obj.Metadata())
	//		if err != nil {
	//			return nil, err
	//		}
	//	}
	//
	//	// do we add fields
	//	if len(obj.Fields()) != 0 {
	//		logDebug(impl.config.Logger(), fmt.Sprintf("adding fields for ns/oid [%s/%s]", obj.Namespace_(), obj.Id()))
	//		err = impl.store.AddFields(DataStoreKey{obj.Namespace_(), obj.Id()}, obj.Fields())
	//		if err != nil {
	//			return nil, err
	//		}
	//	}
	//
	//	// do we add files
	//	if len(obj.Files()) != 0 {
	//		logDebug(impl.config.Logger(), fmt.Sprintf("adding files for ns/oid [%s/%s]", obj.Namespace_(), obj.Id()))
	//		for _, b := range obj.Files() {
	//			err = impl.store.AddBlob(DataStoreKey{obj.Namespace_(), obj.Id()}, b)
	//			if err != nil {
	//				return nil, err
	//			}
	//		}
	//	}
	//
	//	// publish the appropriate event, errors are not too important
	//	err = pubObjectCreate(impl.messageBus, obj)
	//	if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
	//		logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
	//	}
	//
	//	// get the full object
	//	return impl.GetByKey(obj.Namespace_(), obj.Id(), AllComponents)
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

	return nil, ErrNotImplemented

	//	// get the current object and compare the vtag
	//	current, err := impl.GetByKey(obj.Namespace_(), obj.Id(), BaseComponent)
	//	if err != nil {
	//		return nil, err
	//	}
	//	if current.VTag() != obj.VTag() {
	//		return nil, ErrStaleObject
	//	}
	//
	//	// do we update fields
	//	if (which & Fields) == Fields {
	//		logDebug(impl.config.Logger(), fmt.Sprintf("updating fields for ns/oid [%s/%s]", obj.Namespace_(), obj.Id()))
	//		// delete the current fields
	//		err := impl.store.DeleteFieldsByKey(DataStoreKey{obj.Namespace_(), obj.Id()})
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		// if we have new fields, add them
	//		if len(obj.Fields()) != 0 {
	//			err := impl.store.AddFields(DataStoreKey{obj.Namespace_(), obj.Id()}, obj.Fields())
	//			if err != nil {
	//				return nil, err
	//			}
	//		}
	//	}
	//
	//	// do we update files
	//	if (which & Files) == Files {
	//		logDebug(impl.config.Logger(), fmt.Sprintf("updating files for ns/oid [%s/%s]", obj.Namespace_(), obj.Id()))
	//		// delete the current files
	//		err := impl.store.DeleteBlobsByKey(DataStoreKey{obj.Namespace_(), obj.Id()})
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		// if we have new files, add them
	//		if len(obj.Files()) != 0 {
	//			for _, b := range obj.Files() {
	//				err = impl.store.AddBlob(DataStoreKey{obj.Namespace_(), obj.Id()}, b)
	//				if err != nil {
	//					return nil, err
	//				}
	//
	//				// publish the appropriate event, errors are not too important
	//				err = pubFileCreate(impl.messageBus, obj)
	//				if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
	//					logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
	//				}
	//			}
	//		}
	//	}
	//
	//	// do we update metadata
	//	if (which & Metadata) == Metadata {
	//		logDebug(impl.config.Logger(), fmt.Sprintf("updating metadata for ns/oid [%s/%s]", obj.Namespace_(), obj.Id()))
	//		// delete the current metadata
	//		err := impl.store.DeleteMetadataByKey(DataStoreKey{obj.Namespace_(), obj.Id()})
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		// if we have new metadata, add it
	//		if obj.Metadata() != nil {
	//			err := impl.store.AddMetadata(DataStoreKey{obj.Namespace_(), obj.Id()}, obj.Metadata())
	//			if err != nil {
	//				return nil, err
	//			}
	//
	//			// publish the appropriate event, errors are not too important
	//			err = pubMetadataUpdate(impl.messageBus, obj)
	//			if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
	//				logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
	//			}
	//		}
	//	}
	//
	//	// update the object (timestamp and vtag)
	//	err = impl.store.UpdateObject(DataStoreKey{obj.Namespace_(), obj.Id()})
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	// publish the appropriate event, errors are not too important
	//	err = pubObjectUpdate(impl.messageBus, obj)
	//	if err != nil && errors.Is(err, ErrBusNotConfigured) == false {
	//		logError(impl.config.Logger(), fmt.Sprintf("publishing event (%s)", err.Error()))
	//	}
	//
	//	// get the full object
	//	return impl.GetByKey(obj.Namespace_(), obj.Id(), AllComponents)
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

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s%s", impl.config.Endpoint(), obj.Namespace(), obj.Id(), attribs, vtag)
	_, err := httpDelete(impl.HTTPClient, url)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (impl easyStoreProxyReadonlyImpl) Close() error {
	return nil
}

func (impl easyStoreProxyReadonlyImpl) Check() error {
	url := fmt.Sprintf("%s/healthcheck", impl.config.Endpoint())
	_, err := httpGet(impl.HTTPClient, url)
	return err
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

	// issue the request
	url := fmt.Sprintf("%s/%s/%s%s", impl.config.Endpoint(), namespace, id, attribs)
	respBytes, err := httpGet(impl.HTTPClient, url)
	if err != nil {
		return nil, err
	}

	// process the response
	var resp easyStoreObjectImpl
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, err
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

	// create the request structure
	var req getObjectsRequest
	req.Ids = ids
	reqBytes, _ := json.Marshal(req)

	// issue the request
	url := fmt.Sprintf("%s/%s%s", impl.config.Endpoint(), namespace, attribs)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		return nil, err
	}

	// process the response
	var resp getObjectsResponse
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, err
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

	//logDebug(impl.config.Logger(), fmt.Sprintf("getting by fields"))

	reqBytes, _ := json.Marshal(fields)

	// issue the request
	url := fmt.Sprintf("%s/%s/search%s", impl.config.Endpoint(), namespace, attribs)
	respBytes, err := httpPut(impl.HTTPClient, url, reqBytes, jsonContentType)
	if err != nil {
		return nil, err
	}

	// process the response
	var resp searchObjectsResponse
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("ERROR: Unable to unmarshal response (%s)", err.Error())
		return nil, err
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
