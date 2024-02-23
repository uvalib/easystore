//
//
//

package main

import (
	"errors"
	"fmt"
	"github.com/uvalib/easystore/uvaeasystore"
	"log"
	"os"
)

func makeObjectFromOpen(serializer uvaeasystore.EasyStoreSerializer, indir string) (uvaeasystore.EasyStoreObject, error) {

	buf, err := os.ReadFile(fmt.Sprintf("%s/work.json", indir))
	if err != nil {
		log.Fatalf("ERROR: reading file (%s)", err.Error())
	}

	// import base object
	obj, err := serializer.ObjectDeserialize(buf)
	if err != nil {
		return nil, err
	}

	// import fields
	fields, err := serializer.FieldsDeserialize(buf)
	if err != nil {
		return nil, err
	}
	obj.SetFields(fields)

	// import metadata
	metadata, err := serializer.MetadataDeserialize(buf)
	if err != nil {
		return nil, err
	}
	obj.SetMetadata(metadata)

	// import files if they exist
	buf, err = os.ReadFile(fmt.Sprintf("%s/fileset-001.json", indir))
	if err == nil {

		// for each possible blob file
		blobs := make([]uvaeasystore.EasyStoreBlob, 0)
		ix := 0
		var blob uvaeasystore.EasyStoreBlob
		buf, err = os.ReadFile(fmt.Sprintf("%s/fileset-%03d.json", indir, ix+1))
		for err == nil {

			blob, err = serializer.BlobDeserialize(buf)
			if err != nil {
				return nil, err
			}

			blobs = append(blobs, blob)
			ix++
			buf, err = os.ReadFile(fmt.Sprintf("%s/fileset-%03d.json", indir, ix+1))
		}
		if errors.Is(err, os.ErrNotExist) == true {
			obj.SetFiles(blobs)
			//log.Printf("DEBUG: ==> imported %d blob(s) for [%s]", ix, obj.Id())
		} else {
			return nil, err
		}
	} else {
		if errors.Is(err, os.ErrNotExist) == true {
			log.Printf("WARNING: no files for [%s]", obj.Id())
		} else {
			return nil, err
		}
	}

	return obj, nil
}

//
// end of file
//
