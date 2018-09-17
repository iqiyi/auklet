// Copyright (c) 2015 Rackspace
// Copyright (c) 2016-2018 iQIYI.com.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectserver

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/middleware"
	"github.com/iqiyi/auklet/common/pickle"
	"github.com/iqiyi/auklet/common/srv"
	"github.com/iqiyi/auklet/objectserver/engine"
	"github.com/iqiyi/auklet/objectserver/engine/pack"
	"go.uber.org/zap"
)

func (s *ObjectServer) newObject(req *http.Request,
	vars map[string]string, needData bool) (engine.Object, error) {
	p := 0
	if pi := req.Header.Get(common.XBackendPolicyIndex); pi != "" {
		var err error
		if p, err = strconv.Atoi(pi); err != nil {
			s.logger.Error("malformed engine index", zap.Error(err))
			return nil, err
		}
	}
	engine, ok := s.objEngines[p]
	if !ok {
		s.logger.Error("object engine not found", zap.Int("engine-index", p))
		return nil, EngineNotFound
	}
	return engine.New(vars, needData)
}

func resolveEtag(req *http.Request, metadata map[string]string) string {
	etag := metadata["ETag"]
	for _, ph := range strings.Split(
		req.Header.Get(common.XBackendEtagIsAt), ",") {
		ph = strings.Trim(ph, " ")
		altEtag, exists := metadata[http.CanonicalHeaderKey(ph)]
		if exists && ph != "" {
			etag = altEtag
		}
	}

	return etag
}

func (s *ObjectServer) ObjGetHandler(w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)
	headers := w.Header()
	// Simple solution to indicate that we need to migrate a object
	vars["quse-migration"] = "yes"
	obj, err := s.newObject(req, vars, req.Method == http.MethodGet)
	if err != nil {
		s.logger.Error("unable to open object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	ifMatches := common.ParseIfMatch(req.Header.Get(common.IfMatch))
	ifNoneMatches := common.ParseIfMatch(req.Header.Get(common.IfNoneMatch))

	if !obj.Exists() {
		if ifMatches["*"] {
			common.StandardResponse(w, http.StatusPreconditionFailed)
		} else {
			common.StandardResponse(w, http.StatusNotFound)
		}
		return
	}

	metadata := obj.Metadata()
	etag := resolveEtag(req, metadata)

	headers.Set(common.XBackendTimestamp, metadata[common.XTimestamp])
	if deleteAt, ok := metadata[common.XDeleteAt]; ok {
		deleteTime, err := common.ParseDate(deleteAt)
		if err == nil && deleteTime.Before(time.Now()) {
			common.StandardResponse(w, http.StatusNotFound)
			return
		}
	}

	lastModified, err := common.ParseDate(metadata[common.XTimestamp])
	if err != nil {
		s.logger.Error("error getting timestamp", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	headers.Set(common.LastModified, common.FormatLastModified(lastModified))
	headers.Set(common.HEtag, fmt.Sprintf("\"%s\"", etag))
	timestamp, err := common.GetEpochFromTimestamp(metadata[common.XTimestamp])
	if err != nil {
		s.logger.Error("unable to get the epoch time from x-timestamp",
			zap.Error(err))
		// X-Timestamp comes from proxy server, thus server error returned
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}

	headers.Set(common.XTimestamp, timestamp)
	for k, v := range metadata {
		if s.isHeaderAllowed(k) {
			headers.Set(k, v)
		}
	}

	if len(ifMatches) > 0 && !ifMatches[etag] && !ifMatches["*"] {
		common.StandardResponse(w, http.StatusPreconditionFailed)
		return
	}

	if len(ifNoneMatches) > 0 && (ifNoneMatches[etag] || ifNoneMatches["*"]) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	ius, err := common.ParseDate(req.Header.Get(common.IfUnmodifiedSince))
	if err == nil && lastModified.After(ius) {
		common.StandardResponse(w, http.StatusPreconditionFailed)
		return
	}

	ims, err := common.ParseDate(req.Header.Get(common.IfModifiedSince))
	if err == nil && lastModified.Before(ims) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	headers.Set(common.HAcceptRanges, common.VBytes)
	headers.Set(common.HContentType, metadata[common.HContentType])
	headers.Set(common.HContentLength, metadata[common.HContentLength])

	if rangeHeader := req.Header.Get(common.HRange); rangeHeader != "" {
		ranges, err := common.ParseRange(rangeHeader, obj.ContentLength())
		if err != nil {
			headers.Set(common.HContentLength, "0")
			headers.Set(
				common.HContentRange, fmt.Sprintf("bytes */%d", obj.ContentLength()))

			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}

		if ranges != nil && len(ranges) == 1 {
			headers.Set(common.HContentLength,
				strconv.FormatInt(int64(ranges[0].End-ranges[0].Start), 10))

			r := fmt.Sprintf(
				"bytes %d-%d/%d", ranges[0].Start, ranges[0].End-1, obj.ContentLength())

			headers.Set(common.HContentRange, r)
			w.WriteHeader(http.StatusPartialContent)
			obj.CopyRange(w, ranges[0].Start, ranges[0].End)
			return
		}

		if ranges != nil && len(ranges) > 1 {
			mw := common.NewMultiWriter(
				w, metadata[common.HContentType], obj.ContentLength())
			defer mw.Close()
			for _, rng := range ranges {
				mw.Expect(rng.Start, rng.End)
			}
			headers.Set(
				common.HContentLength, strconv.FormatInt(mw.ContentLength(), 10))
			headers.Set(
				common.HContentType, "multipart/byteranges;boundary="+mw.Boundary())
			w.WriteHeader(http.StatusPartialContent)
			for _, rng := range ranges {
				part, err := mw.CreatePart(rng.Start, rng.End)
				if err != nil {
					s.logger.Error("unable to create multi part", zap.Error(err))
					return
				}

				if _, err = obj.CopyRange(part, rng.Start, rng.End); err != nil {
					s.logger.Error("unable to copy range content", zap.Error(err))
				}
			}
			return
		}
	}

	// This help to log the status code correctly.
	// If not called explicitly, Go http library will send the OK by default,
	// but the status won't be captured by the request logger.
	w.WriteHeader(http.StatusOK)

	if req.Method == http.MethodHead {
		return
	}

	if s.checkEtags {
		hash := md5.New()
		obj.Copy(w, hash)
		if hex.EncodeToString(hash.Sum(nil)) != metadata[common.HEtag] {
			obj.Quarantine()
		}
	} else {
		if _, err := obj.Copy(w); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func (s *ObjectServer) ObjPutHandler(w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)
	outHeaders := w.Header()

	timestamp, err := common.StandardizeTimestamp(
		req.Header.Get(common.XTimestamp))
	if err != nil {
		s.logger.Error("unable to standardize X-Timestamp header", zap.Error(err))
		common.StandardResponse(w, http.StatusBadRequest)
		return
	}

	if req.Header.Get(common.HContentType) == "" {
		common.CustomResponse(w, http.StatusBadRequest, ReqContentTypeMissed)
		return
	}

	if deleteAt := req.Header.Get(common.XDeleteAt); deleteAt != "" {
		deleteTime, err := common.ParseDate(deleteAt)
		if err != nil || deleteTime.Before(time.Now()) {
			common.CustomResponse(w, http.StatusBadRequest, ReqDeleteInPass)
			return
		}
	}

	obj, err := s.newObject(req, vars, false)
	if err != nil {
		s.logger.Error("unable to open object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	if obj.Exists() {
		inm := req.Header.Get(common.IfNoneMatch)
		if inm == "*" {
			common.StandardResponse(w, http.StatusPreconditionFailed)
			return
		}

		metadata := obj.Metadata()
		if ts, err := common.ParseDate(timestamp); err == nil {
			lastModified, err := common.ParseDate(metadata[common.XTimestamp])
			if err == nil && !ts.After(lastModified) {
				outHeaders.Set(common.XBackendTimestamp, metadata[common.XTimestamp])
				common.StandardResponse(w, http.StatusConflict)
				return
			}
		}

		if strings.Contains(inm, metadata[common.XTimestamp]) {
			common.StandardResponse(w, http.StatusPreconditionFailed)
			return
		}
	}

	size := req.ContentLength
	if lenHeader := req.Header.Get(common.XBackendObjLength); lenHeader != "" {
		backendObjLen, err := strconv.ParseInt(lenHeader, 10, 64)
		if err != nil {
			// This header is sent from proxy server, so a 500 should be sent
			common.StandardResponse(w, http.StatusInternalServerError)
			s.logger.Error("invalid header value",
				zap.String("name", common.XBackendObjLength),
				zap.String("value", lenHeader))
			return
		}
		size = backendObjLen
	}

	tempFile, err := obj.SetData(size)
	if err == DriveFull {
		s.logger.Error("not enough space available")
		common.StandardResponse(w, http.StatusInsufficientStorage)
		return
	}

	if err != nil {
		s.logger.Error("unable to create new object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}

	hash := md5.New()
	totalSize, err := common.Copy(req.Body, tempFile, hash)
	if err == io.ErrUnexpectedEOF {
		common.StandardResponse(w, common.StatusClientClosedRequest)
		return
	}
	if err != nil {
		s.logger.Error("unable to write to object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	if size >= 0 && totalSize != size {
		s.logger.Error("incomplete data written",
			zap.Int64("expected", size), zap.Int64("actual", totalSize))
		common.StandardResponse(w, common.StatusClientClosedRequest)
		return
	}

	name := fmt.Sprintf("/%s/%s/%s",
		vars["account"], vars["container"], vars["obj"])
	metadata := map[string]string{
		"name":                name,
		common.XTimestamp:     timestamp,
		common.HContentType:   req.Header.Get(common.HContentType),
		common.HContentLength: strconv.FormatInt(totalSize, 10),
		common.HEtag:          hex.EncodeToString(hash.Sum(nil)),
	}
	for k := range req.Header {
		if s.isHeaderAllowed(k) {
			metadata[k] = req.Header.Get(k)
		}
	}

	etag := strings.Trim(strings.ToLower(req.Header.Get(common.HEtag)), "\"")
	if etag != "" && etag != metadata[common.HEtag] {
		common.StandardResponse(w, http.StatusUnprocessableEntity)
		return
	}
	outHeaders.Set(common.HEtag, metadata[common.HEtag])

	if err := obj.Commit(metadata); err != nil {
		s.logger.Error("unable to commit object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	s.containerUpdates(w, req, metadata, req.Header.Get(common.XDeleteAt), vars)
	common.StandardResponse(w, http.StatusCreated)
}

func (s *ObjectServer) ObjPostHandler(
	w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)
	// Simple solution to indicate that we need to migrate a object
	vars["quse-migration"] = "yes"

	timestamp, err := common.StandardizeTimestamp(
		req.Header.Get(common.XTimestamp))
	if err != nil {
		s.logger.Error("unable to standardize X-Timestamp", zap.Error(err))
		common.StandardResponse(w, http.StatusBadRequest)
		return
	}

	var deleteAt time.Time
	if xDelAt := req.Header.Get(common.XDeleteAt); xDelAt != "" {
		deleteAt, err = common.ParseDate(xDelAt)
		if err != nil || deleteAt.Before(time.Now()) {
			common.CustomResponse(w, http.StatusBadRequest, ReqDeleteInPass)
			return
		}
	}

	obj, err := s.newObject(req, vars, false)
	if err != nil {
		s.logger.Error("unable to open object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	defer obj.Close()
	if !obj.Exists() {
		common.StandardResponse(w, http.StatusNotFound)
		return
	}

	orig := obj.Metadata()
	if time, err := common.ParseDate(timestamp); err == nil {
		modified, err := common.ParseDate(orig[common.XTimestamp])
		if err == nil && !time.After(modified) {
			w.Header().Set(common.XBackendTimestamp, orig[common.XTimestamp])
			common.StandardResponse(w, http.StatusConflict)
			return
		}
	}
	ct := req.Header.Get(common.HContentType)
	if ct != "" && ct != orig[common.HContentType] {
		common.CustomResponse(w, http.StatusConflict, ReqContentTypeNotAllowed)
		return
	}

	metadata := make(map[string]string)
	//N.B. This behaviour is different from the Swift Kilo.
	// In Swift Kilo, x-static-large-object will be taken like simple
	// normal user meta
	if v, ok := orig[common.XStaticLargeObject]; ok {
		metadata[common.XStaticLargeObject] = v
	}

	copyHdrs := map[string]bool{}
	for _, v := range strings.Fields(
		req.Header.Get(common.XBackendReplicationHeaders)) {
		copyHdrs[v] = true
	}

	for k := range req.Header {
		if s.isHeaderAllowed(k) || copyHdrs[k] {
			metadata[k] = req.Header.Get(k)
		}
	}
	metadata["name"] = fmt.Sprintf("/%s/%s/%s",
		vars["account"], vars["container"], vars["obj"])
	metadata[common.XTimestamp] = timestamp
	// TODO: should we check if object has expired yet or not?
	var origDeleteAt time.Time
	if oda := orig[common.XDeleteAt]; oda != "" {
		if origDeleteAt, err = common.ParseDate(oda); err != nil {
			origDeleteAt = time.Time{}
		}
	}
	if !deleteAt.Equal(origDeleteAt) {
		if !deleteAt.IsZero() {
			s.updateDeleteAt(http.MethodPut, req.Header, deleteAt, vars)
		}
		if !origDeleteAt.IsZero() {
			s.updateDeleteAt(
				http.MethodDelete, req.Header, origDeleteAt, vars)
		}
	}

	if err := obj.CommitMeta(metadata); err != nil {
		s.logger.Error("unable to commit object meta", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}

	common.StandardResponse(w, http.StatusAccepted)
}

func (s *ObjectServer) ObjDeleteHandler(
	w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)
	// Simple solution to indicate that we need to migrate a object
	vars["quse-migration"] = "yes"
	headers := w.Header()
	timestamp, err := common.StandardizeTimestamp(
		req.Header.Get(common.XTimestamp))
	if err != nil {
		s.logger.Error("unable to standardize X-Timestamp", zap.Error(err))
		common.CustomResponse(w, http.StatusBadRequest, ReqInvalidTimestamp)
		return
	}
	obj, err := s.newObject(req, vars, false)
	if err != nil {
		s.logger.Error("unable to open object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	if ida := req.Header.Get(common.XIfDeleteAt); ida != "" {
		_, err = strconv.ParseInt(ida, 10, 64)
		if err != nil {
			common.StandardResponse(w, http.StatusBadRequest)
			return
		}
		if !obj.Exists() {
			common.StandardResponse(w, http.StatusNotFound)
			return
		}
		metadata := obj.Metadata()
		if _, ok := metadata[common.XDeleteAt]; ok {
			if ida != metadata[common.XDeleteAt] {
				common.StandardResponse(w, http.StatusPreconditionFailed)
				return
			}
		} else {
			common.StandardResponse(w, http.StatusPreconditionFailed)
			return
		}
	}

	status := http.StatusNotFound
	deleteAt := ""
	if obj.Exists() {
		status = http.StatusNoContent
		metadata := obj.Metadata()
		if xda, ok := metadata[common.XDeleteAt]; ok {
			deleteAt = xda
		}
		origTime, ok := metadata[common.XTimestamp]
		if ok && origTime >= timestamp {
			headers.Set(common.XBackendTimestamp, origTime)
			common.StandardResponse(w, http.StatusConflict)
			return
		}
	} else {
		status = http.StatusNotFound
	}

	metadata := map[string]string{
		"name":            common.ObjectName(vars["account"], vars["container"], vars["obj"]),
		common.XTimestamp: timestamp,
	}
	if err := obj.Delete(metadata); err == DriveFull {
		s.logger.Error("not enough space available")
		common.StandardResponse(w, http.StatusInsufficientStorage)
		return
	} else if err != nil {
		s.logger.Error("unable to delete object", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}
	headers.Set(common.XBackendTimestamp, metadata[common.XTimestamp])
	s.containerUpdates(w, req, metadata, deleteAt, vars)
	common.StandardResponse(w, status)
}

func (s *ObjectServer) HealthcheckHandler(
	w http.ResponseWriter, req *http.Request) {
	msg := "OK"
	w.Header().Set(common.HContentLength, strconv.Itoa(len(msg)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(msg))
}

func (s *ObjectServer) ReconHandler(w http.ResponseWriter, req *http.Request) {
	middleware.ReconHandler(s.driveRoot, w, req)
}

func (s *ObjectServer) OptionsHandler(
	w http.ResponseWriter, req *http.Request) {
	middleware.OptionsHandler("object-server", w, req)
}

func (s *ObjectServer) DiskUsageHandler(
	w http.ResponseWriter, req *http.Request) {
	data, err := s.diskInUse.MarshalJSON()
	if err == nil {
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func (s *ObjectServer) ReplicateHandler(
	w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)

	if s.checkMounts {
		devPath := filepath.Join(s.driveRoot, vars["device"])
		if mounted, err := fs.IsMount(devPath); err != nil || mounted != true {
			vars["Method"] = req.Method
			common.StandardResponse(w, http.StatusInsufficientStorage)
			return
		}
	}

	var recalculate []string
	if len(vars["suffixes"]) > 0 {
		recalculate = strings.Split(vars["suffixes"], "-")
	}

	pi := req.Header.Get(common.XBackendPolicyIndex)
	policy, err := strconv.Atoi(pi)
	if err != nil {
		if pi != "" {
			common.StandardResponse(w, http.StatusInternalServerError)
			return
		}

		policy = 0
	}

	engine, ok := s.objEngines[policy]
	if !ok {
		common.CustomResponse(w, http.StatusBadRequest, ReqPolicyNotFound)
		return
	}

	hashes, err := engine.GetHashes(
		vars["device"], vars["partition"], recalculate)

	if err != nil {
		s.logger.Error("unable to get hashes",
			zap.String("device", vars["device"]),
			zap.String("partition", vars["partition"]))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(pickle.PickleDumps(hashes))
}

func (s *ObjectServer) DiffReplicasHandler(
	w http.ResponseWriter, req *http.Request) {
	vars := srv.GetVars(req)

	if s.checkMounts {
		devPath := filepath.Join(s.driveRoot, vars["device"])
		if mounted, err := fs.IsMount(devPath); err != nil || mounted != true {
			vars["Method"] = req.Method
			common.StandardResponse(w, http.StatusInsufficientStorage)
			return
		}
	}

	var err error

	policy := 0
	pi := req.Header.Get(common.XBackendPolicyIndex)
	if pi != "" {
		if policy, err = strconv.Atoi(pi); err != nil {
			// This header comes from proxy server, so server error returned
			common.StandardResponse(w, http.StatusInternalServerError)
			return
		}
	}

	eng, ok := s.objEngines[policy]
	if !ok {
		common.CustomResponse(w, http.StatusBadRequest, ReqPolicyNotFound)
		return
	}

	engine, ok := eng.(*pack.PackEngine)
	if !ok {
		common.CustomResponse(w, http.StatusBadRequest, ReqNotPackEngine)
		return
	}

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		s.logger.Error("unable to read request body", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}

	objs := new(pack.CheckedObjects)
	if err = proto.Unmarshal(b, objs); err != nil {
		s.logger.Error("unable to deserialize request body", zap.Error(err))
		common.CustomResponse(w, http.StatusBadRequest, ReqNotPbData)
		return
	}

	wantedList, err := engine.DiffReplicas(
		vars["device"], vars["partition"], objs.Objects)
	if err != nil {
		s.logger.Error("unable to check objects", zap.Error(err))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}

	wantedObjs := &pack.WantedObjects{
		Objects: wantedList,
	}

	b, err = proto.Marshal(wantedObjs)
	if err != nil {
		s.logger.Error("unable to serialize response body", zap.Binary("body", b))
		common.StandardResponse(w, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}
