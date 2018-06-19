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
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/middleware"
	"github.com/iqiyi/auklet/common/pickle"
	"go.uber.org/zap"
)

/*This hash is used to represent a zero byte async file that is
  created for an expiring object*/
const (
	zeroByteHash    = "d41d8cd98f00b204e9800998ecf8427e"
	deleteAtAccount = ".expiring_objects"
)

func (s *ObjectServer) hashObjectName(account, container, obj string) string {
	return common.HashObjectName(
		s.hashPrefix, account, container, obj, s.hashSuffix)
}

func (s *ObjectServer) generateVars(
	method, account, container, object, device, policy string) map[string]string {

	vars := map[string]string{}
	vars["method"] = method
	vars["account"] = account
	vars["container"] = container
	vars["object"] = object
	vars["device"] = device
	vars["policy"] = policy

	return vars
}

func (s *ObjectServer) expirerContainer(
	deleteAt time.Time, account, container, obj string) string {
	i := new(big.Int)
	fmt.Sscanf(s.hashObjectName(account, container, obj), "%x", i)
	shardInt := i.Mod(i, big.NewInt(100)).Int64()
	timestamp := (deleteAt.Unix()/s.expiringDivisor)*s.expiringDivisor - shardInt
	if timestamp < 0 {
		timestamp = 0
	} else if timestamp > 9999999999 {
		timestamp = 9999999999
	}
	return fmt.Sprintf("%010d", timestamp)
}

func (s *ObjectServer) sendContainerUpdate(
	host, device, method, partition, account, container, obj string,
	headers http.Header) bool {
	objUrl := fmt.Sprintf("http://%s/%s/%s/%s/%s/%s",
		host,
		device,
		partition,
		common.Urlencode(account),
		common.Urlencode(container),
		common.Urlencode(obj))
	req, err := http.NewRequest(method, objUrl, nil)
	if err != nil {
		s.logger.Error("unable to create container update request", zap.Error(err))
		return false
	}

	req.Header = headers
	resp, err := s.updateClient.Do(req)
	if err != nil {
		s.logger.Error("unable to update container server", zap.Error(err))
		return false
	}
	resp.Body.Close()
	return resp.StatusCode/100 == 2
}

func (s *ObjectServer) saveAsync(
	method, account, container, obj, localDevice string, headers http.Header) {
	hash := s.hashObjectName(account, container, obj)
	asyncFile := filepath.Join(s.driveRoot, localDevice, "async_pending",
		hash[29:32], hash+"-"+headers.Get(common.XTimestamp))
	tempDir := fs.TempDir(s.driveRoot, localDevice)
	data := map[string]interface{}{
		"op":        method,
		"account":   account,
		"container": container,
		"obj":       obj,
		"headers":   common.Headers2Map(headers),
	}
	if os.MkdirAll(filepath.Dir(asyncFile), 0755) == nil {
		writer, err := fs.NewAtomicFileWriter(tempDir, filepath.Dir(asyncFile))
		if err == nil {
			defer writer.Abandon()
			writer.Write(pickle.PickleDumps(data))
			writer.Save(asyncFile)
		}
	}
}

func (s *ObjectServer) updateContainer(
	metadata map[string]string, req *http.Request, vars map[string]string) {
	partition := req.Header.Get(common.XContainerPartition)
	hosts := splitHeader(req.Header.Get(common.XContainerHost))
	devices := splitHeader(req.Header.Get(common.XContainerDevice))
	if partition == "" || len(hosts) == 0 || len(devices) == 0 {
		s.logger.Info("no container update task found")
		return
	}
	headers := http.Header{
		common.XBackendPolicyIndex: {common.GetDefault(req.Header, common.XBackendPolicyIndex, "0")},
		common.HReferer:            {common.GetDefault(req.Header, common.HReferer, "-")},
		common.HUserAgent:          {common.GetDefault(req.Header, common.HUserAgent, "-")},
		common.XTransId:            {common.GetDefault(req.Header, common.XTransId, "-")},
		common.XTimestamp:          {req.Header.Get(common.XTimestamp)},
	}
	if req.Method != http.MethodDelete {
		headers.Add(common.XContentType, metadata[common.HContentType])
		headers.Add(common.XSize, metadata[common.HContentLength])
		headers.Add(common.XEtag, metadata[common.HEtag])
	}
	failures := 0
	for index := range hosts {
		status := s.sendContainerUpdate(
			hosts[index], devices[index], req.Method, partition,
			vars["account"], vars["container"], vars["obj"], headers)
		if !status {
			s.logger.Error("unable to update container, async retry later",
				zap.String("host", hosts[index]),
				zap.String("device", devices[index]))
			failures++
		}
	}
	if failures > 0 {
		vs := s.generateVars(req.Method,
			vars["account"],
			vars["container"],
			vars["obj"],
			vars["device"],
			headers.Get(common.XBackendPolicyIndex))

		job := s.asyncJobMgr.New(vs, common.Headers2Map(headers))
		if err := s.asyncJobMgr.Save(job); err != nil {
			glogger.Error("unable to save async pending job", zap.Error(err))
		}
	}
}

func (s *ObjectServer) updateDeleteAt(method string, header http.Header,
	deleteAtTime time.Time, vars map[string]string) {
	container := header.Get(common.XDeleteAtContainer)
	if container == "" {
		container = s.expirerContainer(
			deleteAtTime, vars["account"], vars["container"], vars["obj"])
	}
	obj := fmt.Sprintf("%010d-%s/%s/%s",
		deleteAtTime.Unix(), vars["account"], vars["container"], vars["obj"])
	partition := header.Get(common.XDeleteAtPartition)
	hosts := splitHeader(header.Get(common.XDeleteAtHost))
	devices := splitHeader(header.Get(common.XDeleteAtDevice))
	headers := http.Header{
		common.XBackendPolicyIndex: {common.GetDefault(header, common.XBackendPolicyIndex, "0")},
		common.HReferer:            {common.GetDefault(header, common.HReferer, "-")},
		common.HUserAgent:          {common.GetDefault(header, common.HUserAgent, "-")},
		common.XTransId:            {common.GetDefault(header, common.XTransId, "-")},
		common.XTimestamp:          {header.Get(common.XTimestamp)},
	}
	if method != http.MethodDelete {
		headers.Add(common.XContentType, common.VTextPlain)
		headers.Add(common.XSize, "0")
		headers.Add(common.XEtag, zeroByteHash)
	}
	failures := 0
	for index := range hosts {
		status := s.sendContainerUpdate(hosts[index], devices[index],
			method, partition, deleteAtAccount, container, obj, headers)
		if !status {
			s.logger.Error("unable to update container delete, async retry later",
				zap.String("host", hosts[index]),
				zap.String("device", devices[index]))
			failures++
		}
	}
	if failures > 0 || len(hosts) == 0 {
		vs := s.generateVars(method,
			deleteAtAccount,
			container,
			obj,
			vars["device"],
			headers.Get(common.XBackendPolicyIndex))
		job := s.asyncJobMgr.New(vs, common.Headers2Map(headers))
		if err := s.asyncJobMgr.Save(job); err != nil {
			glogger.Error("unable to save async pending job", zap.Error(err))
		}
	}
}

func (s *ObjectServer) containerUpdates(
	w http.ResponseWriter, req *http.Request,
	metadata map[string]string,
	deleteAt string,
	vars map[string]string) {
	defer middleware.Recover(w, req, "PANIC WHILE UPDATING CONTAINER LISTINGS")
	if deleteAtTime, err := common.ParseDate(deleteAt); err == nil {
		go s.updateDeleteAt(req.Method, req.Header, deleteAtTime, vars)
	}

	firstDone := make(chan struct{}, 1)
	go func() {
		s.updateContainer(metadata, req, vars)
		firstDone <- struct{}{}
	}()
	select {
	case <-firstDone:
	case <-time.After(s.updateTimeout):
	}
}
