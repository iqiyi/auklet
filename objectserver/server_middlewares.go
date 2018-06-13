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
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"strconv"
	"time"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/srv"
	"go.uber.org/zap"
)

func (server *ObjectServer) RequestLogger(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		newWriter := &srv.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
		start := time.Now()
		logr := server.logger.With(zap.String("txn", request.Header.Get("X-Trans-Id")))
		request = srv.SetLogger(request, logr)
		next.ServeHTTP(newWriter, request)
		forceAcquire := request.Header.Get("X-Force-Acquire") == "true"

		extraInfo := "-"
		if forceAcquire {
			extraInfo = "FA"
		}
		logr.Info("Request log",
			zap.String("remoteAddr", request.RemoteAddr),
			zap.String("eventTime", time.Now().Format("02/Jan/2006:15:04:05 -0700")),
			zap.String("method", request.Method),
			zap.String("urlPath", common.Urlencode(request.URL.Path)),
			zap.Int("status", newWriter.Status),
			zap.String("contentLength", common.GetDefault(newWriter.Header(), "Content-Length", "-")),
			zap.String("referer", common.GetDefault(request.Header, "Referer", "-")),
			zap.String("userAgent", common.GetDefault(request.Header, "User-Agent", "-")),
			zap.Float64("requestTimeSeconds", time.Since(start).Seconds()),
			zap.String("extraInfo", extraInfo))
	}
	return http.HandlerFunc(fn)
}

func (s *ObjectServer) DeviceAcquirer(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, req *http.Request) {
		vars := srv.GetVars(req)
		if device, ok := vars["device"]; ok && device != "" {
			devicePath := filepath.Join(s.driveRoot, device)
			if s.checkMounts {
				mounted, err := fs.IsMount(devicePath)
				if err != nil || mounted != true {
					vars["Method"] = req.Method
					common.StandardResponse(w, http.StatusInsufficientStorage)
					return
				}
			}

			forceAcquire := req.Header.Get(common.XForceAcquire) == "true"
			concRequests := s.diskInUse.Acquire(device, forceAcquire)
			if concRequests != 0 {
				w.Header().Set(common.XDiskUsage, strconv.FormatInt(concRequests, 10))
				common.StandardResponse(w, http.StatusServiceUnavailable)
				return
			}
			defer s.diskInUse.Release(device)

			if account, ok := vars["account"]; ok && account != "" {
				limitKey := fmt.Sprintf("%s/%s", device, account)
				concRequests := s.accountDiskInUse.Acquire(limitKey, false)
				if concRequests != 0 {
					common.StandardResponse(w, 498)
					return
				}
				defer s.accountDiskInUse.Release(limitKey)
			}
		}
		next.ServeHTTP(w, req)
	}
	return http.HandlerFunc(fn)
}

func (s *ObjectServer) updateDeviceLocks(seconds int64) {
	reloadTime := time.Duration(seconds) * time.Second
	for {
		time.Sleep(reloadTime)
		for _, key := range s.diskInUse.Keys() {
			lockPath := filepath.Join(s.driveRoot, key, "lock_device")
			if fs.Exists(lockPath) {
				s.diskInUse.Lock(key)
			} else {
				s.diskInUse.Unlock(key)
			}
		}
	}
}
