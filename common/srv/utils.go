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

package srv

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/iqiyi/auklet/common"

	"go.uber.org/zap"
)

func ValidateRequest(w http.ResponseWriter, r *http.Request) bool {
	// if invalid request will right own response and return false, otherwise true
	if !utf8.ValidString(r.URL.Path) || strings.Contains(r.URL.Path, "\x00") {
		common.CustomResponse(
			w, http.StatusPreconditionFailed, "invalid UTF8 or contains NULL")
		return false
	}
	if !utf8.ValidString(r.Header.Get(common.HContentType)) ||
		strings.Contains(r.Header.Get(common.HContentType), "\x00") {
		common.CustomResponse(
			w, http.StatusPreconditionFailed, "invalid UTF8 or contains NULL")
		return false
	}
	return true
}

func LogPanics(logger *zap.Logger, msg string) {
	if e := recover(); e != nil {
		recoveredMsg := fmt.Sprintf("PANIC (%s)", msg)
		logger.Error(recoveredMsg, zap.Any("err", e))
	}
}

func DumpGoroutinesStackTrace(pid int) {
	filename := filepath.Join("/tmp", strconv.Itoa(pid)+".dump")
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	ioutil.WriteFile(filename, buf, 0644)
}

func RetryListen(ip string, port int) (net.Listener, error) {
	address := fmt.Sprintf("%s:%d", ip, port)
	started := time.Now()
	for {
		if sock, err := net.Listen("tcp", address); err == nil {
			return sock, nil
		} else if time.Now().Sub(started) > 10*time.Second {
			return nil,
				errors.New(fmt.Sprintf("Failed to bind for 10 seconds (%v)", err))
		}
		time.Sleep(time.Second / 5)
	}
}

func CopyRequestHeaders(r *http.Request, dst *http.Request) {
	for key := range r.Header {
		dst.Header.Set(key, r.Header.Get(key))
	}
}

func CopyResponseHeaders(w http.ResponseWriter, src *http.Response) {
	for key := range src.Header {
		w.Header().Set(key, src.Header.Get(key))
	}
}
