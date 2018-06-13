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
//

package srv

import (
	"bufio"
	"net"
	"net/http"
)

// ResponseWriter that saves its status - used for logging.
type WebWriter struct {
	http.ResponseWriter
	Status          int
	ResponseStarted bool
}

func (w *WebWriter) WriteHeader(status int) {
	w.Status = status
	w.ResponseStarted = true
	w.ResponseWriter.WriteHeader(status)
}

func (w WebWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

func (w *WebWriter) Response() (bool, int) {
	return w.ResponseStarted, w.Status
}

type WebWriterInterface interface {
	http.ResponseWriter
	Response() (bool, int)
}
