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

package common

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func CustomResponse(w http.ResponseWriter, statusCode int, message string) {
	body := []byte(message)
	w.Header().Set(HContentType, VTextHtml)
	w.Header().Set(XContentTypeOptions, VNoSniff)
	w.Header().Set(HContentLength, strconv.Itoa(len(body)))
	w.WriteHeader(statusCode)
	w.Write(body)
}

func StandardResponse(w http.ResponseWriter, statusCode int) {
	body := http.StatusText(statusCode)
	if body == "" {
		body = StatusText(statusCode)
	}

	CustomResponse(w, statusCode, body)
}

func IsUserMeta(server, key string) bool {
	// The format of user metadata looks like this
	// X-$SERVER_TYPE-Meta-$NAME
	// For example, X-Object-Meta-Bussiness.
	if len(key) <= 8+len(server) {
		return false
	}

	prefix := fmt.Sprintf("x-%s-meta-", strings.ToLower(server))
	return strings.HasPrefix(strings.ToLower(key), prefix)
}

func IsSysMeta(server, key string) bool {
	// The format of user metadata looks like this
	// X-$SERVER_TYPE-Sysmeta-$NAME
	// For example, X-Object-Sysmeta-Bussiness.
	if len(key) <= 11+len(server) {
		return false
	}

	prefix := fmt.Sprintf("x-%s-sysmeta-", strings.ToLower(server))
	return strings.HasPrefix(strings.ToLower(key), prefix)
}

func IsObjectTransientSysMeta(key string) bool {
	if len(key) <= len(ObjectTransientSysMetaPrefix) {
		return false
	}

	return strings.HasPrefix(strings.ToLower(key), ObjectTransientSysMetaPrefix)
}
