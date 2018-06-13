// Copyright (c) 2017 Rackspace
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

package middleware

import (
	"fmt"
	"net/http"

	"github.com/iqiyi/auklet/common"
)

func OptionsHandler(serverType string, writer http.ResponseWriter, request *http.Request) {
	server := fmt.Sprintf("%s/%s", serverType, common.Version)
	//We could use introspection in future to figure out the Allow Header.
	writer.Header().Set("Allow", "HEAD,GET,PUT,POST,DELETE,OPTIONS")
	writer.Header().Set("Server", server)
	writer.WriteHeader(http.StatusOK)
	return
}
