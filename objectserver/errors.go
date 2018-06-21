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

package objectserver

import (
	"errors"
)

var (
	EngineNotFound          = errors.New("object engine not found")
	LockPathError           = errors.New("unable to lock path")
	PathNotDirError         = errors.New("path is not a directory")
	NotPackEngine           = errors.New("engine is not pack type")
	DriveFull               = errors.New("drive is full")
	ErrHashConfNotFound     = errors.New("unable to read hash prefix and suffxi")
	ErrAsyncJobDBNotFound   = errors.New("unable to find db for async jobs")
	ErrKVAsyncJobNotSaved   = errors.New("unable to save async job")
	ErrKVAsyncJobNotClean   = errors.New("unable to clean async job")
	ErrUnknownAsyncJobMgr   = errors.New("unknown async job manager type")
	ErrFSAsyncJobMgrNotInit = errors.New("unable to create fs job mgr")
)

// Client bad request error text
var (
	ReqPolicyNotFound        = "storage policy not found"
	ReqNotPackEngine         = "target engine is not pack type"
	ReqNotPbData             = "body is not protobuf data"
	ReqContentTypeMissed     = "content type not specified"
	ReqDeleteInPass          = "X-Delete-At in past"
	ReqContentTypeNotAllowed = "Content-Type is not allowed in POST"
	ReqInvalidTimestamp      = "invalid X-Timestamp header"
)
